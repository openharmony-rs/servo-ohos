/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![cfg_attr(crown, allow(crown::jscontext_first_arg))]

use std::cell::Cell;
use std::collections::VecDeque;

use dom_struct::dom_struct;
use js::context::JSContext;
use js::jsapi::Heap;
use js::jsval::{JSVal, NullValue, UndefinedValue};
use js::rust::{HandleValue, MutableHandleValue};
use script_bindings::cell::DomRefCell;
use script_bindings::reflector::{Reflector, reflect_dom_object};
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, IndexedDBCursorDirection,
    IndexedDBCursorStep, IndexedDBKeyRange, IndexedDBKeyType, IndexedDBRecord, RequestOutput,
};

use crate::dom::bindings::codegen::Bindings::IDBCursorBinding::{
    IDBCursorDirection, IDBCursorMethods,
};
use crate::dom::bindings::codegen::Bindings::IDBIndexBinding::IDBIndexMethods;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::IDBTransactionMode;
use crate::dom::bindings::codegen::UnionTypes::IDBObjectStoreOrIDBIndex;
use crate::dom::bindings::error::{Error, ErrorResult, Fallible};
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::root::{Dom, DomRoot, MutNullableDom};
use crate::dom::bindings::structuredclone;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbindex::IDBIndex;
use crate::dom::indexeddb::idbobjectstore::IDBObjectStore;
use crate::dom::indexeddb::idbrequest::{IDBRequest, RequestSource};
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::key::{
    ExtractionResult, convert_value_to_key, extract_key, key_type_to_jsval,
};

/// The backend's form of `direction`.
pub(crate) fn backend_direction(direction: IDBCursorDirection) -> IndexedDBCursorDirection {
    match direction {
        IDBCursorDirection::Next => IndexedDBCursorDirection::Next,
        IDBCursorDirection::Nextunique => IndexedDBCursorDirection::NextUnique,
        IDBCursorDirection::Prev => IndexedDBCursorDirection::Prev,
        IDBCursorDirection::Prevunique => IndexedDBCursorDirection::PrevUnique,
    }
}

/// The most records that a cursor asks the backend for in one step: the record the step finds,
/// and those that the next steps without a key would find, which the cursor keeps.
const MAX_PREFETCHED_RECORDS: u32 = 64;

/// The records that a cursor's request to the backend returned after the record that the
/// request moved the cursor to, as the next steps without a key would find them.
#[derive(Default, MallocSizeOf)]
struct Prefetched {
    records: VecDeque<IndexedDBRecord>,
    /// The writes count of the cursor's transaction for its effective object store when the
    /// request was made; the records are current while it stays the same.
    writes: u64,
}

/// A request of a cursor that the backend runs.
#[derive(Clone, Copy, MallocSizeOf)]
struct Fetch {
    /// The most records that it asks for.
    records: u32,
    /// The writes count of the cursor's transaction for its effective object store when it was
    /// made.
    writes: u64,
}

#[derive(JSTraceable, MallocSizeOf)]
#[cfg_attr(crown, crown::unrooted_must_root_lint::must_root)]
pub(crate) enum ObjectStoreOrIndex {
    ObjectStore(Dom<IDBObjectStore>),
    Index(Dom<IDBIndex>),
}

#[dom_struct]
pub(crate) struct IDBCursor {
    reflector_: Reflector,

    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-transaction>
    transaction: Dom<IDBTransaction>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-range>
    #[no_trace]
    range: IndexedDBKeyRange,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-source>
    source: ObjectStoreOrIndex,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-direction>
    direction: IDBCursorDirection,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-position>
    #[no_trace]
    position: DomRefCell<Option<IndexedDBKeyType>>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-key>
    #[no_trace]
    key: DomRefCell<Option<IndexedDBKeyType>>,
    #[ignore_malloc_size_of = "mozjs"]
    cached_key: DomRefCell<Option<Heap<JSVal>>>,
    #[ignore_malloc_size_of = "mozjs"]
    cached_primary_key: DomRefCell<Option<Heap<JSVal>>>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-value>
    #[ignore_malloc_size_of = "mozjs"]
    value: Heap<JSVal>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-got-value-flag>
    got_value: Cell<bool>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-object-store-position>
    #[no_trace]
    object_store_position: DomRefCell<Option<IndexedDBKeyType>>,
    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-key-only-flag>
    key_only: bool,

    /// <https://w3c.github.io/IndexedDB/#cursor-request>
    request: MutNullableDom<IDBRequest>,

    /// The records that the next steps of the cursor can take without asking the backend.
    #[no_trace]
    prefetched: DomRefCell<Prefetched>,
    /// The last request of the cursor that the backend runs, and whether its result is pending.
    #[no_trace]
    last_fetch: Cell<Option<Fetch>>,
    fetch_pending: Cell<bool>,
}

impl IDBCursor {
    #[cfg_attr(crown, expect(crown::unrooted_must_root))]
    pub(crate) fn new_inherited(
        transaction: &IDBTransaction,
        direction: IDBCursorDirection,
        got_value: bool,
        source: ObjectStoreOrIndex,
        range: IndexedDBKeyRange,
        key_only: bool,
    ) -> IDBCursor {
        IDBCursor {
            reflector_: Reflector::new(),
            transaction: Dom::from_ref(transaction),
            range,
            source,
            direction,
            position: DomRefCell::new(None),
            key: DomRefCell::new(None),
            cached_key: DomRefCell::new(None),
            cached_primary_key: DomRefCell::new(None),
            value: Heap::default(),
            got_value: Cell::new(got_value),
            object_store_position: DomRefCell::new(None),
            key_only,
            request: Default::default(),
            prefetched: Default::default(),
            last_fetch: Cell::new(None),
            fetch_pending: Cell::new(false),
        }
    }

    #[cfg_attr(crown, expect(crown::unrooted_must_root))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        transaction: &IDBTransaction,
        direction: IDBCursorDirection,
        got_value: bool,
        source: ObjectStoreOrIndex,
        range: IndexedDBKeyRange,
        key_only: bool,
    ) -> DomRoot<IDBCursor> {
        reflect_dom_object(
            cx,
            Box::new(IDBCursor::new_inherited(
                transaction,
                direction,
                got_value,
                source,
                range,
                key_only,
            )),
            global,
        )
    }

    fn set_position(&self, position: Option<IndexedDBKeyType>) {
        let changed = *self.position.borrow() != position;
        *self.position.borrow_mut() = position;
        if changed {
            *self.cached_primary_key.borrow_mut() = None;
        }
    }

    fn set_key(&self, key: Option<IndexedDBKeyType>) {
        let key_changed = {
            let current_key = self.key.borrow();
            current_key.as_ref() != key.as_ref()
        };
        *self.key.borrow_mut() = key;
        if key_changed {
            *self.cached_key.borrow_mut() = None;
        }
    }

    fn set_object_store_position(&self, object_store_position: Option<IndexedDBKeyType>) {
        let changed = *self.object_store_position.borrow() != object_store_position;
        *self.object_store_position.borrow_mut() = object_store_position;
        if changed {
            *self.cached_primary_key.borrow_mut() = None;
        }
    }

    pub(crate) fn set_request(&self, request: &IDBRequest) {
        self.request.set(Some(request));
    }

    pub(crate) fn value(&self, mut out: MutableHandleValue) {
        out.set(self.value.get());
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-effective-key>
    pub(crate) fn effective_key(&self) -> Option<IndexedDBKeyType> {
        match &self.source {
            ObjectStoreOrIndex::ObjectStore(_) => self.position.borrow().clone(),
            ObjectStoreOrIndex::Index(_) => self.object_store_position.borrow().clone(),
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-source>, as the source of the requests that
    /// iterate the cursor.
    fn request_source(&self) -> RequestSource<'_> {
        match &self.source {
            ObjectStoreOrIndex::ObjectStore(store) => RequestSource::ObjectStore(store),
            ObjectStoreOrIndex::Index(index) => RequestSource::Index(index),
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#cursor-effective-object-store>
    pub(crate) fn effective_object_store(&self) -> DomRoot<IDBObjectStore> {
        match &self.source {
            ObjectStoreOrIndex::ObjectStore(store) => store.as_rooted(),
            ObjectStoreOrIndex::Index(index) => index.object_store(),
        }
    }

    pub(crate) fn verify_not_deleted(&self) -> ErrorResult {
        match &self.source {
            ObjectStoreOrIndex::ObjectStore(store) => store.verify_not_deleted(),
            ObjectStoreOrIndex::Index(index) => index.verify_not_deleted(),
        }
    }

    pub(crate) fn check_transaction_active(&self) -> ErrorResult {
        if !self.transaction.is_active() || !self.transaction.is_usable() {
            Err(Error::TransactionInactive(Some(
                "Transaction is not active".to_owned(),
            )))
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_transaction_readwrite(&self) -> ErrorResult {
        if let IDBTransactionMode::Readonly = self.transaction.get_mode() {
            return Err(Error::ReadOnly(Some(
                "Transaction is a read-only transaction".to_owned(),
            )));
        }
        Ok(())
    }

    /// How many records the next request of the cursor to the backend asks for: twice as many
    /// as the last request, up to [`MAX_PREFETCHED_RECORDS`], for a step without a key, as
    /// Chromium and Gecko prefetch; one at first, after a write in the transaction, and for a
    /// step with a key, which went beyond the records taken before.
    ///
    /// A "nextunique" cursor over an index that isn't unique asks for one record: to return the
    /// keys after the first, the backend reads every record of each key, while a step reads only
    /// the first record of its key.
    fn next_fetch(&self, with_key: bool) -> Fetch {
        let writes = self.transaction.writes(self.effective_object_store().id());
        let unique_steps = self.direction == IDBCursorDirection::Nextunique &&
            matches!(&self.source, ObjectStoreOrIndex::Index(index) if !index.Unique());
        let records = match self.last_fetch.get() {
            Some(last) if last.writes == writes && !with_key && !unique_steps => {
                (last.records * 2).min(MAX_PREFETCHED_RECORDS)
            },
            _ => 1,
        };
        Fetch { records, writes }
    }

    /// The operation of <https://w3c.github.io/IndexedDB/#iterate-a-cursor> with this cursor,
    /// `key`, `primary_key` and `count`: the backend finds the record from the cursor's position
    /// (step 9), which `iterate_cursor` then moves the cursor to, and returns the records that the
    /// next steps without a key would find after it, which the cursor keeps.
    pub(crate) fn iterate_operation(
        &self,
        key: Option<IndexedDBKeyType>,
        primary_key: Option<IndexedDBKeyType>,
        count: u32,
    ) -> AsyncOperation {
        // Step 3. Assert: if primaryKey is given, source is an index and direction is "next" or
        // "prev".
        if primary_key.is_some() {
            assert!(matches!(self.source, ObjectStoreOrIndex::Index(..)));
            assert!(matches!(
                self.direction,
                IDBCursorDirection::Next | IDBCursorDirection::Prev
            ));
        }
        let index = match &self.source {
            ObjectStoreOrIndex::ObjectStore(_) => None,
            ObjectStoreOrIndex::Index(index) => Some(index.id()),
        };
        let fetch = self.next_fetch(key.is_some());
        self.last_fetch.set(Some(fetch));
        self.fetch_pending.set(true);
        self.prefetched.borrow_mut().records.clear();
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Iterate {
            index,
            key_range: self.range.clone(),
            direction: backend_direction(self.direction),
            step: IndexedDBCursorStep {
                position: self.position.borrow().clone(),
                object_store_position: self.object_store_position.borrow().clone(),
                key,
                primary_key,
                count,
            },
            records: fetch.records,
            key_only: self.key_only,
        })
    }

    /// Takes the record that <https://w3c.github.io/IndexedDB/#iterate-a-cursor> step 9 finds
    /// with `key`, `primary_key` and `count` from the prefetched records, with those before it, if
    /// they are current and hold it; returns `None` and drops them otherwise.
    ///
    /// The prefetched records are the records that the steps without a key find one after the
    /// other, from the cursor's position, as of the cursor's last request to the backend; no
    /// write was made in the transaction since, so the records are still those. Step 9 with a key
    /// (and a primary key) finds the first of them that meets its requirements for the key (and
    /// primary key) as well, and with count n, the nth.
    fn take_prefetched(
        &self,
        key: Option<&IndexedDBKeyType>,
        primary_key: Option<&IndexedDBKeyType>,
        count: u32,
    ) -> Option<IndexedDBRecord> {
        let mut prefetched = self.prefetched.borrow_mut();
        if prefetched.writes != self.transaction.writes(self.effective_object_store().id()) {
            prefetched.records.clear();
            return None;
        }
        let reverse = matches!(
            self.direction,
            IDBCursorDirection::Prev | IDBCursorDirection::Prevunique
        );
        let beyond = |a: &IndexedDBKeyType, b: &IndexedDBKeyType| {
            if reverse { a < b } else { a > b }
        };
        let found = match (key, primary_key) {
            (None, _) => (count as usize).checked_sub(1),
            // "If key is defined, the record’s key is greater than or equal to key", or less for
            // "prev" and "prevunique".
            (Some(key), None) => prefetched
                .records
                .iter()
                .position(|record| record.key == *key || beyond(&record.key, key)),
            // "If primaryKey is defined, the record’s key is equal to key and the record’s value
            // is greater than or equal to primaryKey, or the record’s key is greater than key",
            // or less for "prev".
            (Some(key), Some(primary_key)) => prefetched.records.iter().position(|record| {
                beyond(&record.key, key) ||
                    (record.key == *key &&
                        (record.primary_key == *primary_key ||
                            beyond(&record.primary_key, primary_key)))
            }),
        };
        match found.filter(|found| *found < prefetched.records.len()) {
            Some(found) => {
                prefetched.records.drain(..found);
                prefetched.records.pop_front()
            },
            None => {
                prefetched.records.clear();
                None
            },
        }
    }

    /// Drops the records that the cursor keeps, which its transaction, once finished, can't use.
    pub(crate) fn drop_prefetched_records(&self) {
        self.prefetched.borrow_mut().records = VecDeque::new();
    }

    /// Runs <https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request> with this
    /// cursor's source, its request and the steps to iterate this cursor with `key`,
    /// `primary_key` and `count`: with the record that the prefetched records give, or else
    /// with the backend.
    fn iterate(
        &self,
        cx: &mut JSContext,
        key: Option<IndexedDBKeyType>,
        primary_key: Option<IndexedDBKeyType>,
        count: u32,
    ) -> ErrorResult {
        let request = self.Request();
        let iteration_param = IterationParam {
            cursor: Trusted::new(self),
        };
        if let Some(record) = self.take_prefetched(key.as_ref(), primary_key.as_ref(), count) {
            return IDBRequest::execute_with_result(
                self.request_source(),
                Ok(RequestOutput::Records(vec![record])),
                request,
                iteration_param,
            );
        }
        IDBRequest::execute_async(
            cx,
            self.request_source(),
            self.iterate_operation(key, primary_key, count),
            Some(request),
            Some(iteration_param),
        )?;
        Ok(())
    }
}

impl IDBCursorMethods<crate::DomTypeHolder> for IDBCursor {
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-source>
    fn Source(&self) -> IDBObjectStoreOrIDBIndex {
        match &self.source {
            ObjectStoreOrIndex::ObjectStore(source) => {
                IDBObjectStoreOrIDBIndex::IDBObjectStore(source.as_rooted())
            },
            ObjectStoreOrIndex::Index(source) => {
                IDBObjectStoreOrIDBIndex::IDBIndex(source.as_rooted())
            },
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-direction>
    fn Direction(&self) -> IDBCursorDirection {
        self.direction
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-key>
    fn Key(&self, cx: &mut JSContext, mut value: MutableHandleValue) {
        // The key getter steps are to return the result of converting a key to a value with the cursor’s current key.
        //
        // NOTE: If key returns an object (e.g. a Date or Array), it returns the
        // same object instance every time it is inspected, until the cursor’s key is changed.
        // This means that if the object is modified, those modifications will be seen by
        // anyone inspecting the value of the cursor. However modifying such an object does not
        // modify the contents of the database.
        if let Some(cached) = &*self.cached_key.borrow() {
            value.set(cached.get());
            return;
        }

        match self.key.borrow().as_ref() {
            Some(key) => key_type_to_jsval(cx, key, value.reborrow()),
            None => value.set(UndefinedValue()),
        }

        *self.cached_key.safe_borrow_mut(cx.no_gc()) = Some(Heap::default());
        self.cached_key.borrow().as_ref().unwrap().set(value.get());
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-primarykey>
    fn PrimaryKey(&self, cx: &mut JSContext, mut value: MutableHandleValue) {
        // NOTE: If primaryKey returns an object (e.g. a Date or Array),
        // it returns the same object instance every time it is inspected,
        // until the cursor’s effective key is changed. This means that if the object is modified,
        // those modifications will be seen by anyone inspecting the value of the cursor.
        // However modifying such an object does not modify the contents of the database.
        if let Some(cached) = &*self.cached_primary_key.borrow() {
            value.set(cached.get());
            return;
        }

        match self.effective_key() {
            Some(effective_key) => key_type_to_jsval(cx, &effective_key, value.reborrow()),
            None => value.set(UndefinedValue()),
        }

        *self.cached_primary_key.safe_borrow_mut(cx.no_gc()) = Some(Heap::default());
        self.cached_primary_key
            .borrow()
            .as_ref()
            .unwrap()
            .set(value.get());
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbcursor-request>
    fn Request(&self) -> DomRoot<IDBRequest> {
        self.request
            .get()
            .expect("IDBCursor.request should be set when cursor is opened")
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-advance>
    fn Advance(&self, cx: &mut JSContext, count: u32) -> ErrorResult {
        // Step 1: If count is 0 (zero), throw a TypeError.
        if count == 0 {
            return Err(Error::Type(c"Count cannot be zero".to_owned()));
        }

        // Step 2: Let transaction be this cursor's transaction.
        // Step 3: If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 4: If the cursor's source or effective object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 5: If this cursor's got value flag is unset, indicating that the cursor is being
        // iterated or has iterated past its end, throw an "InvalidStateError" DOMException.
        if !self.got_value.get() {
            return Err(Error::InvalidState(Some(
                "The cursor is being iterated or has iterated past its end".to_owned(),
            )));
        }

        // Step 7: Unset the got value flag on the cursor.
        self.got_value.set(false);

        // Step 8: Let request be the request created when this cursor was created.
        // Step 9: Unset the request's done flag on request.
        self.Request().unset_done_flag();

        // Step 10: Run the steps to asynchronously execute a request with the cursor's source as
        // source, the steps to iterate a cursor as operation and request, using the current Realm
        // as targetRealm, this cursor and count.
        self.iterate(cx, None, None, count)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-continue>
    fn Continue(&self, cx: &mut JSContext, key: HandleValue) -> ErrorResult {
        // Step 1: Let transaction be this cursor's transaction.
        // Step 2: If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 3: If the cursor's source or effective object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4: If this cursor's got value flag is unset, indicating that the cursor is being
        // iterated or has iterated past its end, throw an "InvalidStateError" DOMException.
        if !self.got_value.get() {
            return Err(Error::InvalidState(Some(
                "The cursor is being iterated or has iterated past its end".to_owned(),
            )));
        }

        // Step 5: If key is given, then:
        let key = if !key.is_undefined() {
            // Step 5.1: Let r be the result of running the steps to convert a value to a key with
            // key. Rethrow any exceptions.
            // Step 5.2: If r is invalid, throw a "DataError" DOMException.
            // Step 5.3: Set key to r.
            let key = convert_value_to_key(cx, key, None)?.into_result()?;

            // Step 5.4: If key is less than or equal to this cursor's position and this cursor's
            // direction is "next" or "nextunique", throw a "DataError" DOMException.
            if let Some(position) = self.position.borrow().as_ref() {
                match self.direction {
                    IDBCursorDirection::Next | IDBCursorDirection::Nextunique => {
                        if &key <= position {
                            return Err(Error::Data(Some(
                                "Key is less than or equal to this cursor's position".to_owned(),
                            )));
                        }
                    },
                    // Step 5.5: If key is greater than or equal to this cursor's position
                    // and this cursor's direction is "prev" or "prevunique", throw a
                    // "DataError" DOMException.
                    IDBCursorDirection::Prev | IDBCursorDirection::Prevunique => {
                        if &key >= position {
                            return Err(Error::Data(Some(
                                "Key is greater than or equal to this cursor's position".to_owned(),
                            )));
                        }
                    },
                }
            }

            Some(key)
        } else {
            None
        };

        // Step 6: Unset the got value flag on the cursor.
        self.got_value.set(false);

        // Step 7: Let request be the request created when this cursor was created.
        // Step 8: Unset the request's done flag on request.
        self.Request().unset_done_flag();

        // Step 9: Run the steps to asynchronously execute a request with the cursor's source as
        // source, the steps to iterate a cursor as operation and request, using the current Realm
        // as targetRealm, this cursor and key (if given).
        self.iterate(cx, key, None, 1)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-continueprimarykey>
    fn ContinuePrimaryKey(
        &self,
        cx: &mut JSContext,
        key: HandleValue,
        primary_key: HandleValue,
    ) -> ErrorResult {
        // Step 1: Let transaction be this cursor's transaction.
        // Step 2: If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 3: If the cursor's source or effective object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4: If this cursor's source is not an index, throw an "InvalidAccessError"
        // DOMException.
        if !matches!(self.source, ObjectStoreOrIndex::Index(_)) {
            return Err(Error::InvalidAccess(Some(
                "The cursor's source is not an index".to_owned(),
            )));
        }

        // Step 5: If this cursor's direction is not "next" or "prev", throw an "InvalidAccessError"
        // DOMException.
        if !matches!(
            self.direction,
            IDBCursorDirection::Next | IDBCursorDirection::Prev
        ) {
            return Err(Error::InvalidAccess(Some(
                "The cursor's direction is not 'next' or 'prev'".to_owned(),
            )));
        }

        // Step 6: If this cursor's got value flag is unset, indicating that the cursor is being
        // iterated or has iterated past its end, throw an "InvalidStateError" DOMException.
        if !self.got_value.get() {
            return Err(Error::InvalidState(Some(
                "The cursor is being iterated or has iterated past its end".to_owned(),
            )));
        }

        // Step 7: Let r be the result of running the steps to convert a value to a key with key.
        // Step 8: If r is invalid, throw a "DataError" DOMException.
        // Step 9: Set key to r.
        let key = convert_value_to_key(cx, key, None)?.into_result()?;

        // Step 10: Let r be the result of running the steps to convert a value to a key with
        // primaryKey.
        // Step 11: If r is invalid, throw a "DataError" DOMException.
        // Step 12: Set primaryKey to r.
        let primary_key = convert_value_to_key(cx, primary_key, None)?.into_result()?;

        let position = self.position.borrow().clone();
        let object_store_position = self.object_store_position.borrow().clone();

        match self.direction {
            IDBCursorDirection::Next => {
                // Step 13: If key is less than this cursor's position and this cursor's direction
                // is "next", throw a "DataError" DOMException.
                if let Some(pos) = &position {
                    if &key < pos {
                        return Err(Error::Data(Some(
                            "Key is less than this cursor's position".to_owned(),
                        )));
                    }
                    // Step 15: If key is equal to this cursor's position and primaryKey is less
                    // than or equal to this cursor's object store position and this cursor's
                    // direction is "next", throw a "DataError" DOMException.
                    if &key == pos &&
                        object_store_position
                            .as_ref()
                            .is_some_and(|osp| &primary_key <= osp)
                    {
                        return Err(Error::Data(Some(
                            "Primary key is less than or equal to object store position".to_owned(),
                        )));
                    }
                }
            },
            IDBCursorDirection::Prev => {
                // Step 14: If key is greater than this cursor's position and this cursor's
                // direction is "prev", throw a "DataError" DOMException.
                if let Some(pos) = &position {
                    if &key > pos {
                        return Err(Error::Data(Some(
                            "Key is greater than this cursor's position".to_owned(),
                        )));
                    }
                    // Step 16: If key is equal to this cursor's position and primaryKey is greater
                    // than or equal to this cursor's object store position and this cursor's
                    // direction is "prev", throw a "DataError" DOMException.
                    if &key == pos &&
                        object_store_position
                            .as_ref()
                            .is_some_and(|osp| &primary_key >= osp)
                    {
                        return Err(Error::Data(Some(
                            "Primary key is greater than or equal to object store position"
                                .to_owned(),
                        )));
                    }
                }
            },
            _ => unreachable!("Guaranteed by Step 5"),
        }

        // Step 17: Unset the got value flag on the cursor.
        self.got_value.set(false);

        // Step 18: Let request be the request created when this cursor was created.
        // Step 19: Unset the request's done flag on request.
        self.Request().unset_done_flag();

        // Step 20: Run the steps to asynchronously execute a request with the cursor's source as
        // source, the steps to iterate a cursor as operation and request, using the current Realm
        // as targetRealm, this cursor, key and primaryKey.
        self.iterate(cx, Some(key), Some(primary_key), 1)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-update>
    fn Update(&self, cx: &mut JSContext, value: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this's transaction.
        // Step 2. If transaction's state is not active, then throw a "TransactionInactiveError"
        // DOMException.
        self.check_transaction_active()?;

        // Step 3. If transaction is a read-only transaction, throw a "ReadOnlyError" DOMException.
        self.check_transaction_readwrite()?;

        // Step 4. If this's source or effective object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;
        let effective_object_store = self.effective_object_store();
        effective_object_store.verify_not_deleted()?;

        // Step 5. If this's got value flag is false, indicating that the cursor is being iterated
        // or has iterated past its end, throw an "InvalidStateError" DOMException.
        if !self.got_value.get() {
            return Err(Error::InvalidState(Some(
                "The cursor is being iterated or has iterated past its end".to_owned(),
            )));
        }

        // Step 6. If this's key only flag is true, throw an "InvalidStateError" DOMException.
        if self.key_only {
            return Err(Error::InvalidState(Some(
                "This cursor is a key-only cursor".to_owned(),
            )));
        }

        // Step 7. Let targetRealm be a user-agent defined Realm.
        // Step 8. Let clone be a clone of value in targetRealm during transaction. Rethrow any
        // exceptions.
        rooted!(&in(cx) let mut cloned_js_value = NullValue());
        effective_object_store.clone_value_in_target_realm(
            cx,
            value,
            cloned_js_value.handle_mut(),
        )?;

        // Step 9. If this's effective object store uses in-line keys, then:
        if effective_object_store.uses_inline_keys() {
            // Step 9.1. Let kpk be the result of extracting a key from a value using a key path
            // with clone and the key path of this's effective object store. Rethrow any exceptions.
            let key_path = effective_object_store
                .key_path()
                .expect("Uses in-line keys requires key_path to be Some");
            let kpk = extract_key(cx, cloned_js_value.handle(), key_path, None)?;

            // Step 9.2. If kpk is failure, invalid, or not equal to this's effective key, throw a
            // "DataError" DOMException.
            match kpk {
                ExtractionResult::Failure | ExtractionResult::Invalid => {
                    return Err(Error::Data(Some(
                        "Failed to extract key from value using key path".to_owned(),
                    )));
                },
                ExtractionResult::Key(key) => {
                    if !self
                        .effective_key()
                        .is_some_and(|effective_key| effective_key == key)
                    {
                        return Err(Error::Data(Some(
                            "Extracted key does not match the effective key".to_owned(),
                        )));
                    }
                },
            }
        }

        let cloned_value = structuredclone::write(cx, cloned_js_value.handle(), None)?;
        let Ok(serialized_value) = postcard::to_stdvec(&cloned_value) else {
            return Err(Error::InvalidState(Some(
                "Failed to serialize cloned value".to_owned(),
            )));
        };

        // Step 10. Let operation be an algorithm to run store a record into an object store with
        // this's effective object store, clone, this's effective key, and false.
        let index_keys = effective_object_store.index_keys(cx, cloned_js_value.handle(), false);
        // Step 11. Return the result (an IDBRequest) of running asynchronously execute a request
        // with this and operation.
        IDBRequest::execute_async(
            cx,
            RequestSource::Cursor(self),
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                key: self.effective_key(),
                value: serialized_value,
                should_overwrite: true,
                index_keys,
            }),
            None,
            None,
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbcursor-delete>
    fn Delete(&self, cx: &mut JSContext) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this's transaction.
        // Step 2. If transaction's state is not active, then throw a "TransactionInactiveError"
        // DOMException.
        self.check_transaction_active()?;

        // Step 3. If transaction is a read-only transaction, throw a "ReadOnlyError" DOMException.
        self.check_transaction_readwrite()?;

        // Step 4. If this's source or effective object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;
        let effective_object_store = self.effective_object_store();
        effective_object_store.verify_not_deleted()?;

        // Step 5. If this's got value flag is false, indicating that the cursor is being iterated
        // or has iterated past its end, throw an "InvalidStateError" DOMException.
        if !self.got_value.get() {
            return Err(Error::InvalidState(Some(
                "The cursor is being iterated or has iterated past its end".to_owned(),
            )));
        }

        // Step 6. If this's key only flag is true, throw an "InvalidStateError" DOMException.
        if self.key_only {
            return Err(Error::InvalidState(Some(
                "This cursor is a key-only cursor".to_owned(),
            )));
        }

        // Step 7. Let operation be an algorithm to run delete records from an object store with
        // this's effective object store and this's effective key.
        // Step 8. Return the result (an IDBRequest) of running asynchronously execute a request
        // with this and operation.
        let effective_key = self.effective_key().ok_or_else(|| {
            Error::InvalidState(Some("Missing effective key on cursor".to_owned()))
        })?;

        IDBRequest::execute_async(
            cx,
            RequestSource::Cursor(self),
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem {
                key_range: IndexedDBKeyRange::only(effective_key),
            }),
            None,
            None,
        )
    }
}

/// The cursor that a request runs <https://www.w3.org/TR/IndexedDB-3/#iterate-a-cursor> for.
#[derive(Clone)]
pub(crate) struct IterationParam {
    pub(crate) cursor: Trusted<IDBCursor>,
}

/// <https://www.w3.org/TR/IndexedDB-3/#iterate-a-cursor>, with `records` from the backend, which
/// ran steps 2 to 9 but the cursor's changes (see `IDBCursor::iterate_operation`): first the
/// record that the cursor moves to, none if it moves past the end.
///
/// NOTE: For an index source, "record’s key" is the index key, "record’s value" the primary key
/// of the record in the object store, and "record’s referenced value" the value of that record.
pub(crate) fn iterate_cursor(
    cx: &mut JSContext,
    param: &IterationParam,
    records: Vec<IndexedDBRecord>,
) -> Result<Option<DomRoot<IDBCursor>>, Error> {
    let cursor = param.cursor.root();

    // Step 1. Let source be cursor’s source.
    let source = &cursor.source;

    // Steps 2-9.1: the backend found the record, after count - 1 rounds of step 9.
    let mut records = VecDeque::from(records);
    let found_record = records.pop_front();
    // The records after it are those that the next steps without a key find, as of the request.
    // A transaction that is finishing or finished takes no more steps.
    if cursor.fetch_pending.replace(false) && cursor.transaction.is_usable() {
        let writes = cursor.last_fetch.get().map_or(0, |fetch| fetch.writes);
        if !records.is_empty() {
            cursor.transaction.note_prefetching_cursor(&cursor);
        }
        *cursor.prefetched.borrow_mut() = Prefetched { records, writes };
    }
    let Some(found_record) = found_record else {
        // Step 9.2. If found record is not defined, then:
        // Step 9.2.1. Set cursor’s key to undefined.
        cursor.set_key(None);

        // Step 9.2.2. If source is an index, set cursor’s object store position to undefined.
        if matches!(source, ObjectStoreOrIndex::Index(_)) {
            cursor.set_object_store_position(None);
        }

        // Step 9.2.3. If cursor’s key only flag is unset, set cursor’s value to undefined.
        if !cursor.key_only {
            cursor.value.set(UndefinedValue());
        }

        // Step 9.2.4. Return null.
        return Ok(None);
    };

    // Step 9.3. Let position be found record’s key.
    // Step 10. Set cursor’s position to position.
    cursor.set_position(Some(found_record.key.clone()));

    // Step 9.4. If source is an index, let object store position be found record’s value.
    // Step 11. If source is an index, set cursor’s object store position to object store position.
    if let ObjectStoreOrIndex::Index(_) = source {
        cursor.set_object_store_position(Some(found_record.primary_key.clone()));
    }

    // Step 12. Set cursor’s key to found record’s key.
    cursor.set_key(Some(found_record.key));

    // Step 13. If cursor’s key only flag is unset, then:
    if !cursor.key_only {
        // Step 13.1. Let serialized be found record’s referenced value.
        // Step 13.2. Set cursor’s value to ! StructuredDeserialize(serialized, targetRealm)
        let serialized = found_record
            .value
            .ok_or_else(|| Error::Data(Some("A cursor record without its value".to_owned())))?;
        rooted!(&in(cx) let mut new_cursor_value = UndefinedValue());
        cursor.effective_object_store().deserialize_value(
            cx,
            &found_record.primary_key,
            &serialized,
            new_cursor_value.handle_mut(),
        )?;
        cursor.value.set(new_cursor_value.get());
    }

    // Step 14. Set cursor’s got value flag.
    cursor.got_value.set(true);

    // Step 15. Return cursor.
    Ok(Some(cursor))
}
