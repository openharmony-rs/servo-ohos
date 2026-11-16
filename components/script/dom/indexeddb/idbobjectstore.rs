/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::collections::HashMap;

use dom_struct::dom_struct;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::gc::MutableHandleValue;
use js::jsapi::Heap;
use js::jsval::{JSVal, NullValue, UndefinedValue};
use js::rust::HandleValue;
use script_bindings::cell::DomRefCell;
use script_bindings::codegen::GenericBindings::IDBObjectStoreBinding::{
    IDBGetAllOptions, IDBIndexParameters,
};
use script_bindings::codegen::GenericUnionTypes::StringOrStringSequence;
use script_bindings::error::ErrorResult;
use script_bindings::reflector::{Reflector, reflect_dom_object};
use servo_base::generic_channel::{GenericSend, GenericSender};
use storage_traits::indexeddb::{
    self, AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, IndexPopulationStep, IndexedDBIndex, IndexedDBIndexId,
    IndexedDBIndexKeys, IndexedDBKeyType, IndexedDBObjectStore, IndexedDBObjectStoreId,
    IndexedDBRecordIndexKeys, IndexedDBThreadMsg, IndexedDBValue, Utf16String,
};

use crate::dom::bindings::codegen::Bindings::IDBCursorBinding::IDBCursorDirection;
use crate::dom::bindings::codegen::Bindings::IDBObjectStoreBinding::IDBObjectStoreMethods;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::{
    IDBTransactionMethods, IDBTransactionMode,
};
use crate::dom::bindings::error::{Error, Fallible};
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot};
use crate::dom::bindings::str::DOMString;
use crate::dom::bindings::structuredclone;
use crate::dom::bindings::trace::NoTrace;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbcursor::{IDBCursor, IterationParam, ObjectStoreOrIndex};
use crate::dom::indexeddb::idbcursorwithvalue::IDBCursorWithValue;
use crate::dom::indexeddb::idbindex::IDBIndex;
use crate::dom::indexeddb::idbrequest::{IDBRequest, QueryOrOptions, RequestSource, RetrievalKind};
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::key::{
    EvaluationResult, ExtractionResult, can_inject_key_into_value, clear_pending_exception,
    convert_value_to_key, convert_value_to_key_range, dom_string_from_name,
    evaluate_key_path_on_value, extract_key, index_keys, inject_key_into_value,
    injected_key_is_stored, is_valid_key_path, name_from_dom_string, sorted_name_list,
};

#[derive(Clone, JSTraceable, MallocSizeOf)]
pub enum KeyPath {
    String(DOMString),
    StringSequence(Vec<DOMString>),
}

impl From<StringOrStringSequence> for KeyPath {
    fn from(value: StringOrStringSequence) -> Self {
        match value {
            StringOrStringSequence::String(s) => KeyPath::String(s),
            StringOrStringSequence::StringSequence(ss) => KeyPath::StringSequence(ss),
        }
    }
}

impl From<indexeddb::KeyPath> for KeyPath {
    fn from(value: indexeddb::KeyPath) -> Self {
        match value {
            indexeddb::KeyPath::String(string) => KeyPath::String(string.into()),
            indexeddb::KeyPath::Sequence(ss) => {
                KeyPath::StringSequence(ss.into_iter().map(Into::into).collect())
            },
        }
    }
}

impl KeyPath {
    /// Sets `retval` to the key path converted to a value, which `cache` (undefined until then)
    /// keeps, so that the keyPath getters of IDBObjectStore and IDBIndex return the same Array
    /// every time.
    pub(crate) fn to_jsval_cached(
        &self,
        cx: &mut JSContext,
        cache: &Heap<JSVal>,
        mut retval: MutableHandleValue,
    ) {
        if cache.get().is_undefined() {
            match self {
                KeyPath::String(path) => path.to_jsval(cx, retval.reborrow()),
                KeyPath::StringSequence(paths) => paths.to_jsval(cx, retval.reborrow()),
            }
            cache.set(retval.get());
        }
        retval.set(cache.get());
    }
}

impl From<KeyPath> for indexeddb::KeyPath {
    fn from(item: KeyPath) -> Self {
        match item {
            KeyPath::String(s) => Self::String(String::from(s)),
            KeyPath::StringSequence(ss) => {
                Self::Sequence(ss.into_iter().map(String::from).collect())
            },
        }
    }
}

#[dom_struct]
pub struct IDBObjectStore {
    reflector_: Reflector,
    /// The id of the handle's object store.
    id: IndexedDBObjectStoreId,
    #[no_trace]
    name: DomRefCell<Utf16String>,
    key_path: Option<KeyPath>,
    /// The key path converted to a value by the first `keyPath` getter, undefined before.
    #[ignore_malloc_size_of = "mozjs"]
    key_path_value: Heap<JSVal>,
    index_set: DomRefCell<HashMap<NoTrace<Utf16String>, Dom<IDBIndex>>>,
    /// Every index handle of this handle, also those of indexes deleted since, whose names an
    /// aborted upgrade transaction reverts.
    index_handles: DomRefCell<Vec<Dom<IDBIndex>>>,
    transaction: Dom<IDBTransaction>,
    has_key_generator: bool,

    // We store the db name in the object store to address backend operations
    // that are keyed by (origin, database name, object store id).
    #[no_trace]
    db_name: Utf16String,
}

impl IDBObjectStore {
    fn new_inherited(store: &IndexedDBObjectStore, transaction: &IDBTransaction) -> IDBObjectStore {
        IDBObjectStore {
            reflector_: Reflector::new(),
            id: store.id,
            name: DomRefCell::new(store.name.clone()),
            key_path: store.key_path.clone().map(Into::into),
            key_path_value: Heap::default(),
            index_set: DomRefCell::new(HashMap::new()),
            index_handles: Default::default(),
            transaction: Dom::from_ref(transaction),
            has_key_generator: store.has_key_generator,
            db_name: transaction.get_db_name(),
        }
    }

    /// <https://w3c.github.io/IndexedDB/#object-store-handle>: a new handle of the object store
    /// `store` in `transaction`, with a handle of each of its indexes.
    pub fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        store: &IndexedDBObjectStore,
        transaction: &IDBTransaction,
    ) -> DomRoot<IDBObjectStore> {
        let handle = reflect_dom_object(
            cx,
            Box::new(IDBObjectStore::new_inherited(store, transaction)),
            global,
        );
        for index in &store.indexes {
            handle.add_index(cx, index);
        }
        handle
    }

    pub(crate) fn id(&self) -> IndexedDBObjectStoreId {
        self.id
    }

    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>, steps 5 and 6 for this
    /// handle, once the connection's object store set has been reverted.
    pub(crate) fn restore_metadata_after_abort(&self, cx: &mut JSContext) {
        // A store that the transaction created has an id that the reverted set doesn't have.
        let store = self.transaction.get_db().object_store(self.id);

        // Step 5.1. If handle’s object store was not newly created during transaction,
        // set handle’s name to its object store’s name.
        if let Some(store) = &store {
            *self.name.safe_borrow_mut(cx.no_gc()) = store.name.clone();
        }

        // Step 6. For each index handle handle associated with transaction, including those for
        // indexes that were created or deleted during transaction: If handle’s index was not
        // newly created during transaction, set handle’s name to its index’s name.
        let indexes = store.map(|store| store.indexes).unwrap_or_default();
        let handles: Vec<DomRoot<IDBIndex>> = self
            .index_handles
            .borrow()
            .iter()
            .map(|handle| handle.as_rooted())
            .collect();
        for handle in &handles {
            if let Some(index) = indexes.iter().find(|index| index.id == handle.id()) {
                handle.set_name(index.name.clone());
            }
        }

        // Step 5.2. Set handle’s index set to the set of indexes that reference its object
        // store.
        self.index_set.safe_borrow_mut(cx.no_gc()).clear();
        for index in &indexes {
            match handles.iter().find(|handle| handle.id() == index.id) {
                Some(handle) => {
                    self.index_set
                        .safe_borrow_mut(cx.no_gc())
                        .insert(NoTrace(handle.name()), Dom::from_ref(&**handle));
                },
                None => {
                    self.add_index(cx, index);
                },
            }
        }
    }

    pub(crate) fn transaction(&self) -> DomRoot<IDBTransaction> {
        self.transaction.as_rooted()
    }

    fn get_idb_thread(&self) -> GenericSender<IndexedDBThreadMsg> {
        self.global().storage_threads().sender()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#clone>
    pub(crate) fn clone_value_in_target_realm(
        &self,
        cx: &mut JSContext,
        value: HandleValue,
        clone: MutableHandleValue<'_>,
    ) -> Fallible<()> {
        // Step 1. Assert: transaction's state is active.
        debug_assert!(self.transaction.is_active());

        // Step 2. Set transaction's state to inactive.
        //
        // NOTE: The transaction is made inactive so that getters or other side
        // effects triggered by the cloning operation are unable to make
        // additional requests against the transaction.
        self.transaction.set_active_flag(false);

        let result = (|| {
            // Step 3. Let serialized be ? StructuredSerializeForStorage(value).
            let serialized = structuredclone::write(cx, value, None)?;

            // Step 4. Let clone be ? StructuredDeserialize(serialized, targetRealm).
            let _ = structuredclone::read(cx, &self.global(), serialized, clone)?;
            Ok(())
        })();

        // Step 5. Set transaction's state to active.
        self.transaction.set_active_flag(true);

        // Step 6. Return clone.
        result
    }

    /// Runs `f` with the transaction inactive, as cloning a value does: computing index keys
    /// runs in the page's realm, where a getter reached through a hole of an array or a
    /// prototype could otherwise make requests that overtake the one being made.
    fn with_transaction_inactive<T>(&self, f: impl FnOnce() -> T) -> T {
        let active = self.transaction.is_active();
        self.transaction.set_active_flag(false);
        let result = f();
        self.transaction.set_active_flag(active);
        result
    }

    fn has_key_generator(&self) -> bool {
        self.has_key_generator
    }

    /// Deserializes `serialized`, a value of this store whose record has the key `primary_key`,
    /// into `value`. A value stored under a generated key doesn't contain the key, so it is
    /// injected here, as <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store>
    /// step 1.1.3 does when storing, where step 4's serialization then keeps it only if it is a
    /// property of ordinary objects and Arrays.
    pub(crate) fn deserialize_value(
        &self,
        cx: &mut JSContext,
        primary_key: &IndexedDBKeyType,
        serialized: &[u8],
        mut value: MutableHandleValue,
    ) -> Fallible<()> {
        let data = postcard::from_bytes(serialized)
            .map_err(|_| Error::Data(Some("Failed to decode a stored value".to_owned())))?;
        structuredclone::read(cx, &self.global(), data, value.reborrow())?;
        let Some(key_path) = self.key_path.as_ref().filter(|_| self.has_key_generator) else {
            return Ok(());
        };
        // The key was generated if, and only if, the value had none at the key path when it was
        // stored. Values stored with the key already in them, such as those from before the key
        // generator moved to the backend, keep what they have.
        rooted!(&in(cx) let mut stored_key = UndefinedValue());
        if let EvaluationResult::Success =
            evaluate_key_path_on_value(cx, value.handle(), key_path, stored_key.handle_mut())?
        {
            return Ok(());
        }
        let injected = match key_path {
            KeyPath::String(key_path) => {
                if !injected_key_is_stored(cx, value.handle(), key_path)? {
                    return Ok(());
                }
                inject_key_into_value(cx, value.handle(), primary_key, key_path)?
            },
            KeyPath::StringSequence(_) => false,
        };
        if !injected {
            return Err(Error::Data(Some(
                "Failed to inject the generated key into a stored value".to_owned(),
            )));
        }
        Ok(())
    }

    /// The keys of the records that storing `value`, a clone, gives in each index of the store,
    /// for the backend to store with the record: steps 5.1 and 5.2 of
    /// <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store>. If
    /// `key_is_generated`, the backend generates the record's key, and the parts of the index
    /// keys that come from the key are placeholders.
    pub(crate) fn index_keys(
        &self,
        cx: &mut JSContext,
        value: HandleValue,
        key_is_generated: bool,
    ) -> Vec<IndexedDBIndexKeys> {
        let generated_key_path = match &self.key_path {
            Some(KeyPath::String(key_path)) if key_is_generated => Some(key_path.str()),
            _ => None,
        };
        let indexes = self.transaction.get_db().indexes(self.id);
        self.with_transaction_inactive(|| {
            indexes
                .iter()
                .map(|index| IndexedDBIndexKeys {
                    index_id: index.id,
                    keys: index_keys(cx, value, index, generated_key_path.as_deref()),
                })
                .collect()
        })
    }

    /// The keys that the records `records` of this store get in `index`, which is being
    /// populated: steps 5.1 and 5.2 of
    /// <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store> on each stored
    /// value, with its generated key injected. Records without keys are left out. Stops early if
    /// page code that computing the keys reached aborted the transaction.
    pub(crate) fn index_keys_of_records(
        &self,
        cx: &mut JSContext,
        index: &IndexedDBIndex,
        records: Vec<IndexedDBValue>,
    ) -> BackendResult<Vec<IndexedDBRecordIndexKeys>> {
        let mut keys_of_records = Vec::with_capacity(records.len());
        rooted!(&in(cx) let mut value = UndefinedValue());
        for record in records {
            if !self.transaction.is_usable() {
                break;
            }
            if self
                .deserialize_value(cx, &record.primary_key, &record.value, value.handle_mut())
                .is_err()
            {
                clear_pending_exception(cx);
                return Err(BackendError::DbErr(
                    "Failed to read a stored value to populate an index".to_owned(),
                ));
            }
            let keys =
                self.with_transaction_inactive(|| index_keys(cx, value.handle(), index, None));
            if !keys.is_empty() {
                keys_of_records.push(IndexedDBRecordIndexKeys {
                    primary_key: record.primary_key,
                    keys,
                });
            }
        }
        Ok(keys_of_records)
    }

    /// Sends a step of populating the index `index_id` of this handle's store to the backend.
    pub(crate) fn send_index_population_step(
        &self,
        index_id: IndexedDBIndexId,
        step: IndexPopulationStep,
    ) {
        if self
            .send_schema_operation(AsyncSchemaOperation::PopulateIndex { index_id, step })
            .is_err()
        {
            error!("Failed to send a step of populating IndexedDB index {index_id}");
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#object-store-in-line-keys>
    pub(crate) fn uses_inline_keys(&self) -> bool {
        self.key_path.is_some()
    }

    pub(crate) fn key_path(&self) -> Option<&KeyPath> {
        self.key_path.as_ref()
    }

    pub(crate) fn verify_not_deleted(&self) -> ErrorResult {
        if !self.transaction.get_db().has_object_store(self.id) {
            return Err(Error::InvalidState(None));
        }
        Ok(())
    }

    /// Checks if the transaction is active, throwing a "TransactionInactiveError" DOMException if not.
    fn check_transaction_active(&self) -> Fallible<()> {
        // Let transaction be this object store handle's transaction.
        let transaction = &self.transaction;

        // If transaction is not active, throw a "TransactionInactiveError" DOMException.
        // https://w3c.github.io/IndexedDB/#transaction-inactive
        // A transaction is in this state after control returns to the event loop after its creation, and when events are not being dispatched.
        // No requests can be made against the transaction when it is in this state.
        if !transaction.is_active() || !transaction.is_usable() {
            return Err(Error::TransactionInactive(None));
        }

        Ok(())
    }

    /// Checks if the transaction is active, throwing a "TransactionInactiveError" DOMException if not.
    /// it then checks if the transaction is a read-only transaction, throwing a "ReadOnlyError" DOMException if so.
    fn check_readwrite_transaction_active(&self) -> Fallible<()> {
        // Let transaction be this object store handle's transaction.
        let transaction = &self.transaction;

        // If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        if let IDBTransactionMode::Readonly = transaction.get_mode() {
            return Err(Error::ReadOnly(None));
        }
        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#add-or-put>
    fn put(
        &self,
        cx: &mut JSContext,
        value: HandleValue,
        key: HandleValue,
        no_overwrite: bool,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be handle’s transaction.
        // Step 2. Let store be handle’s object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        // Step 5. If transaction is a read-only transaction, throw a "ReadOnlyError" DOMException.
        self.check_readwrite_transaction_active()?;

        // Step 6. If store uses in-line keys and key was given, throw a "DataError"
        // DOMException.
        if !key.is_undefined() && self.uses_inline_keys() {
            return Err(Error::Data(None));
        }

        // Step 7. If store uses out-of-line keys and has no key generator and key
        // was not given, throw a "DataError" DOMException.
        if !self.uses_inline_keys() && !self.has_key_generator() && key.is_undefined() {
            return Err(Error::Data(None));
        }

        // Step 8. If key was given, then:
        let mut serialized_key = None;

        if !key.is_undefined() {
            // Step 8.1. Let r be the result of converting a value to a key with key.
            // Rethrow any exceptions.
            let key = convert_value_to_key(cx, key, None)?.into_result()?;
            // Step 8.2. If r is "invalid value" or "invalid type", throw a
            // "DataError" DOMException.
            // Handled by `into_result()` above.
            // Step 8.3. Let key be r.
            serialized_key = Some(key);
        }

        // Step 9. Let targetRealm be a user-agent defined Realm.
        // Step 10. Let clone be a clone of value in targetRealm during transaction.
        // Rethrow any exceptions.
        rooted!(&in(cx) let mut cloned_js_value = NullValue());
        self.clone_value_in_target_realm(cx, value, cloned_js_value.handle_mut())?;

        // Step 11. If store uses in-line keys, then:
        if let Some(key_path) = self.key_path.as_ref() {
            // Step 11.1. Let kpk be the result of extracting a key from a value using a key
            // path with clone and store’s key path. Rethrow any exceptions.
            match extract_key(cx, cloned_js_value.handle(), key_path, None)? {
                // Step 11.2. If kpk is invalid, throw a "DataError" DOMException.
                ExtractionResult::Invalid => return Err(Error::Data(None)),
                // Step 11.3. If kpk is not failure, let key be kpk.
                ExtractionResult::Key(kpk) => serialized_key = Some(kpk),
                // Step 11.4. Otherwise (kpk is failure):
                ExtractionResult::Failure => {
                    // Step 11.4.1. If store does not have a key generator, throw a
                    // "DataError" DOMException.
                    if !self.has_key_generator() {
                        return Err(Error::Data(None));
                    }
                    let KeyPath::String(key_path) = key_path else {
                        return Err(Error::Data(None));
                    };
                    // Step 11.4.2. If check that a key could be injected into a value with
                    // clone and store’s key path return false, throw a "DataError"
                    // DOMException.
                    if !can_inject_key_into_value(cx, cloned_js_value.handle(), key_path)? {
                        return Err(Error::Data(None));
                    }
                },
            }
        }
        let cloned_value = structuredclone::write(cx, cloned_js_value.handle(), None)?;
        let Ok(serialized_value) = postcard::to_stdvec(&cloned_value) else {
            return Err(Error::InvalidState(None));
        };
        // Step 12. Let operation be an algorithm to run store a record into an object store with
        // store, clone, key, and no-overwrite flag.
        // NOTE: The backend generates the key if none was given, and the value is stored without
        // it: reading the value injects the key, see `deserialize_value`. The index keys that the
        // operation's step 5 extracts from the clone are computed here.
        let index_keys = self.index_keys(cx, cloned_js_value.handle(), serialized_key.is_none());
        self.transaction.note_records_added(self.id);
        // Step 13. Return the result (an IDBRequest) of running asynchronously execute a request
        // with handle and operation.
        IDBRequest::execute_async(
            cx,
            RequestSource::ObjectStore(self),
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                key: serialized_key,
                value: serialized_value,
                should_overwrite: !no_overwrite,
                index_keys,
            }),
            None,
            None,
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-opencursor>
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-openkeycursor>
    fn open_cursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
        key_only: bool,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this object store handle's transaction.
        // Step 2. Let store be this object store handle's object store.

        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of running the steps to convert a value to a key range
        // with query. Rethrow any exceptions.
        //
        // The query parameter may be a key or an IDBKeyRange to use as the cursor's range. If null
        // or not given, an unbounded key range is used.
        let range = convert_value_to_key_range(cx, query, Some(false))?;

        // Step 6. Let cursor be a new cursor with transaction set to transaction, an undefined
        // position, direction set to direction, got value flag unset, and undefined key and value.
        // The source of cursor is store. The range of cursor is range.
        //
        // NOTE: A cursor that has the key only flag unset implements the IDBCursorWithValue
        // interface as well.
        let cursor = if key_only {
            IDBCursor::new(
                cx,
                &self.global(),
                &self.transaction,
                direction,
                false,
                ObjectStoreOrIndex::ObjectStore(Dom::from_ref(self)),
                range,
                key_only,
            )
        } else {
            DomRoot::upcast(IDBCursorWithValue::new(
                cx,
                &self.global(),
                &self.transaction,
                direction,
                false,
                ObjectStoreOrIndex::ObjectStore(Dom::from_ref(self)),
                range,
                key_only,
            ))
        };

        // Step 7. Run the steps to asynchronously execute a request and return the IDBRequest
        // created by these steps. The steps are run with this object store handle as source and
        // the steps to iterate a cursor as operation, using the current Realm as targetRealm, and
        // cursor.
        let iteration_param = IterationParam {
            cursor: Trusted::new(&cursor),
        };

        IDBRequest::execute_async(
            cx,
            RequestSource::ObjectStore(self),
            cursor.iterate_operation(None, None, 1),
            None,
            Some(iteration_param),
        )
        .inspect(|request| cursor.set_request(request))
    }

    /// Adds a handle of the index `index` to this handle's index set.
    fn add_index(&self, cx: &mut JSContext, index: &IndexedDBIndex) -> DomRoot<IDBIndex> {
        let handle = IDBIndex::new(
            cx,
            &self.global(),
            self,
            index.id,
            index.name.clone(),
            index.multi_entry,
            index.unique,
            index.key_path.clone().into(),
        );
        self.index_set
            .safe_borrow_mut(cx.no_gc())
            .insert(NoTrace(index.name.clone()), Dom::from_ref(&handle));
        self.index_handles
            .safe_borrow_mut(cx.no_gc())
            .push(Dom::from_ref(&handle));
        handle
    }

    pub(crate) fn has_index(&self, name: &Utf16String) -> bool {
        self.index_set.borrow().contains_key(&NoTrace(name.clone()))
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-deleteobjectstore> step 6.
    pub(crate) fn clear_index_set(&self) {
        self.index_set.borrow_mut().clear();
    }

    /// Sends the schema operation `operation` of this handle's object store to the backend, in
    /// the handle's transaction.
    fn send_schema_operation(&self, operation: AsyncSchemaOperation) -> Fallible<()> {
        self.transaction.note_write(None);
        self.get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.db_name.clone(),
                store_id: self.id,
                operation,
                transaction: self.transaction.id(),
            })
            .map_err(|_| {
                Error::Operation(Some(
                    "Failed to send a schema operation to the backend".to_owned(),
                ))
            })
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-name> step 9: renames the index `index`,
    /// named `name` in the index set, to `new_name`. The caller must ensure that it exists.
    pub(crate) fn rename_index(
        &self,
        index: IndexedDBIndexId,
        name: &Utf16String,
        new_name: &Utf16String,
    ) {
        self.transaction
            .get_db()
            .rename_index(self.id, index, new_name.clone());
        let operation = AsyncSchemaOperation::RenameIndex {
            index_id: index,
            new_name: new_name.clone(),
        };
        if self.send_schema_operation(operation).is_err() {
            warn!("Could not send AsyncSchemaOperation");
        }

        // We also need to update the key in the index set
        let index = self
            .index_set
            .borrow_mut()
            .remove(&NoTrace(name.clone()))
            .expect("Earlier steps of the algorithm checked that the index exists")
            .as_rooted();
        self.index_set
            .borrow_mut()
            .insert(NoTrace(new_name.clone()), Dom::from_ref(&index));
    }
}

impl IDBObjectStoreMethods<crate::DomTypeHolder> for IDBObjectStore {
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-put>
    fn Put(
        &self,
        cx: &mut JSContext,
        value: HandleValue,
        key: HandleValue,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of running add or put with this, value, key and the
        // no-overwrite flag false.
        self.put(cx, value, key, false)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-add>
    fn Add(
        &self,
        cx: &mut JSContext,
        value: HandleValue,
        key: HandleValue,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of running add or put with this, value, key and the
        // no-overwrite flag true.
        self.put(cx, value, key, true)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-delete>
    fn Delete(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        // Step 5. If transaction is a read-only transaction, throw a "ReadOnlyError" DOMException.
        self.check_readwrite_transaction_active()?;

        // Step 6. Let range be the result of running the steps to convert a value to a key range with query and null disallowed flag set. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, Some(true));
        // Step 7. Let operation be an algorithm to run delete records from an object store with store and range.
        // Step 8. Return the result (an IDBRequest) of running asynchronously execute a request with this and operation.
        serialized_query.and_then(|key_range| {
            IDBRequest::execute_async(
                cx,
                RequestSource::ObjectStore(self),
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem { key_range }),
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-clear>
    fn Clear(&self, cx: &mut JSContext) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        // Step 5. If transaction is a read-only transaction, throw a "ReadOnlyError" DOMException.
        self.check_readwrite_transaction_active()?;

        // Step 6. Let operation be an algorithm to run clear an object store with store.
        // Step 7. Return the result (an IDBRequest) of running asynchronously execute a request with this and operation.
        IDBRequest::execute_async(
            cx,
            RequestSource::ObjectStore(self),
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear),
            None,
            None,
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-get>
    fn Get(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query and true. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, Some(true));

        // Step 6. Let operation be an algorithm to run retrieve a value from an object store with the current Realm record, store, and range.
        // Step 7. Return the result (an IDBRequest) of running asynchronously execute a request with this and operation.
        serialized_query.and_then(|q| {
            IDBRequest::execute_async(
                cx,
                RequestSource::ObjectStore(self),
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                    index: None,
                    key_range: q,
                }),
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-getkey>
    fn GetKey(&self, cx: &mut JSContext, query: HandleValue) -> Result<DomRoot<IDBRequest>, Error> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query and true. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, Some(true));

        // Step 6. Run the steps to asynchronously execute a request and return the IDBRequest created by these steps.
        // The steps are run with this object store handle as source and the steps to retrieve a key from an object
        // store as operation, using store and range.
        serialized_query.and_then(|q| {
            IDBRequest::execute_async(
                cx,
                RequestSource::ObjectStore(self),
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetKey {
                    index: None,
                    key_range: q,
                }),
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-getall>
    fn GetAll(
        &self,
        cx: &mut JSContext,
        query_or_options: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of creating a request to retrieve multiple items with the
        // current Realm record, this, "value", queryOrOptions, and count if given. Rethrow any
        // exceptions.
        IDBRequest::retrieve_multiple_items(
            cx,
            RequestSource::ObjectStore(self),
            RetrievalKind::Value,
            QueryOrOptions::Arguments(query_or_options, count),
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-getallkeys>
    fn GetAllKeys(
        &self,
        cx: &mut JSContext,
        query_or_options: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of creating a request to retrieve multiple items with the
        // current Realm record, this, "key", queryOrOptions, and count if given. Rethrow any
        // exceptions.
        IDBRequest::retrieve_multiple_items(
            cx,
            RequestSource::ObjectStore(self),
            RetrievalKind::Key,
            QueryOrOptions::Arguments(query_or_options, count),
        )
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbobjectstore-getallrecords>
    fn GetAllRecords(
        &self,
        cx: &mut JSContext,
        options: &IDBGetAllOptions,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of creating a request to retrieve multiple items with the
        // current Realm record, this, "record", and options. Rethrow any exceptions.
        IDBRequest::retrieve_multiple_items(
            cx,
            RequestSource::ObjectStore(self),
            RetrievalKind::Record,
            QueryOrOptions::Options(options),
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-count>
    fn Count(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, None);

        // Step 6. Let operation be an algorithm to run count the records in a range with store and range.
        // Step 7. Return the result (an IDBRequest) of running asynchronously execute a request with this and operation.
        serialized_query.and_then(|q| {
            IDBRequest::execute_async(
                cx,
                RequestSource::ObjectStore(self),
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                    index: None,
                    key_range: q,
                }),
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-opencursor>
    fn OpenCursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
    ) -> Fallible<DomRoot<IDBRequest>> {
        self.open_cursor(cx, query, direction, false)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-openkeycursor>
    fn OpenKeyCursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
    ) -> Fallible<DomRoot<IDBRequest>> {
        self.open_cursor(cx, query, direction, true)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-name>
    fn Name(&self) -> DOMString {
        dom_string_from_name(&self.name.borrow())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-name>
    fn SetName(&self, value: DOMString) -> ErrorResult {
        // Step 1. Let name be the given value.
        let name = name_from_dom_string(&value);

        // Step 2. Let transaction be this’s transaction.
        let transaction = &self.transaction;

        // Step 3. Let store be this’s object store.
        // Step 4. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 5. If transaction is not an upgrade transaction, throw an "InvalidStateError" DOMException.
        if transaction.Mode() != IDBTransactionMode::Versionchange {
            return Err(Error::InvalidState(None));
        }
        // Step 6. If transaction’s state is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 7. If store’s name is equal to name, terminate these steps.
        if *self.name.borrow() == name {
            return Ok(());
        }

        // Step 8. If an object store named name already exists in store’s database,
        // throw a "ConstraintError" DOMException.
        if transaction.get_db().object_store_exists(&name) {
            return Err(Error::Constraint(None));
        }

        // Step 9. Set store’s name to name.
        // The records and indexes of the store stay with it: the backend renames the store's row.
        transaction
            .get_db()
            .rename_object_store(self.id, name.clone());
        self.send_schema_operation(AsyncSchemaOperation::RenameObjectStore {
            new_name: name.clone(),
        })?;

        // Step 10. Set this’s name to name.
        *self.name.borrow_mut() = name;
        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-keypath>
    fn KeyPath(&self, cx: &mut JSContext, mut ret_val: MutableHandleValue) {
        // NOTE: If this attribute returns an object (specifically an Array), it returns the same
        // object instance every time it is inspected.
        match &self.key_path {
            Some(key_path) => key_path.to_jsval_cached(cx, &self.key_path_value, ret_val),
            None => ret_val.set(NullValue()),
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-indexnames>
    fn IndexNames(&self, cx: &mut JSContext) -> DomRoot<DOMStringList> {
        let names: Vec<Utf16String> = self
            .index_set
            .borrow()
            .keys()
            .map(|name| name.0.clone())
            .collect();
        sorted_name_list(cx, &self.global(), names)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-transaction>
    fn Transaction(&self) -> DomRoot<IDBTransaction> {
        self.transaction()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-autoincrement>
    fn AutoIncrement(&self) -> bool {
        self.has_key_generator()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-createindex>
    fn CreateIndex(
        &self,
        cx: &mut JSContext,
        name: DOMString,
        key_path: StringOrStringSequence,
        options: &IDBIndexParameters,
    ) -> Fallible<DomRoot<IDBIndex>> {
        let key_path: KeyPath = key_path.into();
        // Step 3. If transaction is not an upgrade transaction, throw an "InvalidStateError" DOMException.
        if self.transaction.Mode() != IDBTransactionMode::Versionchange {
            return Err(Error::InvalidState(None));
        }

        // Step 4. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;
        // Step 5. If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 6. If an index named name already exists in store, throw a "ConstraintError" DOMException.
        let name = name_from_dom_string(&name);
        if self.has_index(&name) {
            return Err(Error::Constraint(None));
        }

        let js_key_path = match key_path.clone() {
            KeyPath::String(s) => StringOrStringSequence::String(s),
            KeyPath::StringSequence(s) => StringOrStringSequence::StringSequence(s),
        };

        // Step 7. If keyPath is not a valid key path, throw a "SyntaxError" DOMException.
        if !is_valid_key_path(cx, &js_key_path)? {
            return Err(Error::Syntax(None));
        }
        // Step 8. Let unique be set if options’s unique member is true, and unset otherwise.
        // Step 9. Let multiEntry be set if options’s multiEntry member is true, and unset otherwise.
        // Step 10. If keyPath is a sequence and multiEntry is set, throw an "InvalidAccessError" DOMException.
        if matches!(key_path, KeyPath::StringSequence(_)) && options.multiEntry {
            return Err(Error::InvalidAccess(None));
        }

        // Step 11. Let index be a new index in store.
        // Set index’s name to name and key path to keyPath. If unique is set, set index’s unique flag.
        // If multiEntry is set, set index’s multiEntry flag.
        let db = self.transaction.get_db();
        let index = IndexedDBIndex {
            id: db.allocate_index_id(),
            name,
            key_path: key_path.into(),
            unique: options.unique,
            multi_entry: options.multiEntry,
        };
        // NOTE: The index gets records for the store's records, which creating it "processed as
        // an asynchronous request within the upgrade transaction" computes, and the backend holds
        // the transaction's later requests back until then. A store that the transaction created
        // and that no record was added to has none.
        let populate = !self.transaction.is_new_empty_store(self.id);
        self.send_schema_operation(AsyncSchemaOperation::CreateIndex {
            index_id: index.id,
            name: index.name.clone(),
            key_path: index.key_path.clone(),
            unique: index.unique,
            multi_entry: index.multi_entry,
            populate,
        })?;
        db.add_index(self.id, index.clone());
        if populate {
            self.transaction.start_index_population(self, index.clone());
        }

        // Step 12. Add index to this object store handle's index set.
        let index = self.add_index(cx, &index);

        // Step 13. Return a new index handle associated with index and this object store handle.
        Ok(index)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-deleteindex>
    fn DeleteIndex(&self, name: DOMString) -> Fallible<()> {
        // Step 3. If transaction is not an upgrade transaction, throw an "InvalidStateError" DOMException.
        if self.transaction.Mode() != IDBTransactionMode::Versionchange {
            return Err(Error::InvalidState(None));
        }
        // Step 4. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;
        // Step 5. If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;
        // Step 6. Let index be the index named name in store if one exists,
        // or throw a "NotFoundError" DOMException otherwise.
        let name = NoTrace(name_from_dom_string(&name));
        let Some(index) = self
            .index_set
            .borrow()
            .get(&name)
            .map(|index| index.as_rooted())
        else {
            return Err(Error::NotFound(None));
        };
        // Step 7. Remove index from this object store handle's index set.
        self.index_set.borrow_mut().remove(&name);
        // Step 8. Destroy index.
        self.transaction.get_db().remove_index(self.id, index.id());
        self.send_schema_operation(AsyncSchemaOperation::DeleteIndex {
            index_id: index.id(),
        })
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbobjectstore-index>
    fn Index(&self, name: DOMString) -> Fallible<DomRoot<IDBIndex>> {
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If the transaction's state is finished, then throw an "InvalidStateError" DOMException.
        if self.transaction.is_finished() {
            return Err(Error::InvalidState(None));
        }

        // Step 5. Let index be the index named name in this’s index set if one exists, or throw a "NotFoundError" DOMException otherwise.
        let index_set = self.index_set.borrow();
        let index = index_set
            .get(&NoTrace(name_from_dom_string(&name)))
            .ok_or(Error::NotFound(None))?;

        // Step 6. Return an index handle associated with index and this.
        Ok(index.as_rooted())
    }
}
