/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::collections::HashMap;

use dom_struct::dom_struct;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::gc::MutableHandleValue;
use js::jsval::{NullValue, UndefinedValue};
use js::rust::HandleValue;
use script_bindings::cell::DomRefCell;
use script_bindings::codegen::GenericBindings::IDBObjectStoreBinding::IDBIndexParameters;
use script_bindings::codegen::GenericUnionTypes::StringOrStringSequence;
use script_bindings::domstring::TracedDOMString;
use script_bindings::error::ErrorResult;
use script_bindings::reflector::{Reflector, reflect_dom_object};
use servo_base::generic_channel::{GenericSend, GenericSender};
use storage_traits::indexeddb::{
    self, AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    IndexedDBKeyType, IndexedDBThreadMsg,
};

use crate::dom::bindings::codegen::Bindings::IDBCursorBinding::IDBCursorDirection;
use crate::dom::bindings::codegen::Bindings::IDBDatabaseBinding::IDBObjectStoreParameters;
use crate::dom::bindings::codegen::Bindings::IDBObjectStoreBinding::IDBObjectStoreMethods;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::{
    IDBTransactionMethods, IDBTransactionMode,
};
// We need to alias this name, otherwise test-tidy complains at &String reference.
use crate::dom::bindings::codegen::UnionTypes::StringOrStringSequence as StrOrStringSequence;
use crate::dom::bindings::error::{Error, Fallible};
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot};
use crate::dom::bindings::str::DOMString;
use crate::dom::bindings::structuredclone;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbcursor::{IDBCursor, IterationParam, ObjectStoreOrIndex};
use crate::dom::indexeddb::idbcursorwithvalue::IDBCursorWithValue;
use crate::dom::indexeddb::idbindex::IDBIndex;
use crate::dom::indexeddb::idbrequest::IDBRequest;
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::key::{
    EvaluationResult, ExtractionResult, can_inject_key_into_value, convert_value_to_key,
    convert_value_to_key_range, evaluate_key_path_on_value, extract_key, inject_key_into_value,
    injected_key_is_stored, is_valid_key_path,
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

#[derive(Clone, JSTraceable, MallocSizeOf)]
struct IDBObjectStoreRollbackState {
    newly_created_during_transaction: bool,
    rollback_name: Option<DOMString>,
    #[no_trace]
    rollback_indexes: Vec<indexeddb::IndexedDBIndex>,
}

#[dom_struct]
pub struct IDBObjectStore {
    reflector_: Reflector,
    name: DomRefCell<TracedDOMString>,
    key_path: Option<KeyPath>,
    index_set: DomRefCell<HashMap<TracedDOMString, Dom<IDBIndex>>>,
    abort_state_on_abort: DomRefCell<Option<IDBObjectStoreRollbackState>>,
    transaction: Dom<IDBTransaction>,
    has_key_generator: bool,

    // We store the db name in the object store to address backend operations
    // that are keyed by (origin, database name, object store name).
    db_name: TracedDOMString,
}

pub(crate) struct IDBObjectStoreAbortState {
    pub(crate) newly_created_during_transaction: bool,
    pub(crate) rollback_indexes_on_abort: Vec<indexeddb::IndexedDBIndex>,
}

impl IDBObjectStore {
    pub fn new_inherited(
        db_name: DOMString,
        name: DOMString,
        options: Option<&IDBObjectStoreParameters>,
        abort_state: IDBObjectStoreAbortState,
        transaction: &IDBTransaction,
    ) -> IDBObjectStore {
        let key_path: Option<KeyPath> = match options {
            Some(options) => options.keyPath.as_ref().map(|path| match path {
                StrOrStringSequence::String(inner) => KeyPath::String(inner.clone()),
                StrOrStringSequence::StringSequence(inner) => {
                    KeyPath::StringSequence(inner.clone())
                },
            }),
            None => None,
        };
        let has_key_generator = options.is_some_and(|options| options.autoIncrement);
        let IDBObjectStoreAbortState {
            newly_created_during_transaction,
            rollback_indexes_on_abort,
        } = abort_state;

        IDBObjectStore {
            reflector_: Reflector::new(),
            name: DomRefCell::new(name.traced()),
            key_path,
            index_set: DomRefCell::new(HashMap::new()),
            abort_state_on_abort: DomRefCell::new(Some(IDBObjectStoreRollbackState {
                newly_created_during_transaction,
                rollback_name: None,
                rollback_indexes: rollback_indexes_on_abort,
            })),
            transaction: Dom::from_ref(transaction),
            has_key_generator,
            db_name: db_name.traced(),
        }
    }

    pub fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        db_name: DOMString,
        name: DOMString,
        options: Option<&IDBObjectStoreParameters>,
        abort_state: IDBObjectStoreAbortState,
        transaction: &IDBTransaction,
    ) -> DomRoot<IDBObjectStore> {
        reflect_dom_object(
            cx,
            Box::new(IDBObjectStore::new_inherited(
                db_name,
                name,
                options,
                abort_state,
                transaction,
            )),
            global,
        )
    }

    pub fn get_name(&self) -> DOMString {
        self.name.borrow().clone().root()
    }

    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>
    pub(crate) fn restore_metadata_after_abort(&self, cx: &mut JSContext) {
        let Some(abort_state) = self.abort_state_on_abort.borrow().as_ref().cloned() else {
            return;
        };

        // Step 5.1. If handle’s object store was not newly created during transaction,
        // set handle’s name to its object store’s name.
        if !abort_state.newly_created_during_transaction &&
            let Some(name) = abort_state.rollback_name
        {
            *self.name.safe_borrow_mut(cx.no_gc()) = name.traced();
        }

        // Step 5.2. Set handle’s index set to the set of indexes that reference
        // its object store.
        self.index_set.safe_borrow_mut(cx.no_gc()).clear();
        for index in abort_state.rollback_indexes {
            self.add_index(
                cx,
                index.name.clone().into(),
                &IDBIndexParameters {
                    multiEntry: index.multi_entry,
                    unique: index.unique,
                },
                index.key_path.clone().into(),
            );
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

    /// <https://www.w3.org/TR/IndexedDB-3/#object-store-in-line-keys>
    pub(crate) fn uses_inline_keys(&self) -> bool {
        self.key_path.is_some()
    }

    pub(crate) fn key_path(&self) -> Option<&KeyPath> {
        self.key_path.as_ref()
    }

    pub(crate) fn verify_not_deleted(&self) -> ErrorResult {
        let db = self.transaction.Db();
        if !db.object_store_exists(&self.name.borrow()) {
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
        // it: reading the value injects the key, see `deserialize_value`.
        // Step 13. Return the result (an IDBRequest) of running asynchronously execute a request
        // with handle and operation.
        IDBRequest::execute_async(
            cx,
            self,
            |callback| {
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                    callback,
                    key: serialized_key,
                    value: serialized_value,
                    should_overwrite: !no_overwrite,
                })
            },
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
            key: None,
            primary_key: None,
            count: None,
        };

        IDBRequest::execute_async(
            cx,
            self,
            |callback| cursor.iterate_operation(callback, None, 1),
            None,
            Some(iteration_param),
        )
        .inspect(|request| cursor.set_request(request))
    }

    pub(crate) fn add_index(
        &self,
        cx: &mut JSContext,
        name: DOMString,
        options: &IDBIndexParameters,
        key_path: KeyPath,
    ) -> DomRoot<IDBIndex> {
        let index = IDBIndex::new(
            cx,
            &self.global(),
            self,
            name.clone(),
            options.multiEntry,
            options.unique,
            key_path,
        );
        self.index_set
            .safe_borrow_mut(cx.no_gc())
            .insert(name.traced(), Dom::from_ref(&index));
        index
    }

    pub(crate) fn has_index(&self, name: &TracedDOMString) -> bool {
        self.index_set.borrow().contains_key(name)
    }

    /// The caller must ensure that the original index exists.
    pub(crate) fn rename_index(&self, name: &TracedDOMString, new_name: &DOMString) {
        let operation = AsyncSchemaOperation::RenameIndex {
            callback: self.transaction.create_abort_callback(),
            index_name: name.to_string(),
            new_name: new_name.to_string(),
        };

        if self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.db_name.to_string(),
                store_name: self.name.borrow().clone().into(),
                operation,
                transaction: self.transaction.id(),
            })
            .is_err()
        {
            warn!("Could not send AsyncSchemaOperation");
        }

        // We also need to update the key in the index set
        let index = self
            .index_set
            .borrow_mut()
            .remove(name)
            .expect("Earlier steps of the algorithm checked that the index exists")
            .as_rooted();
        self.index_set
            .borrow_mut()
            .insert(new_name.clone().traced(), Dom::from_ref(&index));
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
                self,
                |callback| {
                    AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem {
                        callback,
                        key_range,
                    })
                },
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
            self,
            |callback| AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear(callback)),
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
                self,
                |callback| {
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                        callback,
                        key_range: q,
                    })
                },
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
                self,
                |callback| {
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetKey {
                        callback,
                        key_range: q,
                    })
                },
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-getall>
    fn GetAll(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query and true. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, None);

        // Step 6. Run the steps to asynchronously execute a request and return the IDBRequest created by these steps.
        // The steps are run with this object store handle as source and the steps to retrieve a key from an object
        // store as operation, using store and range.
        serialized_query.and_then(|q| {
            IDBRequest::execute_async(
                cx,
                self,
                |callback| {
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllItems {
                        callback,
                        key_range: q,
                        count,
                    })
                },
                None,
                None,
            )
        })
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-getallkeys>
    fn GetAllKeys(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let store be this's object store.
        // Step 3. If store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query and true. Rethrow any exceptions.
        let serialized_query = convert_value_to_key_range(cx, query, None);

        // Step 6. Run the steps to asynchronously execute a request and return the IDBRequest created by these steps.
        // The steps are run with this object store handle as source and the steps to retrieve a key from an object
        // store as operation, using store and range.
        serialized_query.and_then(|q| {
            IDBRequest::execute_async(
                cx,
                self,
                |callback| {
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllKeys {
                        callback,
                        key_range: q,
                        count,
                    })
                },
                None,
                None,
            )
        })
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
                self,
                |callback| {
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                        callback,
                        key_range: q,
                    })
                },
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
        self.name.borrow().clone().root()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-name>
    fn SetName(&self, value: DOMString) -> ErrorResult {
        // Step 1. Let name be the given value.
        let name = value;

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
        if *self.name.borrow() == *name {
            return Ok(());
        }

        // Step 8. If an object store named name already exists in store’s database,
        // throw a "ConstraintError" DOMException.
        if transaction.Db().object_store_exists(&name) {
            return Err(Error::Constraint(None));
        }

        let old_name = self.name.borrow().clone().root();
        if let Some(abort_state) = self.abort_state_on_abort.borrow_mut().as_mut() &&
            abort_state.rollback_name.is_none()
        {
            abort_state.rollback_name = Some(old_name.clone());
        }

        // Step 9. Set store’s name to name.
        transaction
            .Db()
            .rename_object_store_name(&old_name, name.clone());
        // Step 10. Set this’s name to name.
        *self.name.borrow_mut() = name.clone().traced();
        transaction.rename_object_store_handle_cache(&old_name, &name, self);
        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-keypath>
    fn KeyPath(&self, cx: &mut JSContext, mut ret_val: MutableHandleValue) {
        match &self.key_path {
            Some(KeyPath::String(path)) => path.to_jsval(cx, ret_val),
            Some(KeyPath::StringSequence(paths)) => paths.to_jsval(cx, ret_val),
            None => ret_val.set(NullValue()),
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbobjectstore-indexnames>
    fn IndexNames(&self, cx: &mut JSContext) -> DomRoot<DOMStringList> {
        DOMStringList::new_sorted(cx, &self.global(), self.index_set.borrow().keys())
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
        let operation = AsyncSchemaOperation::CreateIndex {
            callback: self.transaction.create_abort_callback(),
            index_name: name.to_string(),
            key_path: key_path.clone().into(),
            unique: options.unique,
            multi_entry: options.multiEntry,
        };

        if self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.db_name.to_string(),
                store_name: self.name.borrow().clone().into(),
                operation,
                transaction: self.transaction.id(),
            })
            .is_err()
        {
            return Err(Error::Operation(None));
        }

        // Step 12. Add index to this object store handle's index set.
        let index = self.add_index(cx, name, options, key_path);

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
        if !self.index_set.borrow().contains_key(&name) {
            return Err(Error::NotFound(None));
        }
        // Step 7. Remove index from this object store handle's index set.
        self.index_set.borrow_mut().retain(|n, _| n != &*name);
        // Step 8. Destroy index.
        let operation = AsyncSchemaOperation::DeleteIndex {
            callback: self.transaction.create_abort_callback(),
            index_name: name.to_string(),
        };
        if self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.db_name.to_string(),
                store_name: self.name.borrow().clone().into(),
                operation,
                transaction: self.transaction.id(),
            })
            .is_err()
        {
            return Err(Error::Operation(None));
        }
        Ok(())
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
        let index = index_set.get(&name).ok_or(Error::NotFound(None))?;

        // Step 6. Return an index handle associated with index and this.
        Ok(index.as_rooted())
    }
}
