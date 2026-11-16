/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use dom_struct::dom_struct;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::gc::MutableHandleValue;
use js::rust::HandleValue;
use script_bindings::cell::DomRefCell;
use script_bindings::codegen::GenericBindings::IDBIndexBinding::IDBIndexMethods;
use script_bindings::codegen::GenericBindings::IDBTransactionBinding::IDBTransactionMode;
use script_bindings::error::{Error, ErrorResult, Fallible};
use script_bindings::reflector::{Reflector, reflect_dom_object};
use script_bindings::str::DOMString;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, IndexedDBIndexId, Utf16String,
};

use crate::dom::bindings::codegen::Bindings::IDBCursorBinding::IDBCursorDirection;
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot};
use crate::dom::globalscope::GlobalScope;
use crate::dom::idbobjectstore::KeyPath;
use crate::dom::indexeddb::idbcursor::{IDBCursor, IterationParam, ObjectStoreOrIndex};
use crate::dom::indexeddb::idbcursorwithvalue::IDBCursorWithValue;
use crate::dom::indexeddb::idbobjectstore::IDBObjectStore;
use crate::dom::indexeddb::idbrequest::{IDBRequest, RequestSource, RetrievalKind};
use crate::dom::indexeddb::key::{
    convert_value_to_key_range, dom_string_from_name, name_from_dom_string,
};

#[dom_struct]
pub(crate) struct IDBIndex {
    reflector_: Reflector,
    object_store: Dom<IDBObjectStore>,
    /// The id of the handle's index.
    id: IndexedDBIndexId,
    #[no_trace]
    name: DomRefCell<Utf16String>,
    multi_entry: bool,
    unique: bool,
    key_path: KeyPath,
}

impl IDBIndex {
    pub fn new_inherited(
        object_store: &IDBObjectStore,
        id: IndexedDBIndexId,
        name: Utf16String,
        multi_entry: bool,
        unique: bool,
        key_path: KeyPath,
    ) -> IDBIndex {
        IDBIndex {
            reflector_: Reflector::new(),
            object_store: Dom::from_ref(object_store),
            id,
            name: DomRefCell::new(name),
            multi_entry,
            unique,
            key_path,
        }
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        object_store: &IDBObjectStore,
        id: IndexedDBIndexId,
        name: Utf16String,
        multi_entry: bool,
        unique: bool,
        key_path: KeyPath,
    ) -> DomRoot<IDBIndex> {
        reflect_dom_object(
            cx,
            Box::new(IDBIndex::new_inherited(
                object_store,
                id,
                name,
                multi_entry,
                unique,
                key_path,
            )),
            global,
        )
    }

    pub(crate) fn id(&self) -> IndexedDBIndexId {
        self.id
    }

    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction> step 6: reverts the
    /// handle's name.
    pub(crate) fn set_name(&self, name: Utf16String) {
        *self.name.borrow_mut() = name;
    }

    /// <https://w3c.github.io/IndexedDB/#index-handle-name>
    pub(crate) fn name(&self) -> Utf16String {
        self.name.borrow().clone()
    }

    pub(crate) fn object_store(&self) -> DomRoot<IDBObjectStore> {
        DomRoot::from_ref(&self.object_store)
    }

    pub(crate) fn verify_not_deleted(&self) -> ErrorResult {
        let transaction = self.object_store.transaction();
        if !transaction
            .get_db()
            .has_index(self.object_store.id(), self.id)
        {
            return Err(Error::InvalidState(Some(
                "Index or its object store has been deleted".to_owned(),
            )));
        }
        Ok(())
    }

    fn check_transaction_active(&self) -> ErrorResult {
        let transaction = self.object_store.transaction();
        if !transaction.is_active() || !transaction.is_usable() {
            return Err(Error::TransactionInactive(Some(
                "Transaction is not active".to_owned(),
            )));
        }
        Ok(())
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-get> and
    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-getkey>, which differ in their operation.
    fn get(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        key_only: bool,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let index be this’s index.
        // Step 3. If index or index’s object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError"
        // DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query and
        // true. Rethrow any exceptions.
        let key_range = convert_value_to_key_range(cx, query, Some(true))?;

        // Step 6. Let operation be an algorithm to run retrieve a referenced value from an index
        // with the current Realm record, index, and range (get), or retrieve a value from an
        // index with index and range (getKey).
        let index = Some(self.id);
        let operation = if key_only {
            AsyncReadOnlyOperation::GetKey { index, key_range }
        } else {
            AsyncReadOnlyOperation::GetItem { index, key_range }
        };

        // Step 7. Return the result (an IDBRequest) of running asynchronously execute a request
        // with this and operation.
        IDBRequest::execute_async(
            cx,
            RequestSource::Index(self),
            AsyncOperation::ReadOnly(operation),
            None,
            None,
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbindex-opencursor>
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbindex-openkeycursor>
    fn open_cursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
        key_only: bool,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s object store's transaction.
        let transaction = self.object_store.transaction();

        // Step 2. If index or its object store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 3. If transaction is not active, throw a "TransactionInactiveError" DOMException.
        self.check_transaction_active()?;

        // Step 4. Let range be the result of running the steps to convert a value to a key range with query.
        let range = convert_value_to_key_range(cx, query, Some(false))?;

        // Step 5. Let cursor be a new cursor with transaction, direction, source: Index, range.
        let cursor = if key_only {
            IDBCursor::new(
                cx,
                &self.global(),
                &transaction,
                direction,
                false,
                ObjectStoreOrIndex::Index(Dom::from_ref(self)),
                range,
                key_only,
            )
        } else {
            DomRoot::upcast(IDBCursorWithValue::new(
                cx,
                &self.global(),
                &transaction,
                direction,
                false,
                ObjectStoreOrIndex::Index(Dom::from_ref(self)),
                range,
                key_only,
            ))
        };

        // Step 6. Asynchronously execute a request with iteration_param.
        let iteration_param = IterationParam {
            cursor: Trusted::new(&cursor),
            key: None,
            primary_key: None,
            count: None,
        };

        IDBRequest::execute_async(
            cx,
            RequestSource::Index(self),
            cursor.iterate_operation(None, 1),
            None,
            Some(iteration_param),
        )
        .inspect(|request| cursor.set_request(request))
    }
}

impl IDBIndexMethods<crate::DomTypeHolder> for IDBIndex {
    /// <https://www.w3.org/TR/IndexedDB/#dom-idbindex-name>
    fn Name(&self) -> DOMString {
        dom_string_from_name(&self.name.borrow())
    }

    /// <https://www.w3.org/TR/IndexedDB/#ref-for-dom-idbindex-name%E2%91%A2>
    fn SetName(&self, name: DOMString) -> ErrorResult {
        // Step 1: Let name be the given value.
        let name = name_from_dom_string(&name);
        // Step 2: Let transaction be this’s transaction.
        let transaction = self.object_store.transaction();

        // Step 3: Let index be this’s index.
        // We do not have an explicit object representing the underlying index.

        // Step 4: If transaction is not an upgrade transaction, throw an "InvalidStateError" DOMException.
        if transaction.get_mode() != IDBTransactionMode::Versionchange {
            return Err(Error::InvalidState(Some(
                "Transaction is not an upgrade transaction".to_owned(),
            )));
        }

        // Step 5: If transaction’s state is not active, then throw a "TransactionInactiveError" DOMException.
        if !transaction.is_active() {
            return Err(Error::TransactionInactive(Some(
                "Transaction is not active while updating index name".to_owned(),
            )));
        }

        // Step 6: If index or index’s object store has been deleted, throw an "InvalidStateError" DOMException.
        self.verify_not_deleted()?;
        let mut stored_name = self.name.borrow_mut();

        // Step 7: If index’s name is equal to name, terminate these steps.
        if *stored_name == name {
            return Ok(());
        }

        // Step 8: If an index named name already exists in index’s object store, throw a "ConstraintError" DOMException.
        if self.object_store.has_index(&name) {
            return Err(Error::Constraint(Some(
                "An index with the given name already exists".to_owned(),
            )));
        }

        // Step 9: Set index’s name to name.
        self.object_store.rename_index(self.id, &stored_name, &name);

        // Step 10: Set this’s name to name.
        *stored_name = name;
        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbindex-objectstore>
    fn ObjectStore(&self) -> DomRoot<IDBObjectStore> {
        self.object_store.as_rooted()
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbindex-multientry>
    fn MultiEntry(&self) -> bool {
        self.multi_entry
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbindex-unique>
    fn Unique(&self) -> bool {
        self.unique
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbindex-keypath>
    fn KeyPath(&self, cx: &mut JSContext, retval: MutableHandleValue) {
        match &self.key_path {
            KeyPath::String(string) => {
                string.to_jsval(cx, retval);
            },
            KeyPath::StringSequence(sequence) => {
                sequence.to_jsval(cx, retval);
            },
        }
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-get>
    fn Get(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        self.get(cx, query, false)
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-getkey>
    fn GetKey(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        self.get(cx, query, true)
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-getall>
    fn GetAll(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of creating a request to retrieve multiple items with the
        // current Realm record, this, "value", queryOrOptions, and count if given. Rethrow any
        // exceptions.
        IDBRequest::retrieve_multiple_items(
            cx,
            RequestSource::Index(self),
            RetrievalKind::Value,
            query,
            count,
        )
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-getallkeys>
    fn GetAllKeys(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        count: Option<u32>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Return the result of creating a request to retrieve multiple items with the
        // current Realm record, this, "key", queryOrOptions, and count if given. Rethrow any
        // exceptions.
        IDBRequest::retrieve_multiple_items(
            cx,
            RequestSource::Index(self),
            RetrievalKind::Key,
            query,
            count,
        )
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbindex-count>
    fn Count(&self, cx: &mut JSContext, query: HandleValue) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1. Let transaction be this’s transaction.
        // Step 2. Let index be this’s index.
        // Step 3. If index or index’s object store has been deleted, throw an
        // "InvalidStateError" DOMException.
        self.verify_not_deleted()?;

        // Step 4. If transaction’s state is not active, then throw a "TransactionInactiveError"
        // DOMException.
        self.check_transaction_active()?;

        // Step 5. Let range be the result of converting a value to a key range with query.
        // Rethrow any exceptions.
        let key_range = convert_value_to_key_range(cx, query, None)?;

        // Step 6. Let operation be an algorithm to run count the records in a range with index
        // and range.
        let operation = AsyncReadOnlyOperation::Count {
            index: Some(self.id),
            key_range,
        };

        // Step 7. Return the result (an IDBRequest) of running asynchronously execute a request
        // with this and operation.
        IDBRequest::execute_async(
            cx,
            RequestSource::Index(self),
            AsyncOperation::ReadOnly(operation),
            None,
            None,
        )
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbindex-opencursor>
    fn OpenCursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
    ) -> Fallible<DomRoot<IDBRequest>> {
        self.open_cursor(cx, query, direction, false)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbindex-openkeycursor>
    fn OpenKeyCursor(
        &self,
        cx: &mut JSContext,
        query: HandleValue,
        direction: IDBCursorDirection,
    ) -> Fallible<DomRoot<IDBRequest>> {
        self.open_cursor(cx, query, direction, true)
    }
}
