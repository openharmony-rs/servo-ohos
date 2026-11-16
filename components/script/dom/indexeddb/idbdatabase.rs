/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::Cell;

use dom_struct::dom_struct;
use js::context::JSContext;
use script_bindings::cell::DomRefCell;
use script_bindings::reflector::reflect_dom_object;
use servo_base::generic_channel::{GenericSend, GenericSender};
use storage_traits::indexeddb::{
    AsyncSchemaOperation, ConnectionEvent, IndexedDBIndex, IndexedDBIndexId, IndexedDBObjectStore,
    IndexedDBObjectStoreId, IndexedDBThreadMsg, IndexedDBTxnId, KeyPath, SyncOperation,
    Utf16String,
};
use stylo_atoms::Atom;
use uuid::Uuid;

use crate::dom::bindings::codegen::Bindings::IDBDatabaseBinding::{
    IDBDatabaseMethods, IDBObjectStoreParameters, IDBTransactionOptions,
};
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::IDBTransactionMode;
use crate::dom::bindings::codegen::UnionTypes::StringOrStringSequence;
use crate::dom::bindings::error::{Error, Fallible};
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot, MutNullableDom};
use crate::dom::bindings::str::DOMString;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::eventtarget::EventTarget;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbobjectstore::IDBObjectStore;
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::idbversionchangeevent::IDBVersionChangeEvent;
use crate::dom::indexeddb::key::{
    dom_string_from_name, is_valid_key_path, name_from_dom_string, sorted_name_list,
};

#[dom_struct]
pub struct IDBDatabase {
    eventtarget: EventTarget,
    /// <https://w3c.github.io/IndexedDB/#database-name>
    #[no_trace]
    name: Utf16String,
    /// <https://w3c.github.io/IndexedDB/#database-version>
    version: Cell<u64>,
    /// <https://w3c.github.io/IndexedDB/#connection-object-store-set>: the object stores and
    /// their indexes as this connection sees them, in the order they were created. Only an
    /// upgrade transaction changes them, and aborting it reverts them.
    #[no_trace]
    object_stores: DomRefCell<Vec<IndexedDBObjectStore>>,
    /// <https://w3c.github.io/IndexedDB/#database-upgrade-transaction>
    upgrade_transaction: MutNullableDom<IDBTransaction>,
    /// The transactions of this connection that haven't finished, which the results on the
    /// connection's result channel are for.
    transactions: DomRefCell<Vec<Dom<IDBTransaction>>>,

    #[no_trace]
    id: Uuid,
    /// The serial number of the next transaction this connection creates; 0 is its upgrade
    /// transaction's.
    next_transaction_serial: Cell<u64>,
    /// The ids of the next object store and index that the upgrade transaction creates: one more
    /// than the largest id the connection has seen, so that a store or index deleted in the
    /// transaction doesn't share its id with one created after it, while handles of both exist.
    next_object_store_id: Cell<IndexedDBObjectStoreId>,
    next_index_id: Cell<IndexedDBIndexId>,

    // Flags
    /// <https://w3c.github.io/IndexedDB/#connection-close-pending-flag>
    close_pending: Cell<bool>,
}

impl IDBDatabase {
    pub fn new_inherited(
        name: Utf16String,
        id: Uuid,
        version: u64,
        object_stores: Vec<IndexedDBObjectStore>,
    ) -> IDBDatabase {
        let largest_store_id = object_stores.iter().map(|store| store.id).max();
        let largest_index_id = object_stores
            .iter()
            .flat_map(|store| store.indexes.iter().map(|index| index.id))
            .max();
        IDBDatabase {
            eventtarget: EventTarget::new_inherited(),
            name,
            id,
            version: Cell::new(version),
            object_stores: DomRefCell::new(object_stores),
            upgrade_transaction: Default::default(),
            transactions: Default::default(),
            next_transaction_serial: Cell::new(1),
            next_object_store_id: Cell::new(largest_store_id.unwrap_or(0) + 1),
            next_index_id: Cell::new(largest_index_id.unwrap_or(0) + 1),
            close_pending: Cell::new(false),
        }
    }

    pub fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        name: Utf16String,
        id: Uuid,
        version: u64,
        object_stores: Vec<IndexedDBObjectStore>,
    ) -> DomRoot<IDBDatabase> {
        reflect_dom_object(
            cx,
            Box::new(IDBDatabase::new_inherited(name, id, version, object_stores)),
            global,
        )
    }

    fn get_idb_thread(&self) -> GenericSender<IndexedDBThreadMsg> {
        self.global().storage_threads().sender()
    }

    /// <https://w3c.github.io/IndexedDB/#database-name>
    pub(crate) fn name(&self) -> &Utf16String {
        &self.name
    }

    /// The id of a new transaction of this connection, unique among them.
    pub(crate) fn allocate_transaction_id(&self) -> IndexedDBTxnId {
        let serial = self.next_transaction_serial.get();
        self.next_transaction_serial.set(serial + 1);
        IndexedDBTxnId {
            connection: self.id,
            serial,
        }
    }

    pub(crate) fn add_transaction(&self, transaction: &IDBTransaction) {
        self.transactions
            .borrow_mut()
            .push(Dom::from_ref(transaction));
    }

    pub(crate) fn remove_transaction(&self, transaction: &IDBTransaction) {
        self.transactions
            .borrow_mut()
            .retain(|entry| &**entry != transaction);
    }

    /// Handles a message from the backend on the connection's result channel.
    pub(crate) fn handle_connection_event(&self, cx: &mut JSContext, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Open(_) => {
                debug_assert!(false, "The open request handles its own messages");
            },
            ConnectionEvent::Transaction { serial, event } => {
                let transaction = self
                    .transactions
                    .borrow()
                    .iter()
                    .find(|transaction| transaction.id().serial == serial)
                    .map(|transaction| transaction.as_rooted());
                match transaction {
                    Some(transaction) => transaction.handle_event(cx, event),
                    None => debug!("IndexedDB result for finished transaction {serial}"),
                }
            },
            ConnectionEvent::VersionChange {
                old_version,
                new_version,
                request,
            } => {
                // https://w3c.github.io/IndexedDB/#open-a-database-connection
                // Step 10.2: For each entry of openConnections that does not have its close
                // pending flag set to true, queue a database task to fire a version change event
                // named versionchange at entry with db’s version and version.
                // https://w3c.github.io/IndexedDB/#delete-a-database
                // Step 6: [...] with db’s version and null.
                // Note: "Firing this event might cause one or more of the other objects in
                // openConnections to be closed, in which case the versionchange event is not
                // fired at those objects, even if that hasn’t yet been done."
                if !self.close_pending.get() {
                    self.dispatch_versionchange(cx, old_version, new_version);
                }

                // Step 10.3: Wait for all of the events to be fired.
                // Note: backend is at this step; sending a message to continue algo there.
                let global = self.global();
                let operation = SyncOperation::NotifyEndOfVersionChange {
                    id: self.id,
                    name: self.name.clone(),
                    request,
                    origin: global.origin().immutable().clone(),
                };
                if self
                    .get_idb_thread()
                    .send(IndexedDBThreadMsg::Sync(operation))
                    .is_err()
                {
                    error!("Failed to send SyncOperation::NotifyEndOfVersionChange.");
                }
            },
            ConnectionEvent::ForcedClose => self.close_by_user_agent(cx),
        }
    }

    /// <https://w3c.github.io/IndexedDB/#close-a-database-connection> with the forced flag, run
    /// when the backend can't use the database any more.
    fn close_by_user_agent(&self, cx: &mut JSContext) {
        // Step 1: Set connection’s close pending flag to true.
        self.close_pending.set(true);

        // Step 2: If the forced flag is true, then for each transaction created using connection
        // run abort a transaction with transaction and newly created "AbortError" DOMException.
        let transactions: Vec<DomRoot<IDBTransaction>> = self
            .transactions
            .borrow()
            .iter()
            .map(|transaction| transaction.as_rooted())
            .collect();
        for transaction in transactions {
            transaction.abort(
                cx,
                Error::Abort(Some("The database connection was closed".to_owned())),
            );
        }

        // Step 3: Wait for all transactions created using connection to complete. Once they are
        // complete, connection is closed.
        // Step 4: If the forced flag is true, then fire an event named close at connection.
        // Note: the tasks that finish the aborted transactions were queued before this one.
        let this = Trusted::new(self);
        self.global()
            .task_manager()
            .database_access_task_source()
            .queue(task!(fire_indexeddb_close: move |cx| {
                this.root().upcast::<EventTarget>().fire_event(cx, atom!("close"));
            }));
    }

    /// The names of the connection's object stores, sorted.
    pub(crate) fn object_store_names(&self, cx: &mut JSContext) -> DomRoot<DOMStringList> {
        let names: Vec<Utf16String> = self
            .object_stores
            .borrow()
            .iter()
            .map(|store| store.name.clone())
            .collect();
        sorted_name_list(cx, &self.global(), names)
    }

    /// The object store named `name` in the connection's object store set.
    pub(crate) fn object_store_named(&self, name: &Utf16String) -> Option<IndexedDBObjectStore> {
        self.object_stores
            .borrow()
            .iter()
            .find(|store| store.name == *name)
            .cloned()
    }

    /// The object store `id` in the connection's object store set.
    pub(crate) fn object_store(&self, id: IndexedDBObjectStoreId) -> Option<IndexedDBObjectStore> {
        self.object_stores
            .borrow()
            .iter()
            .find(|store| store.id == id)
            .cloned()
    }

    pub(crate) fn object_store_exists(&self, name: &Utf16String) -> bool {
        self.object_stores
            .borrow()
            .iter()
            .any(|store| store.name == *name)
    }

    /// Whether the object store `id` is in the connection's object store set, that is, hasn't
    /// been deleted.
    pub(crate) fn has_object_store(&self, id: IndexedDBObjectStoreId) -> bool {
        self.object_stores
            .borrow()
            .iter()
            .any(|store| store.id == id)
    }

    /// Whether the index `index` of the object store `store` exists.
    pub(crate) fn has_index(&self, store: IndexedDBObjectStoreId, index: IndexedDBIndexId) -> bool {
        self.object_stores.borrow().iter().any(|object_store| {
            object_store.id == store && object_store.indexes.iter().any(|i| i.id == index)
        })
    }

    /// The indexes of the object store `store`, in the order they were created.
    pub(crate) fn indexes(&self, store: IndexedDBObjectStoreId) -> Vec<IndexedDBIndex> {
        self.object_stores
            .borrow()
            .iter()
            .find(|object_store| object_store.id == store)
            .map(|object_store| object_store.indexes.clone())
            .unwrap_or_default()
    }

    pub(crate) fn object_stores_snapshot(&self) -> Vec<IndexedDBObjectStore> {
        self.object_stores.borrow().clone()
    }

    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>
    /// Step 4. Set connection’s object store set to the set of object stores in database if
    /// database previously existed, or the empty set if database was newly created.
    /// NOTE: This reverts the value of objectStoreNames returned by the IDBDatabase object.
    pub(crate) fn restore_object_stores(&self, object_stores: Vec<IndexedDBObjectStore>) {
        *self.object_stores.borrow_mut() = object_stores;
    }

    /// Changes the metadata of the object store `id` with `change`, if it exists.
    fn change_object_store(
        &self,
        id: IndexedDBObjectStoreId,
        change: impl FnOnce(&mut IndexedDBObjectStore),
    ) {
        if let Some(store) = self
            .object_stores
            .borrow_mut()
            .iter_mut()
            .find(|store| store.id == id)
        {
            change(store);
        }
    }

    pub(crate) fn rename_object_store(&self, id: IndexedDBObjectStoreId, name: Utf16String) {
        self.change_object_store(id, |store| store.name = name);
    }

    pub(crate) fn add_index(&self, store: IndexedDBObjectStoreId, index: IndexedDBIndex) {
        self.change_object_store(store, |store| store.indexes.push(index));
    }

    pub(crate) fn remove_index(&self, store: IndexedDBObjectStoreId, index: IndexedDBIndexId) {
        self.change_object_store(store, |store| store.indexes.retain(|i| i.id != index));
    }

    pub(crate) fn rename_index(
        &self,
        store: IndexedDBObjectStoreId,
        index: IndexedDBIndexId,
        name: Utf16String,
    ) {
        self.change_object_store(store, |store| {
            if let Some(index) = store.indexes.iter_mut().find(|i| i.id == index) {
                index.name = name;
            }
        });
    }

    /// The id of a new index of the upgrade transaction.
    pub(crate) fn allocate_index_id(&self) -> IndexedDBIndexId {
        let id = self.next_index_id.get();
        self.next_index_id.set(id + 1);
        id
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-version>
    pub(crate) fn version(&self) -> u64 {
        // The version getter steps are to return this’s version.
        self.version.get()
    }

    pub(crate) fn set_version(&self, version: u64) {
        self.version.set(version);
    }

    pub fn set_transaction(&self, transaction: &IDBTransaction) {
        self.upgrade_transaction.set(Some(transaction));
    }

    pub(crate) fn clear_upgrade_transaction(&self, transaction: &IDBTransaction) {
        let current = self
            .upgrade_transaction
            .get()
            .expect("clear_upgrade_transaction called but no upgrade transaction is set");

        debug_assert!(
            &*current == transaction,
            "clear_upgrade_transaction called with non-current transaction"
        );

        self.upgrade_transaction.set(None);
    }

    /// <https://w3c.github.io/IndexedDB/#eventdef-idbdatabase-versionchange>
    pub fn dispatch_versionchange(
        &self,
        cx: &mut JSContext,
        old_version: u64,
        new_version: Option<u64>,
    ) {
        let global = self.global();
        let _ = IDBVersionChangeEvent::fire_version_change_event(
            cx,
            &global,
            self.upcast(),
            Atom::from("versionchange"),
            old_version,
            new_version,
        );
    }

    /// <https://w3c.github.io/IndexedDB/#close-a-database-connection>
    /// <https://w3c.github.io/IndexedDB/#connection-close-pending-flag>
    pub(crate) fn is_close_pending(&self) -> bool {
        self.close_pending.get()
    }

    pub(crate) fn close_a_database_connection(&self, _forced: bool) {
        // Step 1: Set connection’s close pending flag to true.
        self.close_pending.set(true);

        // Note: rest of the steps run in the storage backend.
        // TODO: `_forced` either needs to be used here or passed to the backend.
        let operation = SyncOperation::CloseDatabase(
            self.global().origin().immutable().clone(),
            self.id,
            self.name.clone(),
        );
        let _ = self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::Sync(operation));
    }
}

impl IDBDatabaseMethods<crate::DomTypeHolder> for IDBDatabase {
    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-transaction>
    fn Transaction(
        &self,
        cx: &mut JSContext,
        store_names: StringOrStringSequence,
        mode: IDBTransactionMode,
        options: &IDBTransactionOptions,
    ) -> Fallible<DomRoot<IDBTransaction>> {
        // Step 1. If a live upgrade transaction is associated with the connection,
        // throw an "InvalidStateError" DOMException.
        if self.upgrade_transaction.get().is_some() {
            return Err(Error::InvalidState(None));
        }

        // Step 2. If this’s close pending flag is true, then throw an
        // "InvalidStateError" DOMException.
        if self.close_pending.get() {
            return Err(Error::InvalidState(None));
        }

        // Step 3. Let scope be the set of unique strings in storeNames if it is
        // a sequence, or a set containing one string equal to storeNames otherwise.
        let mut scope: Vec<Utf16String> = match store_names {
            StringOrStringSequence::String(name) => vec![name_from_dom_string(&name)],
            StringOrStringSequence::StringSequence(sequence) => {
                sequence.iter().map(name_from_dom_string).collect()
            },
        };
        scope.sort_unstable();
        scope.dedup();

        // Step 4. If any string in scope is not the name of an object store in
        // the connected database, throw a "NotFoundError" DOMException.
        let Some(store_ids) = scope
            .iter()
            .map(|name| self.object_store_named(name).map(|store| store.id))
            .collect::<Option<Vec<_>>>()
        else {
            return Err(Error::NotFound(None));
        };

        // Step 5. If scope is empty, throw an "InvalidAccessError" DOMException.
        if scope.is_empty() {
            return Err(Error::InvalidAccess(None));
        }

        // Step 6. If mode is not "readonly" or "readwrite", throw a TypeError.
        if mode != IDBTransactionMode::Readonly && mode != IDBTransactionMode::Readwrite {
            return Err(Error::Type(c"Invalid transaction mode".to_owned()));
        }

        // Step 7. Let transaction be a newly created transaction with this
        // connection, mode, options’ durability member, and the set of object
        // stores named in scope.
        let durability = options.durability;
        let scope = sorted_name_list(cx, &self.global(), scope);
        let transaction = IDBTransaction::new(
            cx,
            &self.global(),
            self,
            mode,
            durability,
            &scope,
            store_ids,
        );

        // Step 8. Set transaction’s cleanup event loop to the current event loop.
        transaction.set_cleanup_event_loop();
        // https://w3c.github.io/IndexedDB/#cleanup-indexed-database-transactions
        // NOTE: These steps are invoked by [HTML]. They ensure that transactions created
        // by a script call to transaction() are deactivated once the task that invoked
        // the script has completed. The steps are run at most once for each transaction.
        // https://w3c.github.io/IndexedDB/#transaction-concept
        // A transaction optionally has a cleanup event loop which is an event loop.
        self.global()
            .ensure_indexeddb_factory(cx)
            .register_indexeddb_transaction(&transaction);

        // Step 9. Return an IDBTransaction object representing transaction.
        Ok(transaction)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-createobjectstore>
    fn CreateObjectStore(
        &self,
        cx: &mut JSContext,
        name: DOMString,
        options: &IDBObjectStoreParameters,
    ) -> Fallible<DomRoot<IDBObjectStore>> {
        // Step 1. Let database be this’s associated database.

        // Step 2. Let transaction be database’s upgrade transaction if it is not null,
        // or throw an "InvalidStateError" DOMException otherwise.
        let transaction = match self.upgrade_transaction.get() {
            Some(txn) => txn,
            None => return Err(Error::InvalidState(None)),
        };

        // Step 3. If transaction’s state is not active, then throw a
        // "TransactionInactiveError" DOMException.
        if !transaction.is_active() {
            return Err(Error::TransactionInactive(None));
        }

        // Step 4. Let keyPath be options’s keyPath member if it is not undefined
        // or null, or null otherwise.
        let key_path = options.keyPath.as_ref();

        // Step 5. If keyPath is not null and is not a valid key path, throw a
        // "SyntaxError" DOMException.
        if let Some(path) = key_path &&
            !is_valid_key_path(cx, path)?
        {
            return Err(Error::Syntax(None));
        }

        // Step 6. If an object store named name already exists in database throw
        // a "ConstraintError" DOMException.
        let name = name_from_dom_string(&name);
        if self.object_store_exists(&name) {
            return Err(Error::Constraint(None));
        }

        // Step 7. Let autoIncrement be options’s autoIncrement member.
        let auto_increment = options.autoIncrement;

        // Step 8. If autoIncrement is true and keyPath is an empty string or any
        // sequence (empty or otherwise), throw an "InvalidAccessError" DOMException.
        if auto_increment {
            match key_path {
                Some(StringOrStringSequence::String(path)) if path.is_empty() => {
                    return Err(Error::InvalidAccess(None));
                },
                Some(StringOrStringSequence::StringSequence(_)) => {
                    return Err(Error::InvalidAccess(None));
                },
                _ => {},
            }
        }

        // Step 9. Let store be a new object store in database. Set the created
        // object store’s name to name. If autoIncrement is true, then the
        // created object store uses a key generator. If keyPath is not null,
        // set the created object store’s key path to keyPath.
        let store_id = self.next_object_store_id.get();
        self.next_object_store_id.set(store_id + 1);
        let key_path = key_path.map(|p| match p {
            StringOrStringSequence::String(s) => KeyPath::String(s.to_string()),
            StringOrStringSequence::StringSequence(s) => {
                KeyPath::Sequence(s.iter().map(|s| s.to_string()).collect())
            },
        });
        let store = IndexedDBObjectStore {
            id: store_id,
            name,
            key_path: key_path.clone(),
            has_key_generator: auto_increment,
            indexes: Vec::new(),
        };
        let operation = AsyncSchemaOperation::CreateObjectStore {
            name: store.name.clone(),
            key_path,
            auto_increment,
        };
        transaction.note_write(None);
        if self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.name.clone(),
                store_id,
                operation,
                transaction: transaction.id(),
            })
            .is_err()
        {
            return Err(Error::Operation(Some(
                "Failed to send the object store to the backend".to_owned(),
            )));
        }
        self.object_stores
            .safe_borrow_mut(cx.no_gc())
            .push(store.clone());
        transaction.note_new_empty_store(store_id);

        let object_store = IDBObjectStore::new(cx, &self.global(), &store, &transaction);
        transaction.register_object_store_handle(&object_store);

        // Step 10. Return a new object store handle associated with store and transaction.
        Ok(object_store)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-deleteobjectstore>
    fn DeleteObjectStore(&self, name: DOMString) -> Fallible<()> {
        // Steps 1 & 2
        let transaction = self.upgrade_transaction.get();
        let transaction = match transaction {
            Some(transaction) => transaction,
            None => return Err(Error::InvalidState(None)),
        };

        // Step 3
        if !transaction.is_active() {
            return Err(Error::TransactionInactive(None));
        }

        // Step 4. Let store be the object store named name in database, or throw a
        // "NotFoundError" DOMException if none.
        let Some(store) = self.object_store_named(&name_from_dom_string(&name)) else {
            return Err(Error::NotFound(None));
        };

        // Step 5. Remove store from this’s object store set.
        self.object_stores
            .borrow_mut()
            .retain(|object_store| object_store.id != store.id);

        // Step 6. If there is an object store handle associated with store and transaction,
        // remove all entries from its index set.
        if let Some(handle) = transaction.object_store_handle(store.id) {
            handle.clear_index_set();
        }

        // Step 7. Destroy store.
        let operation = AsyncSchemaOperation::DeleteObjectStore;
        transaction.note_write(None);
        if self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::AsyncSchemaOperation {
                origin: self.global().origin().immutable().clone(),
                database_name: self.name.clone(),
                store_id: store.id,
                operation,
                transaction: transaction.id(),
            })
            .is_err()
        {
            warn!("Could not send AsyncSchemaOperation");
        }

        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-name>
    fn Name(&self) -> DOMString {
        dom_string_from_name(&self.name)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-version>
    fn Version(&self) -> u64 {
        self.version()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-objectstorenames>
    fn ObjectStoreNames(&self, cx: &mut JSContext) -> DomRoot<DOMStringList> {
        self.object_store_names(cx)
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-close>
    fn Close(&self) {
        // Step 1. Run close a database connection with this connection.
        self.close_a_database_connection(false);
    }

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-onabort
    event_handler!(abort, GetOnabort, SetOnabort);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-onclose
    event_handler!(close, GetOnclose, SetOnclose);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-onerror
    event_handler!(error, GetOnerror, SetOnerror);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-onversionchange
    event_handler!(versionchange, GetOnversionchange, SetOnversionchange);
}
