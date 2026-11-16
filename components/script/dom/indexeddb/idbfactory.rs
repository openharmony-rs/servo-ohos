/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::collections::HashSet;

use dom_struct::dom_struct;
use js::context::JSContext;
use js::jsval::UndefinedValue;
use js::rust::HandleValue;
use profile_traits::generic_callback::GenericCallback;
use script_bindings::cell::DomRefCell;
use script_bindings::reflector::{Reflector, reflect_dom_object};
use servo_base::generic_channel::GenericSend;
use servo_url::origin::ImmutableOrigin;
use storage_traits::indexeddb::{
    BackendResult, ConnectionEvent, DatabaseInfo, IndexedDBThreadMsg, StorageBottleSource,
    SyncOperation, Utf16String,
};
use uuid::Uuid;

use crate::dom::RootedPromise;
use crate::dom::bindings::codegen::Bindings::IDBFactoryBinding::{
    IDBDatabaseInfo, IDBFactoryMethods,
};
use crate::dom::bindings::codegen::Bindings::WindowBinding::WindowMethods;
use crate::dom::bindings::error::{Error, ErrorToJsval, Fallible};
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::refcounted::{Trusted, TrustedPromise};
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot};
use crate::dom::bindings::str::DOMString;
use crate::dom::bindings::trace::HashMapTracedValues;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbopendbrequest::IDBOpenDBRequest;
use crate::dom::indexeddb::key::{
    convert_value_to_key, dom_string_from_name, map_backend_error_to_dom_error,
    name_from_dom_string,
};
use crate::dom::promise::Promise;
use crate::dom::types::IDBTransaction;
use crate::dom::window::Window;

/// A non-jstraceable string wrapper for use in `HashMapTracedValues`.
#[derive(Clone, Debug, Eq, Hash, MallocSizeOf, PartialEq)]
pub(crate) struct DBName(pub(crate) Utf16String);

#[dom_struct]
pub struct IDBFactory {
    reflector_: Reflector,
    /// <https://www.w3.org/TR/IndexedDB-3/#connection>
    /// The connections opened through this factory.
    /// We store the open request, which contains the connection.
    /// TODO: remove when we are sure they are not needed anymore.
    connections:
        DomRefCell<HashMapTracedValues<DBName, HashMapTracedValues<Uuid, Dom<IDBOpenDBRequest>>>>,

    /// <https://www.w3.org/TR/IndexedDB-3/#transaction>
    /// Active transactions associated with this factory's global.
    indexeddb_transactions: DomRefCell<HashMapTracedValues<DBName, Vec<Dom<IDBTransaction>>>>,
}

impl IDBFactory {
    pub fn new_inherited() -> IDBFactory {
        IDBFactory {
            reflector_: Reflector::new(),
            connections: Default::default(),
            indexeddb_transactions: Default::default(),
        }
    }

    pub(crate) fn register_indexeddb_transaction(&self, txn: &IDBTransaction) {
        let db_name = DBName(txn.get_db_name());
        let mut map = self.indexeddb_transactions.borrow_mut();
        let bucket = map.entry(db_name).or_default();
        if !bucket.iter().any(|entry| &**entry == txn) {
            bucket.push(Dom::from_ref(txn));
        }
        txn.set_registered_in_global();
    }

    pub(crate) fn unregister_indexeddb_transaction(&self, txn: &IDBTransaction) {
        let db_name = DBName(txn.get_db_name());
        let mut map = self.indexeddb_transactions.borrow_mut();
        if let Some(bucket) = map.get_mut(&db_name) {
            bucket.retain(|entry| &**entry != txn);
            if bucket.is_empty() {
                map.remove(&db_name);
            }
        }
        txn.clear_registered_in_global();
    }

    pub(crate) fn cleanup_indexeddb_transactions(&self, cx: &mut JSContext) -> bool {
        // We implement the HTML-triggered deactivation effect by tracking script-created
        // transactions on the global and deactivating them at the microtask checkpoint.
        let snapshot: Vec<DomRoot<IDBTransaction>> = {
            let mut map = self.indexeddb_transactions.safe_borrow_mut(cx.no_gc());

            // Transactions are normally unregistered when they finish (commit/abort),
            // but unregister can occur in a queued task (e.g. finalize_abort), so we can
            // briefly observe finished transactions here. Prune them defensively.
            let keys: Vec<DBName> = map.iter().map(|(k, _)| k.clone()).collect();
            for key in keys {
                if let Some(bucket) = map.get_mut(&key) {
                    bucket.retain(|txn| !txn.is_finished());
                    if bucket.is_empty() {
                        map.remove(&key);
                    }
                }
            }

            map.iter()
                .flat_map(|(_db, bucket)| bucket.iter())
                .map(|txn| DomRoot::from_ref(&**txn))
                .collect()
        };
        // https://html.spec.whatwg.org/multipage/#perform-a-microtask-checkpoint
        // https://w3c.github.io/IndexedDB/#cleanup-indexed-database-transactions
        // To cleanup Indexed Database transactions, run the following steps.
        // They will return true if any transactions were cleaned up, or false otherwise.
        // Step 1: If there are no transactions with cleanup event loop matching the current event loop, return false.
        // Step 2: For each transaction transaction with cleanup event loop matching the current event loop:
        // Step 2.1: Set transaction’s state to inactive.
        // Step 2.2: Clear transaction’s cleanup event loop.
        // Step 3: Return true.
        let any_matching = snapshot
            .iter()
            .any(|txn| txn.cleanup_event_loop_matches_current());

        if !any_matching {
            return false;
        }

        for txn in snapshot {
            if txn.cleanup_event_loop_matches_current() {
                txn.set_active_flag(false);
                txn.clear_cleanup_event_loop();
                if txn.is_usable() {
                    txn.maybe_commit(cx);
                }
            }
        }

        // Prune finished transactions again after maybe_commit() progress.
        let mut map = self.indexeddb_transactions.safe_borrow_mut(cx.no_gc());
        let keys: Vec<DBName> = map.iter().map(|(k, _)| k.clone()).collect();
        for key in keys {
            if let Some(bucket) = map.get_mut(&key) {
                bucket.retain(|txn| !txn.is_finished());
                if bucket.is_empty() {
                    map.remove(&key);
                }
            }
        }

        true
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbrequest-transaction>
    /// Clear IDBOpenDBRequest.transaction once the upgrade transaction is finished.
    pub(crate) fn clear_open_request_transaction_for_txn(&self, transaction: &IDBTransaction) {
        let requests: Vec<DomRoot<IDBOpenDBRequest>> = {
            let pending = self.connections.borrow();
            pending
                .iter()
                .flat_map(|(_db_name, entry)| entry.iter())
                .map(|(_id, request)| request.as_rooted())
                .collect()
        };
        let mut cleared = 0usize;
        for request in requests {
            cleared += request.clear_transaction_if_matches(transaction) as usize;
        }

        debug_assert_eq!(
            cleared, 1,
            "A versionchange transaction should belong to exactly one IDBOpenDBRequest."
        );
    }

    pub fn new(cx: &mut JSContext, global: &GlobalScope) -> DomRoot<IDBFactory> {
        reflect_dom_object(cx, Box::new(IDBFactory::new_inherited()), global)
    }

    /// <https://w3c.github.io/IndexedDB/#open-a-database-connection>
    fn open_database(
        &self,
        storage_key: ImmutableOrigin,
        name: Utf16String,
        version: Option<u64>,
        request: &IDBOpenDBRequest,
    ) -> Result<(), ()> {
        let global = self.global();
        let request_id = request.get_id();

        {
            let mut pending = self.connections.borrow_mut();
            let outer = pending.entry(DBName(name.clone())).or_default();
            outer.insert(request_id, Dom::from_ref(request));
        }

        let results = Self::connection_result_channel(&global, request);

        // Step 5: Run these steps in parallel:
        // Step 5.1: Let result be the result of opening a database connection,
        // with storageKey, name, version if given and undefined otherwise, and request.
        let open_operation = SyncOperation::OpenDatabase(
            storage_key,
            name,
            version,
            request.get_id(),
            Self::storage_bottle_source(&global),
            results,
        );

        // Note: algo continues in parallel.
        if global
            .storage_threads()
            .send(IndexedDBThreadMsg::Sync(open_operation))
            .is_err()
        {
            return Err(());
        }
        Ok(())
    }

    /// The result channel of `request` and of the connection it opens. The backend keeps it
    /// while the request is pending, and while the connection or one of its transactions is
    /// alive. Script keeps no sender, so the channel and its route go away once the request
    /// failed, or the connection is closed and its transactions have finished.
    fn connection_result_channel(
        global: &GlobalScope,
        request: &IDBOpenDBRequest,
    ) -> GenericCallback<ConnectionEvent> {
        let request = Trusted::new(request);
        let task_source = global
            .task_manager()
            .database_access_task_source()
            .to_sendable();
        let in_window = global.is::<Window>();
        GenericCallback::new(move |message| {
            let event = match message {
                Ok(event) => event,
                Err(err) => return error!("Error in IndexedDB connection channel {:?}.", err),
            };
            let request = request.clone();
            // A versionchange event at a connection of a document in the back/forward cache would
            // wait there until the document is shown again, and the upgrade or delete with it.
            // Like Gecko and Chromium, Servo evicts the document instead, which closes the
            // connection; the IndexedDB specification doesn't say. A forced close is the same.
            if in_window &&
                matches!(
                    event,
                    ConnectionEvent::VersionChange { .. } | ConnectionEvent::ForcedClose
                )
            {
                let event_task_source = task_source.clone();
                task_source.queue_even_if_not_fully_active(
                    task!(handle_indexeddb_versionchange: move |_cx| {
                        if let Some(window) = request.root().global().downcast::<Window>() &&
                            !window.Document().is_fully_active()
                        {
                            window.Document().evict_from_back_forward_cache();
                        }
                        // The event itself is a normal task, behind the tasks held back so far,
                        // such as the open request's success that created the connection. It is
                        // dropped with an evicted document, and fired if the document was shown
                        // again before the eviction.
                        event_task_source.queue(task!(handle_indexeddb_connection_event: move |cx| {
                            request.root().handle_connection_event(cx, event);
                        }));
                    }),
                );
                return;
            }
            // https://w3c.github.io/IndexedDB/#dom-idbfactory-open
            // Step 5.3: Queue a database task to run these steps:
            task_source.queue(task!(handle_indexeddb_connection_event: move |cx| {
                request.root().handle_connection_event(cx, event);
            }));
        })
        .expect("Could not create the connection's result channel")
    }

    pub(crate) fn abort_pending_upgrades_and_close_databases(&self) {
        // https://w3c.github.io/IndexedDB/#closing-connection
        // Step 2. If the forced flag is true, then for each transaction created using connection
        // run abort a transaction with transaction and newly created "AbortError" DOMException.
        let transactions: Vec<DomRoot<IDBTransaction>> = self
            .indexeddb_transactions
            .borrow()
            .iter()
            .flat_map(|(_, bucket)| bucket.iter())
            .map(|transaction| DomRoot::from_ref(&**transaction))
            .collect();
        for transaction in transactions {
            transaction.abort_when_global_goes_away();
        }

        let global = self.global();
        let connections = self.connections.borrow();
        let pending_upgrades = connections
            .iter()
            .map(|(key, val)| {
                let ids: HashSet<Uuid> = val.iter().map(|(k, _v)| *k).collect();
                (key.0.clone(), ids)
            })
            .collect();
        let origin = global.origin().immutable().clone();
        if global
            .storage_threads()
            .send(IndexedDBThreadMsg::Sync(
                SyncOperation::AbortPendingUpgrades {
                    pending_upgrades,
                    origin,
                },
            ))
            .is_err()
        {
            error!("Failed to send SyncOperation::AbortPendingUpgrade");
        }

        for requests in connections.values() {
            for request in requests.values() {
                if let Some(database) = request.pending_connection() {
                    database.close_a_database_connection(true /* forced */);
                }
            }
        }
    }

    /// What the backend needs to run
    /// <https://storage.spec.whatwg.org/#obtain-a-local-storage-bottle-map> for `global`, which it
    /// does in parallel.
    pub(crate) fn storage_bottle_source(global: &GlobalScope) -> StorageBottleSource {
        StorageBottleSource {
            client_storage: global.storage_threads().client_storage_handle(),
            webview: global.webview_id(),
        }
    }
}

impl IDBFactoryMethods<crate::DomTypeHolder> for IDBFactory {
    /// <https://w3c.github.io/IndexedDB/#dom-idbfactory-open>
    fn Open(
        &self,
        cx: &mut JSContext,
        name: DOMString,
        version: Option<u64>,
    ) -> Fallible<DomRoot<IDBOpenDBRequest>> {
        // Step 1: If version is 0 (zero), throw a TypeError.
        if version == Some(0) {
            return Err(Error::Type(
                c"The version must be an integer >= 1".to_owned(),
            ));
        };

        // Step 2: Let environment be this’s relevant settings object.
        let global = self.global();

        // Step 3: Let storageKey be the result of running obtain a storage key given environment.
        // If failure is returned, then throw a "SecurityError" DOMException and abort these steps.
        let Some(storage_key) = global.obtain_storage_key() else {
            return Err(Error::Security(None));
        };

        // Note: switching to obtaining a storage bottle map,
        // as per https://github.com/w3c/IndexedDB/pull/334/
        // The backend obtains it in parallel, see `open_database`.

        // Step 4: Let request be a new open request.
        let request = IDBOpenDBRequest::new(cx, &self.global());

        // Step 5: Runs in parallel
        if self
            .open_database(storage_key, name_from_dom_string(&name), version, &request)
            .is_err()
        {
            return Err(Error::Operation(None));
        }

        // Step 6: Return a new IDBOpenDBRequest object for request.
        Ok(request)
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbfactory-deletedatabase>
    fn DeleteDatabase(
        &self,
        cx: &mut JSContext,
        name: DOMString,
    ) -> Fallible<DomRoot<IDBOpenDBRequest>> {
        // Step 1: Let environment be this’s relevant settings object.
        let global = self.global();

        // Step 2: Let storageKey be the result of running obtain a storage key given environment.
        // If failure is returned, then throw a "SecurityError" DOMException and abort these steps.
        let Some(storage_key) = global.obtain_storage_key() else {
            return Err(Error::Security(None));
        };

        // Note: switching to obtaining a storage bottle map,
        // as per https://github.com/w3c/IndexedDB/pull/334/
        // The backend obtains it in parallel.

        // Step 3: Let request be a new open request
        let request = IDBOpenDBRequest::new(cx, &self.global());

        // Step 4: Runs in parallel
        if request
            .delete_database(storage_key, name_from_dom_string(&name))
            .is_err()
        {
            return Err(Error::Operation(None));
        }

        // Step 5: Return request
        Ok(request)
    }

    /// <https://www.w3.org/TR/IndexedDB/#dom-idbfactory-databases>
    fn Databases(&self, cx: &mut JSContext) -> RootedPromise {
        // Step 1: Let environment be this’s relevant settings object.
        let global = self.global();

        // Step 2: Let storageKey be the result of running obtain a storage key given environment.
        // If failure is returned, then return a promise rejected with a "SecurityError" DOMException.
        let storage_key = match global.obtain_storage_key() {
            Some(storage_key) => storage_key,
            None => {
                let p = Promise::new(cx, &global);
                p.reject_error(cx, Error::Security(None));
                return p;
            },
        };

        // Step 3: Let p be a new promise.
        let p = Promise::new(cx, &global);

        // Note: the option is required to pass the promise to a task from within the generic callback,
        // see #41356
        let mut trusted_promise: Option<TrustedPromise> = Some(TrustedPromise::from(&p));

        // Step 4: Run these steps in parallel:
        // Note implementing by communicating with the backend.
        let task_source = global
            .task_manager()
            .database_access_task_source()
            .to_sendable();
        let callback = GenericCallback::new(move |message| {
            let result: BackendResult<Vec<DatabaseInfo>> = message.unwrap();
            let Some(trusted_promise) = trusted_promise.take() else {
                return error!("Callback for `DataBases` called twice.");
            };

            // Step 4.4: Queue a database task to resolve p with result.
            task_source.queue(task!(set_request_result_to_database: move |cx| {
                let promise = trusted_promise.root(cx);
                match result {
                    Err(err) => {
                        let error = map_backend_error_to_dom_error(err);
                        rooted!(&in(cx) let mut rval = UndefinedValue());
                        error
                            .to_jsval(cx, &promise.global(), rval.handle_mut());
                        promise.reject_native(cx, &rval.handle());
                    },
                    Ok(info_list) => {
                        let info_list: Vec<IDBDatabaseInfo> = info_list
                            .into_iter()
                            .map(|info| IDBDatabaseInfo {
                                name: Some(dom_string_from_name(&info.name)),
                                version: Some(info.version),
                        })
                        .collect();
                        promise.resolve_native(cx, &info_list);
                },
            }
            }));
        })
        .expect("Could not create databases callback");

        let get_operation = SyncOperation::GetDatabases(callback, storage_key);
        if global
            .storage_threads()
            .send(IndexedDBThreadMsg::Sync(get_operation))
            .is_err()
        {
            error!("Failed to send SyncOperation::GetDatabases");
        }

        // Step 5: Return p.
        p
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbfactory-cmp>
    fn Cmp(&self, cx: &mut JSContext, first: HandleValue, second: HandleValue) -> Fallible<i16> {
        let first_key = convert_value_to_key(cx, first, None)?.into_result()?;
        let second_key = convert_value_to_key(cx, second, None)?.into_result()?;
        let cmp = first_key.partial_cmp(&second_key);
        if let Some(cmp) = cmp {
            match cmp {
                std::cmp::Ordering::Less => Ok(-1),
                std::cmp::Ordering::Equal => Ok(0),
                std::cmp::Ordering::Greater => Ok(1),
            }
        } else {
            Ok(i16::MAX)
        }
    }
}
