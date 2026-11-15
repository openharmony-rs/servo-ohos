/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use dom_struct::dom_struct;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::jsval::UndefinedValue;
use js::rust::HandleValue;
use profile_traits::generic_callback::GenericCallback;
use script_bindings::reflector::reflect_dom_object;
use servo_base::generic_channel::GenericSend;
use servo_url::origin::ImmutableOrigin;
use storage_traits::indexeddb::{
    BackendResult, ConnectionEvent, ConnectionMsg, IndexedDBObjectStore, IndexedDBThreadMsg,
    IndexedDBTxnId, SyncOperation,
};
use stylo_atoms::Atom;
use uuid::Uuid;

use crate::dom::bindings::codegen::Bindings::IDBDatabaseBinding::IDBTransactionDurability;
use crate::dom::bindings::codegen::Bindings::IDBOpenDBRequestBinding::IDBOpenDBRequestMethods;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::IDBTransactionMode;
use crate::dom::bindings::error::Error;
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{DomRoot, MutNullableDom};
use crate::dom::event::{Event, EventBubbles, EventCancelable};
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbdatabase::IDBDatabase;
use crate::dom::indexeddb::idbfactory::IDBFactory;
use crate::dom::indexeddb::idbrequest::IDBRequest;
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::idbversionchangeevent::IDBVersionChangeEvent;
use crate::dom::indexeddb::key::map_backend_error_to_dom_error;
use crate::realms::enter_auto_realm;

#[derive(Clone)]
struct OpenRequestListener {
    open_request: Trusted<IDBOpenDBRequest>,
}

impl OpenRequestListener {
    /// The continuation of the parallel steps of
    /// <https://www.w3.org/TR/IndexedDB/#dom-idbfactory-deletedatabase>
    fn handle_delete_db(&self, cx: &mut JSContext, result: BackendResult<u64>) {
        // Step 4.1: Let result be the result of deleting a database, with storageKey, name, and request.
        // Note: done with the `result` argument.

        // Step 4.2: Set request’s processed flag to true.
        // The backend tracks this flag for connection queue processing.

        // Step 3: Queue a database task to run these steps:
        // Note: we are in the queued task.

        let open_request = self.open_request.root();
        let global = open_request.global();

        // Note: setting the done flag here as it is done in both branches below.
        open_request.idbrequest.set_ready_state_done();

        rooted!(&in(cx) let mut rval = UndefinedValue());

        let mut realm = enter_auto_realm(cx, &*open_request);
        let cx = &mut realm.current_realm();

        match result {
            Ok(version) => {
                // Step 4.3.2: Otherwise,
                // set request’s result to undefined,
                // set request’s done flag to true,
                // and fire a version change event named success at request with result and null.
                open_request.set_result(rval.handle());
                let _ = IDBVersionChangeEvent::fire_version_change_event(
                    cx,
                    &global,
                    open_request.upcast(),
                    Atom::from("success"),
                    version,
                    None,
                );
            },
            Err(err) => {
                // Step 4.3.1:
                // If result is an error,
                // set request’s error to result,
                // set request’s done flag to true,
                // and fire an event named error at request
                // with its bubbles and cancelable attributes initialized to true.
                let error = map_backend_error_to_dom_error(err);
                open_request.set_error(cx, Some(error));
                let event = Event::new(
                    cx,
                    &global,
                    Atom::from("error"),
                    EventBubbles::Bubbles,
                    EventCancelable::Cancelable,
                );
                event.fire(cx, open_request.upcast());
            },
        }
    }
}

#[dom_struct]
pub struct IDBOpenDBRequest {
    idbrequest: IDBRequest,
    pending_connection: MutNullableDom<IDBDatabase>,

    /// The id used both for the request and the related connection.
    #[no_trace]
    id: Uuid,
}

impl IDBOpenDBRequest {
    pub fn new_inherited() -> IDBOpenDBRequest {
        IDBOpenDBRequest {
            idbrequest: IDBRequest::new_inherited(),
            pending_connection: Default::default(),
            id: Uuid::new_v4(),
        }
    }

    pub fn new(cx: &mut JSContext, global: &GlobalScope) -> DomRoot<IDBOpenDBRequest> {
        reflect_dom_object(cx, Box::new(IDBOpenDBRequest::new_inherited()), global)
    }

    pub(crate) fn get_id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn get_or_init_connection(
        &self,
        cx: &mut JSContext,
        global: &GlobalScope,
        name: String,
        version: u64,
        object_stores: Vec<IndexedDBObjectStore>,
        upgraded: bool,
    ) -> DomRoot<IDBDatabase> {
        self.pending_connection.or_init(|| {
            debug_assert!(!upgraded, "A connection should exist for the upgraded db.");
            IDBDatabase::new(
                cx,
                global,
                name.into(),
                self.get_id(),
                version,
                object_stores,
            )
        })
    }

    /// <https://w3c.github.io/IndexedDB/#upgrade-a-database>
    /// Step 10: Queue a database task to run these steps:
    /// The below are the steps in the task.
    pub(crate) fn upgrade_db_version(
        &self,
        cx: &mut JSContext,
        connection: &IDBDatabase,
        old_version: u64,
        version: u64,
        transaction: IndexedDBTxnId,
    ) {
        let global = self.global();

        let scope = connection.object_store_names(cx);

        let transaction = IDBTransaction::new_with_id(
            cx,
            &global,
            connection,
            IDBTransactionMode::Versionchange,
            IDBTransactionDurability::Default,
            &scope,
            transaction,
        );
        transaction.set_versionchange_old_version(old_version);
        connection.set_transaction(&transaction);
        // This task runs Step 10.4 later, so keep the transaction inactive until then.
        transaction.set_active_flag(false);

        rooted!(&in(cx) let mut connection_val = UndefinedValue());
        connection.to_jsval(cx, connection_val.handle_mut());

        // Step 10.1: Set request’s result to connection.
        self.idbrequest.set_result(connection_val.handle());

        // Step 10.2: Set request’s transaction to transaction.
        self.idbrequest.set_transaction(&transaction);

        // Step 10.3: Set request’s done flag to true.
        self.idbrequest.set_ready_state_done();

        // Step 10.4: Set transaction’s state to active.
        transaction.set_active_flag(true);

        // Step 10.5: Let didThrow be the result of firing a version change event
        // named upgradeneeded at request with old version and version.
        let did_throw = IDBVersionChangeEvent::fire_version_change_event(
            cx,
            &global,
            self.upcast(),
            Atom::from("upgradeneeded"),
            old_version,
            Some(version),
        );

        // Step 10.6: If transaction’s state is active, then:
        if transaction.is_active() {
            // Step 10.6.1: Set transaction’s state to inactive.
            transaction.set_active_flag(false);

            // Step 10.6.2: If didThrow is true, run abort a transaction with
            // transaction and a newly created "AbortError" DOMException.
            if did_throw {
                transaction.abort(cx, Error::Abort(None));
            } else {
                // The upgrade transaction auto-commits once inactive and quiescent.
                transaction.maybe_commit(cx);
            }
        }
    }

    pub(crate) fn pending_connection(&self) -> Option<DomRoot<IDBDatabase>> {
        self.pending_connection.get()
    }

    /// Handles a message on the result channel of this request and the connection it opens.
    pub(crate) fn handle_connection_event(&self, cx: &mut JSContext, event: ConnectionEvent) {
        if let ConnectionEvent::Open(message) = event {
            return self.handle_open_message(cx, message);
        }
        let Some(connection) = self.pending_connection.get() else {
            return debug!("IndexedDB connection event before the connection exists");
        };
        connection.handle_connection_event(cx, event);
    }

    /// <https://w3c.github.io/IndexedDB/#open-a-database-connection>
    /// The steps that continue on the script-thread.
    /// This covers interacting with the current open request,
    /// as well as with other open connections preventing the request from making progress.
    fn handle_open_message(&self, cx: &mut JSContext, message: ConnectionMsg) {
        let global = self.global();
        match message {
            ConnectionMsg::Connection {
                name,
                id: _,
                version,
                upgraded,
                object_stores,
            } => {
                // https://w3c.github.io/IndexedDB/#upgrade-transaction-steps
                // Step 3. Set transaction’s scope to connection’s object store set.
                let connection = self.get_or_init_connection(
                    cx,
                    &global,
                    name,
                    version,
                    object_stores,
                    upgraded,
                );

                // Step 2.2: Otherwise,
                // set request’s result to result,
                // set request’s done flag,
                // and fire an event named success at request.
                self.dispatch_success(cx, &connection);
            },
            ConnectionMsg::Upgrade {
                name,
                id: _,
                version,
                old_version,
                transaction,
                object_stores,
            } => {
                let connection =
                    self.get_or_init_connection(cx, &global, name, version, object_stores, false);
                // https://w3c.github.io/IndexedDB/#upgrade-transaction-steps
                // Step 3. Set transaction’s scope to connection’s object store set.
                self.upgrade_db_version(cx, &connection, old_version, version, transaction);
            },
            ConnectionMsg::VersionError { .. } => {
                // Step 2.1 If result is an error, see dispatch_error().
                self.dispatch_error(cx, Error::Version(None));
            },
            ConnectionMsg::AbortError { .. } => {
                // Step 2.1 If result is an error, see dispatch_error().
                self.dispatch_error(cx, Error::Abort(None));
            },
            ConnectionMsg::DatabaseError { error, .. } => {
                // Step 2.1 If result is an error, see dispatch_error().
                self.dispatch_error(cx, map_backend_error_to_dom_error(error));
            },
            ConnectionMsg::Blocked {
                version,
                old_version,
                ..
            } => {
                // Step 10.4: fire a version change event named blocked at request with db’s version and version.
                self.dispatch_blocked(cx, old_version, Some(version));
            },
        }
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbfactory-open>
    /// The error dispatching part from within a task part.
    fn dispatch_error(&self, cx: &mut JSContext, dom_exception: Error) {
        let global = self.global();

        // https://w3c.github.io/IndexedDB/#open-a-database-connection
        // Step 10.7: If connection was closed, return a newly created "AbortError" DOMException
        // and abort these steps.
        // Step 10.8: If request’s error is set, run the steps to close a database connection
        // with connection, return a newly created "AbortError" DOMException and abort these
        // steps.
        // Note: an aborted upgrade transaction closed the connection already, see
        // `IDBTransaction::abort`.
        if let Some(connection) = self.pending_connection.get() &&
            !connection.is_close_pending()
        {
            connection.close_a_database_connection(false);
        }

        // Step 5.3.1: If result is an error, then:
        // Step 5.3.1.1: Set request’s result to undefined.
        self.set_result(HandleValue::undefined());

        // Step 5.3.1.2: Set request’s error to result.
        self.set_error(cx, Some(dom_exception));
        // Open requests expose a transaction only while `upgradeneeded` is being dispatched;
        // otherwise `IDBOpenDBRequest.transaction` must be null.
        // https://w3c.github.io/IndexedDB/#dom-idbrequest-transaction
        // https://w3c.github.io/IndexedDB/#open-a-database-connection
        // Open requests that have completed with an error must not retain an upgrade transaction.
        self.clear_transaction();

        // Step 5.3.1.3: Set request’s done flag to true.
        self.set_ready_state_done();

        // Step 5.3.1.4: Fire an event named error at request
        // with its bubbles
        // and cancelable attributes initialized to true.
        let event = Event::new(
            cx,
            &global,
            Atom::from("error"),
            EventBubbles::Bubbles,
            EventCancelable::Cancelable,
        );
        event.fire(cx, self.upcast());
    }

    pub(crate) fn delete_database(
        &self,
        storage_key: ImmutableOrigin,
        name: String,
    ) -> Result<(), ()> {
        let global = self.global();

        let task_source = global
            .task_manager()
            .database_access_task_source()
            .to_sendable();
        let response_listener = OpenRequestListener {
            open_request: Trusted::new(self),
        };
        let callback = GenericCallback::new(move |message| {
            let response_listener = response_listener.clone();
            task_source.queue(task!(request_callback: move |cx| {
                response_listener.handle_delete_db(cx, message.unwrap());
            }))
        })
        .expect("Could not create delete database callback");

        let delete_operation = SyncOperation::DeleteDatabase(
            callback,
            storage_key,
            name,
            IDBFactory::storage_bottle_source(&global),
            self.get_id(),
        );

        if global
            .storage_threads()
            .send(IndexedDBThreadMsg::Sync(delete_operation))
            .is_err()
        {
            return Err(());
        }
        Ok(())
    }

    pub fn set_result(&self, result: HandleValue) {
        self.idbrequest.set_result(result);
    }

    pub fn set_ready_state_done(&self) {
        self.idbrequest.set_ready_state_done();
    }

    pub fn set_error(&self, cx: &mut JSContext, error: Option<Error>) {
        self.idbrequest.set_error(cx, error);
    }

    pub fn clear_transaction(&self) {
        self.idbrequest.clear_transaction();
    }

    pub(crate) fn clear_transaction_if_matches(&self, transaction: &IDBTransaction) -> bool {
        let matches = self
            .idbrequest
            .transaction()
            .is_some_and(|current| &*current == transaction);
        if matches {
            self.idbrequest.clear_transaction();
        }
        matches
    }

    pub fn dispatch_success(&self, cx: &mut JSContext, result: &IDBDatabase) {
        let global = self.global();
        self.idbrequest.set_ready_state_done();

        let mut realm = enter_auto_realm(cx, result);
        let cx = &mut realm.current_realm();
        rooted!(&in(cx) let mut result_val = UndefinedValue());
        result.to_jsval(cx, result_val.handle_mut());
        self.set_result(result_val.handle());

        let event = Event::new(
            cx,
            &global,
            Atom::from("success"),
            EventBubbles::DoesNotBubble,
            EventCancelable::NotCancelable,
        );
        event.fire(cx, self.upcast());
    }

    /// <https://w3c.github.io/IndexedDB/#eventdef-idbopendbrequest-blocked>
    pub fn dispatch_blocked(&self, cx: &mut JSContext, old_version: u64, new_version: Option<u64>) {
        let global = self.global();
        let _ = IDBVersionChangeEvent::fire_version_change_event(
            cx,
            &global,
            self.upcast(),
            Atom::from("blocked"),
            old_version,
            new_version,
        );
    }
}

impl IDBOpenDBRequestMethods<crate::DomTypeHolder> for IDBOpenDBRequest {
    // https://www.w3.org/TR/IndexedDB-3/#dom-idbopendbrequest-onblocked
    event_handler!(blocked, GetOnblocked, SetOnblocked);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbopendbrequest-onupgradeneeded
    event_handler!(upgradeneeded, GetOnupgradeneeded, SetOnupgradeneeded);
}
