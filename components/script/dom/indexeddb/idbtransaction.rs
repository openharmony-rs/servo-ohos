/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};

use dom_struct::dom_struct;
use js::context::JSContext;
use js::jsval::UndefinedValue;
use script_bindings::cell::DomRefCell;
use script_bindings::reflector::reflect_dom_object;
use servo_base::generic_channel::{GenericSend, GenericSender};
use servo_base::id::ScriptEventLoopId;
use storage_traits::indexeddb::{
    BackendResult, IndexPopulationEvent, IndexPopulationStep, IndexedDBIndex, IndexedDBIndexId,
    IndexedDBObjectStore, IndexedDBObjectStoreId, IndexedDBThreadMsg, IndexedDBTxnId,
    IndexedDBTxnMode, RequestOutput, SyncOperation, TransactionEvent, Utf16String,
};
use stylo_atoms::Atom;

use crate::dom::bindings::codegen::Bindings::IDBDatabaseBinding::IDBTransactionDurability;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::{
    IDBTransactionMethods, IDBTransactionMode,
};
use crate::dom::bindings::error::{Error, Fallible, create_dom_exception};
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{Dom, DomRoot, MutNullableDom};
use crate::dom::bindings::str::DOMString;
use crate::dom::domexception::DOMException;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::event::{Event, EventBubbles, EventCancelable};
use crate::dom::eventtarget::EventTarget;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbcursor::{IDBCursor, IterationParam};
use crate::dom::indexeddb::idbdatabase::IDBDatabase;
use crate::dom::indexeddb::idbobjectstore::IDBObjectStore;
use crate::dom::indexeddb::idbrequest::IDBRequest;
use crate::dom::indexeddb::key::{map_transaction_error_to_dom_error, name_from_dom_string};
use crate::realms::enter_auto_realm;

/// A request in a transaction's request list, and its result once the backend has reported it.
#[derive(JSTraceable, MallocSizeOf)]
#[cfg_attr(crown, crown::unrooted_must_root_lint::must_root)]
struct PendingRequest {
    /// The request, or `None` for the population of an index, which createIndex makes "an
    /// asynchronous request within the upgrade transaction" without an IDBRequest.
    request: Option<Dom<IDBRequest>>,
    /// The id of this execution of the request, which a cursor's request has once per
    /// iteration.
    id: u64,
    #[no_trace]
    #[ignore_malloc_size_of = "Trusted"]
    iteration_param: Option<IterationParam>,
    #[no_trace]
    result: Option<BackendResult<RequestOutput>>,
}

/// The population of an index that createIndex created on an object store that may have
/// records: script reads the store's records in batches, computes their keys in the index and
/// sends them to the backend, which holds the transaction's later requests back meanwhile.
#[derive(JSTraceable, MallocSizeOf)]
#[cfg_attr(crown, crown::unrooted_must_root_lint::must_root)]
struct IndexPopulation {
    /// The handle that createIndex was called on.
    store: Dom<IDBObjectStore>,
    #[no_trace]
    index: IndexedDBIndex,
    /// The id of the request list entry that the population completes.
    entry: u64,
}

#[dom_struct]
pub struct IDBTransaction {
    eventtarget: EventTarget,
    object_store_names: Dom<DOMStringList>,
    /// <https://w3c.github.io/IndexedDB/#transaction-scope>, by object store id, of a transaction
    /// that isn't an upgrade transaction.
    scope: Vec<IndexedDBObjectStoreId>,
    mode: IDBTransactionMode,
    durability: IDBTransactionDurability,
    db: Dom<IDBDatabase>,
    error: MutNullableDom<DOMException>,

    /// <https://w3c.github.io/IndexedDB/#object-store-handle>: the handles of this transaction,
    /// by object store, also of stores deleted since.
    store_handles: DomRefCell<HashMap<IndexedDBObjectStoreId, Dom<IDBObjectStore>>>,
    /// <https://www.w3.org/TR/IndexedDB-3/#transaction-request-list>: the requests whose result
    /// task hasn't run, in the order they were made. Results are delivered from the front only,
    /// so that they complete in request order.
    requests: DomRefCell<VecDeque<PendingRequest>>,
    /// The index populations of this upgrade transaction that haven't ended.
    index_populations: DomRefCell<Vec<IndexPopulation>>,
    /// The object stores that this upgrade transaction created and that no put or add has been
    /// made on since, which have no records to populate an index from.
    new_empty_stores: DomRefCell<Vec<IndexedDBObjectStoreId>>,
    // https://www.w3.org/TR/IndexedDB-3/#transaction-active-flag
    active: Cell<bool>,
    // https://www.w3.org/TR/IndexedDB-3/#transaction-finish
    finished: Cell<bool>,
    abort_initiated: Cell<bool>,
    committing: Cell<bool>,
    commit_started: Cell<bool>,
    version_change_old_version: Cell<Option<u64>>,
    // https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction
    // Step 4. NOTE: This reverts the value of objectStoreNames returned by the IDBDatabase object.
    #[no_trace]
    version_change_old_object_stores: DomRefCell<Option<Vec<IndexedDBObjectStore>>>,
    // https://w3c.github.io/IndexedDB/#transaction-concept
    // “A transaction optionally has a cleanup event loop which is an event loop.”
    #[no_trace]
    cleanup_event_loop: Cell<Option<ScriptEventLoopId>>,
    registered_in_global: Cell<bool>,
    next_request_id: Cell<u64>,
    /// The number of schema operations made in this transaction, and of the requests that write
    /// to each object store, which tell cursors whether the records they prefetched are still
    /// current.
    schema_writes: Cell<u64>,
    store_writes: DomRefCell<HashMap<IndexedDBObjectStoreId, u64>>,
    /// The cursors of this transaction that keep prefetched records, which it drops when it
    /// finishes.
    prefetching_cursors: DomRefCell<Vec<Dom<IDBCursor>>>,

    /// The id the backend knows this transaction by.
    #[no_trace]
    id: IndexedDBTxnId,
}

impl IDBTransaction {
    fn new_inherited(
        connection: &IDBDatabase,
        mode: IDBTransactionMode,
        durability: IDBTransactionDurability,
        scope_names: &DOMStringList,
        scope: Vec<IndexedDBObjectStoreId>,
        id: IndexedDBTxnId,
    ) -> IDBTransaction {
        IDBTransaction {
            eventtarget: EventTarget::new_inherited(),
            object_store_names: Dom::from_ref(scope_names),
            scope,
            mode,
            durability,
            db: Dom::from_ref(connection),
            error: Default::default(),

            store_handles: Default::default(),
            requests: Default::default(),
            index_populations: Default::default(),
            new_empty_stores: Default::default(),
            active: Cell::new(true),
            finished: Cell::new(false),
            abort_initiated: Cell::new(false),
            committing: Cell::new(false),
            commit_started: Cell::new(false),
            version_change_old_version: Cell::new(None),
            version_change_old_object_stores: DomRefCell::new(
                (mode == IDBTransactionMode::Versionchange)
                    .then(|| connection.object_stores_snapshot()),
            ),
            cleanup_event_loop: Cell::new(None),
            registered_in_global: Cell::new(false),
            next_request_id: Cell::new(0),
            schema_writes: Cell::new(0),
            store_writes: Default::default(),
            prefetching_cursors: Default::default(),
            id,
        }
    }

    /// Creates a readonly or readwrite transaction of `connection` with the object stores named
    /// `scope_names`, whose ids are `scope`, and tells the backend about it without waiting for a
    /// reply.
    pub fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        connection: &IDBDatabase,
        mode: IDBTransactionMode,
        durability: IDBTransactionDurability,
        scope_names: &DOMStringList,
        scope: Vec<IndexedDBObjectStoreId>,
    ) -> DomRoot<IDBTransaction> {
        let id = connection.allocate_transaction_id();
        let backend_mode = match mode {
            IDBTransactionMode::Readonly => IndexedDBTxnMode::Readonly,
            IDBTransactionMode::Readwrite => IndexedDBTxnMode::Readwrite,
            IDBTransactionMode::Versionchange => IndexedDBTxnMode::Versionchange,
        };
        // https://w3c.github.io/IndexedDB/#transaction-scheduling
        // The backend orders transactions by the arrival of this message, which is sent before
        // any other message about the transaction.
        if global
            .storage_threads()
            .send(IndexedDBThreadMsg::Sync(SyncOperation::CreateTransaction {
                origin: global.origin().immutable().clone(),
                db_name: connection.name().clone(),
                txn: id,
                mode: backend_mode,
                scope: scope.clone(),
            }))
            .is_err()
        {
            error!("Failed to send SyncOperation::CreateTransaction");
        }
        IDBTransaction::new_with_id(
            cx,
            global,
            connection,
            mode,
            durability,
            scope_names,
            scope,
            id,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new_with_id(
        cx: &mut JSContext,
        global: &GlobalScope,
        connection: &IDBDatabase,
        mode: IDBTransactionMode,
        durability: IDBTransactionDurability,
        scope_names: &DOMStringList,
        scope: Vec<IndexedDBObjectStoreId>,
        id: IndexedDBTxnId,
    ) -> DomRoot<IDBTransaction> {
        let transaction = reflect_dom_object(
            cx,
            Box::new(IDBTransaction::new_inherited(
                connection,
                mode,
                durability,
                scope_names,
                scope,
                id,
            )),
            global,
        );
        connection.add_transaction(&transaction);
        transaction
    }

    /// <https://w3c.github.io/IndexedDB/#transaction-lifecycle>
    pub fn set_active_flag(&self, status: bool) {
        // inactive
        // A transaction is in this state after control returns to the event loop after its creation,
        //  and when events are not being dispatched.
        // No requests can be made against the transaction when it is in this state.
        self.active.set(status);
    }

    pub fn is_active(&self) -> bool {
        self.active.get()
    }

    /// <https://w3c.github.io/IndexedDB/#transaction-lifetime>
    pub(crate) fn is_usable(&self) -> bool {
        // A transaction can be aborted at any time before it is finished,
        //  even if the transaction isn’t currently active or hasn’t yet started.
        // An explicit call to abort() will initiate an abort.
        // An abort will also be initiated following a failed request that is not handled by script.
        !self.finished.get() && !self.abort_initiated.get() && !self.committing.get()
    }

    pub(crate) fn is_inactive(&self) -> bool {
        !self.active.get() &&
            !self.finished.get() &&
            !self.abort_initiated.get() &&
            !self.committing.get()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished.get()
    }

    /// Whether the transaction's state is finished in the spec's sense, which "abort a transaction"
    /// sets in step 4, before the task that fires the abort event sets `finished`.
    pub(crate) fn state_is_finished(&self) -> bool {
        self.finished.get() || self.abort_initiated.get()
    }

    pub(crate) fn set_cleanup_event_loop(&self) {
        // https://w3c.github.io/IndexedDB/#transaction-concept
        // A transaction optionally has a cleanup event loop which is an event loop.
        self.cleanup_event_loop.set(ScriptEventLoopId::installed());
    }

    pub(crate) fn clear_cleanup_event_loop(&self) {
        // https://w3c.github.io/IndexedDB/#cleanup-indexed-database-transactions
        // Clear transaction’s cleanup event loop.
        self.cleanup_event_loop.set(None);
    }

    pub(crate) fn cleanup_event_loop_matches_current(&self) -> bool {
        match ScriptEventLoopId::installed() {
            Some(current) => self.cleanup_event_loop.get() == Some(current),
            None => false,
        }
    }

    pub(crate) fn set_registered_in_global(&self) {
        self.registered_in_global.set(true);
    }

    pub(crate) fn clear_registered_in_global(&self) {
        self.registered_in_global.set(false);
    }

    pub(crate) fn set_versionchange_old_version(&self, version: u64) {
        self.version_change_old_version.set(Some(version));
    }

    pub(crate) fn register_object_store_handle(&self, store: &IDBObjectStore) {
        self.store_handles
            .borrow_mut()
            .insert(store.id(), Dom::from_ref(store));
    }

    /// The handle of the object store `id` in this transaction, if there is one.
    pub(crate) fn object_store_handle(
        &self,
        id: IndexedDBObjectStoreId,
    ) -> Option<DomRoot<IDBObjectStore>> {
        self.store_handles
            .borrow()
            .get(&id)
            .map(|store| DomRoot::from_ref(&**store))
    }

    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>
    fn restore_associated_object_store_handles_after_abort(&self, cx: &mut JSContext) {
        // Step 5. For each object store handle handle associated with transaction,
        // including those for object stores that were created or deleted during
        // transaction:
        let stores = self
            .store_handles
            .borrow()
            .values()
            .map(|store| DomRoot::from_ref(&**store))
            .collect::<Vec<_>>();
        for store in stores {
            store.restore_metadata_after_abort(cx);
        }
    }

    /// Sends the commit to the backend, which commits after the transaction's requests and
    /// reports [`TransactionEvent::Committed`].
    fn attempt_commit(&self) -> bool {
        if self.commit_started.get() {
            return true;
        }
        let global = self.global();
        let commit_operation = SyncOperation::Commit {
            origin: global.origin().immutable().clone(),
            db_name: self.db.name().clone(),
            txn: self.id,
        };

        // https://w3c.github.io/IndexedDB/#transaction-lifecycle
        // When committing, the transaction state is set to committing.
        let send_result = self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::Sync(commit_operation));
        if send_result.is_err() {
            return false;
        }

        self.committing.set(true);
        self.commit_started.set(true);
        true
    }

    /// Commits the transaction if it is inactive or committing, and all its requests have
    /// delivered their results.
    ///
    /// <https://w3c.github.io/IndexedDB/#transaction-lifetime>
    /// Step 5: The implementation must attempt to commit an inactive transaction when all requests
    /// placed against the transaction have completed and their returned results handled, no new
    /// requests have been placed against the transaction, and the transaction has not been
    /// aborted.
    ///
    /// A transaction whose commit() was called commits once its request list is empty too: the
    /// backend then has run all its requests. Results that are errors abort it before that, see
    /// [`Self::deliver_next_result`].
    pub(crate) fn maybe_commit(&self, cx: &mut JSContext) {
        if !self.commit_if_ready() {
            // We failed to initiate the commit algorithm (backend task could not be queued),
            // so the transaction cannot progress to a successful "complete".
            // Choose the most appropriate DOMException mapping for Servo here.
            self.abort(cx, Error::InvalidState(None));
        }
    }

    /// Sends the commit if the transaction is ready for it, as [`Self::maybe_commit`]; returns
    /// false if sending it failed.
    fn commit_if_ready(&self) -> bool {
        if self.finished.get() || self.abort_initiated.get() || self.commit_started.get() {
            return true;
        }
        if self.active.get() || !self.requests.borrow().is_empty() {
            return true;
        }
        self.attempt_commit()
    }

    pub fn get_mode(&self) -> IDBTransactionMode {
        self.mode
    }

    pub fn get_db_name(&self) -> Utf16String {
        self.db.name().clone()
    }

    pub(crate) fn get_db(&self) -> &IDBDatabase {
        &self.db
    }

    pub(crate) fn id(&self) -> IndexedDBTxnId {
        self.id
    }

    /// request_id is only required to be unique within this transaction.
    pub(crate) fn allocate_request_id(&self) -> u64 {
        let id = self.next_request_id.get();
        self.next_request_id.set(id + 1);
        id
    }

    /// Notes that a request that writes to the object store `store`, or a schema operation if
    /// `store` is `None`, was made in this transaction. The requests after it see its changes, so
    /// the records that cursors over the store, or over any store, prefetched before are no
    /// longer current.
    pub(crate) fn note_write(&self, store: Option<IndexedDBObjectStoreId>) {
        match store {
            Some(store) => *self.store_writes.borrow_mut().entry(store).or_default() += 1,
            None => self.schema_writes.set(self.schema_writes.get() + 1),
        }
    }

    /// The number of schema operations, and of requests that write to the object store `store`,
    /// made in this transaction.
    pub(crate) fn writes(&self, store: IndexedDBObjectStoreId) -> u64 {
        self.schema_writes.get() + self.store_writes.borrow().get(&store).copied().unwrap_or(0)
    }

    /// Notes that `cursor` keeps prefetched records, which the transaction drops when it
    /// finishes.
    pub(crate) fn note_prefetching_cursor(&self, cursor: &IDBCursor) {
        let mut cursors = self.prefetching_cursors.borrow_mut();
        if !cursors.iter().any(|kept| &**kept == cursor) {
            cursors.push(Dom::from_ref(cursor));
        }
    }

    fn drop_prefetched_records(&self) {
        let cursors: Vec<DomRoot<IDBCursor>> = self
            .prefetching_cursors
            .borrow_mut()
            .drain(..)
            .map(|cursor| cursor.as_rooted())
            .collect();
        for cursor in cursors {
            cursor.drop_prefetched_records();
        }
    }

    /// <https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request>
    /// Step 4: Add request to the end of transaction’s request list.
    pub(crate) fn add_request(
        &self,
        request: &IDBRequest,
        id: u64,
        iteration_param: Option<IterationParam>,
    ) {
        self.requests.borrow_mut().push_back(PendingRequest {
            request: Some(Dom::from_ref(request)),
            id,
            iteration_param,
            result: None,
        });
    }

    /// Adds `request` to the end of the request list with `result`, which script knows without
    /// asking the backend, and delivers it in request order.
    pub(crate) fn add_request_with_result(
        &self,
        request: &IDBRequest,
        id: u64,
        iteration_param: IterationParam,
        result: BackendResult<RequestOutput>,
    ) {
        self.requests.borrow_mut().push_back(PendingRequest {
            request: Some(Dom::from_ref(request)),
            id,
            iteration_param: Some(iteration_param),
            result: Some(result),
        });
        self.queue_delivery();
    }

    /// Notes that this upgrade transaction created the object store `store`, which has no
    /// records.
    pub(crate) fn note_new_empty_store(&self, store: IndexedDBObjectStoreId) {
        self.new_empty_stores.borrow_mut().push(store);
    }

    /// Notes that a request that adds a record to the object store `store` was made.
    pub(crate) fn note_records_added(&self, store: IndexedDBObjectStoreId) {
        self.new_empty_stores.borrow_mut().retain(|id| *id != store);
    }

    /// Whether the object store `store` is one that this upgrade transaction created and that
    /// no record was added to since.
    pub(crate) fn is_new_empty_store(&self, store: IndexedDBObjectStoreId) -> bool {
        self.new_empty_stores.borrow().contains(&store)
    }

    /// Populates the index `index`, which createIndex created on the store of `store`, the handle
    /// it was called on: the backend reports the store's first records, see
    /// [`Self::index_population_event`]. The population is a request list entry without an
    /// IDBRequest, so that it ends in request order.
    pub(crate) fn start_index_population(&self, store: &IDBObjectStore, index: IndexedDBIndex) {
        let entry = self.allocate_request_id();
        self.requests.borrow_mut().push_back(PendingRequest {
            request: None,
            id: entry,
            iteration_param: None,
            result: None,
        });
        self.index_populations.borrow_mut().push(IndexPopulation {
            store: Dom::from_ref(store),
            index,
            entry,
        });
    }

    /// Takes the next step of populating the index `index_id` after the backend reported
    /// `event`: reads the next records and sends the index keys of those that arrived, or ends
    /// the population's request list entry.
    fn index_population_event(
        &self,
        cx: &mut JSContext,
        index_id: IndexedDBIndexId,
        event: IndexPopulationEvent,
    ) {
        if self.finished.get() || self.abort_initiated.get() {
            return;
        }
        let Some((store, index, entry)) = self
            .index_populations
            .borrow()
            .iter()
            .find(|population| population.index.id == index_id)
            .map(|population| {
                (
                    population.store.as_rooted(),
                    population.index.clone(),
                    population.entry,
                )
            })
        else {
            return;
        };
        let (records, last) = match event {
            IndexPopulationEvent::Records { records, last } => (records, last),
            IndexPopulationEvent::Done(result) => {
                return self.end_index_population(cx, index_id, entry, result);
            },
        };
        // An index or store deleted since is still populated: the requests made before the
        // deletion see the whole index, and the backend runs the deletion after them.

        // The next records are read while these are indexed.
        if !last && let Some(record) = records.last() {
            store.send_index_population_step(
                index_id,
                IndexPopulationStep::ReadRecords {
                    after: record.primary_key.clone(),
                },
            );
        }
        let mut realm = enter_auto_realm(cx, &*store);
        let cx: &mut JSContext = &mut realm;
        let keys = store.index_keys_of_records(cx, &index, records);
        // Page code that computing the keys reached may have aborted the transaction.
        if self.finished.get() || self.abort_initiated.get() {
            return;
        }
        match keys {
            Ok(records) => store.send_index_population_step(
                index_id,
                IndexPopulationStep::SetIndexKeys {
                    records,
                    done: last,
                },
            ),
            Err(error) => self.end_index_population(cx, index_id, entry, Err(error)),
        }
    }

    /// Ends the population of the index `index_id`, whose request list entry is `entry`, with
    /// `result`, and delivers the results that are next in request order.
    fn end_index_population(
        &self,
        cx: &mut JSContext,
        index_id: IndexedDBIndexId,
        entry: u64,
        result: BackendResult<()>,
    ) {
        self.index_populations
            .safe_borrow_mut(cx.no_gc())
            .retain(|population| population.index.id != index_id);
        self.request_result_arrived(cx, entry, result.map(|()| RequestOutput::Done));
    }

    /// Handles what the backend reported about this transaction on the connection's result
    /// channel.
    pub(crate) fn handle_event(&self, cx: &mut JSContext, event: TransactionEvent) {
        match event {
            TransactionEvent::Request { request_id, result } => {
                self.request_result_arrived(cx, request_id, result)
            },
            TransactionEvent::SchemaChangeFailed(error) => {
                self.abort(cx, map_transaction_error_to_dom_error(error))
            },
            TransactionEvent::IndexPopulation { index_id, event } => {
                self.index_population_event(cx, index_id, event)
            },
            TransactionEvent::Committed(result) => self.commit_result_arrived(cx, result),
        }
    }

    /// Stores the result of the request `request_id`, and delivers the results that are next in
    /// request order. A result for a request that is no longer in the request list, because the
    /// transaction was aborted, is discarded.
    fn request_result_arrived(
        &self,
        cx: &mut JSContext,
        request_id: u64,
        result: BackendResult<RequestOutput>,
    ) {
        {
            let mut requests = self.requests.safe_borrow_mut(cx.no_gc());
            let Some(pending) = requests.iter_mut().find(|pending| pending.id == request_id) else {
                return;
            };
            debug_assert!(
                pending.result.is_none(),
                "Request {request_id} completed twice"
            );
            pending.result = Some(result);
        }
        self.deliver_next_result(cx);
    }

    /// Runs the task of <https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request> for
    /// the first request in the request list, if its result has arrived, and queues the same for
    /// the next one if its result is already there.
    fn deliver_next_result(&self, cx: &mut JSContext) {
        if self.finished.get() || self.abort_initiated.get() {
            return;
        }
        // Step 5.1: Wait until request is the first item in transaction’s request list that is
        // not processed.
        let (request, id, iteration_param, result) = {
            let mut requests = self.requests.safe_borrow_mut(cx.no_gc());
            let Some(first) = requests.front_mut() else {
                return;
            };
            let Some(result) = first.result.take() else {
                return;
            };
            let first = (
                first.request.as_ref().map(|request| request.as_rooted()),
                first.id,
                first.iteration_param.take(),
                result,
            );
            // Step 5.6.1: Remove request from transaction’s request list.
            requests.pop_front();
            first
        };

        match request {
            Some(request) => {
                let mut realm = enter_auto_realm(cx, &*request);
                let cx: &mut JSContext = &mut realm;
                rooted!(&in(cx) let mut answer = UndefinedValue());
                match request.convert_result(
                    cx,
                    result,
                    iteration_param.as_ref(),
                    answer.handle_mut(),
                ) {
                    // Step 5.3: If result is an error and transaction’s state is committing, then
                    // run abort a transaction with transaction and result, and terminate these
                    // steps.
                    // The request stays in the request list, so the abort fires its error event.
                    Err(error) if self.committing.get() => {
                        self.requests.borrow_mut().push_front(PendingRequest {
                            request: Some(Dom::from_ref(&*request)),
                            id,
                            iteration_param: None,
                            result: None,
                        });
                        self.abort(cx, error);
                        return;
                    },
                    Err(error) => request.finish_with_error(cx, self, error),
                    Ok(()) => request.finish_with_success(cx, self, answer.handle()),
                }
            },
            None => {
                let mut realm = enter_auto_realm(cx, self);
                let cx: &mut JSContext = &mut realm;
                // https://w3c.github.io/IndexedDB/#dom-idbobjectstore-createindex
                // Once the implementation determines that creating the index has failed, it
                // must run the steps to abort a transaction using an appropriate error. For
                // example [...] if the index can't be created due to unique flag constraints, a
                // "ConstraintError" DOMException must be used as error.
                if let Err(error) = result {
                    self.abort(cx, map_transaction_error_to_dom_error(error));
                    return;
                }
                self.maybe_commit(cx);
            },
        }

        if self
            .requests
            .borrow()
            .front()
            .is_some_and(|pending| pending.result.is_some())
        {
            self.queue_delivery();
        }
    }

    /// Queues a task that delivers the result of the first request in the request list, if it
    /// is there by then.
    fn queue_delivery(&self) {
        let this = Trusted::new(self);
        self.global()
            .task_manager()
            .database_access_task_source()
            .queue(task!(deliver_indexeddb_result: move |cx| {
                this.root().deliver_next_result(cx);
            }));
    }

    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>, the steps after the backend
    /// wrote the changes or failed to.
    fn commit_result_arrived(&self, cx: &mut JSContext, result: BackendResult<()>) {
        if self.finished.get() || self.abort_initiated.get() {
            return;
        }
        debug_assert!(
            self.requests.borrow().is_empty(),
            "A transaction commits once all its results have been delivered"
        );
        match result {
            Ok(()) => self.finalize_commit(cx),
            Err(err) => {
                // Step 2.4. If an error occurs while writing the changes to the database, then
                // run abort a transaction with transaction and an appropriate type for the error,
                // for example a QuotaExceededError or an "UnknownError" DOMException, and
                // terminate these steps.
                self.abort(cx, map_transaction_error_to_dom_error(err));
            },
        }
    }

    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>
    pub(crate) fn abort(&self, cx: &mut JSContext, error: Error) {
        // Step 1: If transaction’s state is finished, abort these steps.
        if self.finished.get() || self.abort_initiated.get() {
            return;
        }

        // Step 2: All the changes made to the database by the transaction are reverted.
        // The backend rolls back after the requests it has been handed, and results that arrive
        // later are discarded, see `request_result_arrived`.
        self.send_abort_to_backend();

        // Step 3: If transaction is an upgrade transaction, run the steps to abort an upgrade
        // transaction with transaction.
        if self.mode == IDBTransactionMode::Versionchange {
            // https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction
            // Step 3. Set connection’s version to database’s version if database previously
            // existed, or 0 (zero) if database was newly created.
            // NOTE: This reverts the value of version returned by the IDBDatabase object.
            if let Some(old_version) = self.version_change_old_version.get() {
                self.db.set_version(old_version);
            }
            // Step 4. Set connection’s object store set to the set of object stores in database if database previously existed,
            // or the empty set if database was newly created.
            if let Some(object_stores) = self.version_change_old_object_stores.borrow().clone() {
                self.db.restore_object_stores(object_stores);
            }
            self.restore_associated_object_store_handles_after_abort(cx);

            // https://w3c.github.io/IndexedDB/#open-a-database-connection
            // Step 10.8: If request’s error is set, run the steps to close a database connection
            // with connection, return a newly created "AbortError" DOMException and abort these
            // steps.
            // Note: the open steps continue once the transaction is finished, which it is from
            // here on, and its open request fails with AbortError.
            self.db.close_a_database_connection(false);
        }

        // Step 4: Set transaction’s state to finished.
        self.abort_initiated.set(true);
        self.active.set(false);
        self.committing.set(false);
        self.commit_started.set(false);

        // Step 5: Set transaction’s error to error.
        // NOTE: Implementors need to keep in mind that the value "null" is considered an error, as it is set from abort()
        if let Ok(exception) = create_dom_exception(cx, &self.global(), error) {
            self.error.set(Some(&exception));
        }

        // Step 6: For each request of transaction’s request list, abort the steps to
        // asynchronously execute a request for request, set request’s processed flag to true,
        // and queue a database task to run these steps:
        let global = self.global();
        let task_manager = global.task_manager();
        let task_source = task_manager.database_access_task_source();
        self.index_populations.safe_borrow_mut(cx.no_gc()).clear();
        let requests: Vec<Trusted<IDBRequest>> = self
            .requests
            .safe_borrow_mut(cx.no_gc())
            .drain(..)
            .filter_map(|pending| pending.request.as_deref().map(Trusted::new))
            .collect();
        for request in requests {
            task_source.queue(task!(fire_indexeddb_request_abort_error: move |cx| {
                request.root().finish_with_abort_error(cx);
            }));
        }

        // Step 7: Queue a database task to run these steps:
        let this = Trusted::new(self);
        task_source.queue(task!(send_abort_notification: move |cx| {
            this.root().fire_abort(cx);
        }));
    }

    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>, the task of step 7.
    fn fire_abort(&self, cx: &mut JSContext) {
        // Step 7.1: If transaction is an upgrade transaction, then set transaction’s connection’s
        // associated database’s upgrade transaction to null.
        if self.mode == IDBTransactionMode::Versionchange {
            self.db.clear_upgrade_transaction(self);
        }
        // Step 7.2: Fire an event named abort at transaction with its bubbles attribute initialized
        // to true.
        let global = self.global();
        let event = Event::new(
            cx,
            &global,
            Atom::from("abort"),
            EventBubbles::Bubbles,
            EventCancelable::NotCancelable,
        );
        event.fire(cx, self.upcast());
        // Step 7.3: If transaction is an upgrade transaction, then:
        if self.mode == IDBTransactionMode::Versionchange {
            // Step 7.3.1: Let request be the open request associated with transaction.
            // Step 7.3.2: Set request’s transaction to null.
            global
                .ensure_indexeddb_factory(cx)
                .clear_open_request_transaction_for_txn(self);
            let _ = self.get_idb_thread().send(IndexedDBThreadMsg::Sync(
                SyncOperation::UpgradeTransactionFinished {
                    origin: global.origin().immutable().clone(),
                    db_name: self.db.name().clone(),
                    txn: self.id,
                    committed: false,
                },
            ));
        }
        // https://w3c.github.io/IndexedDB/#transaction-lifetime
        // Step 6: When a transaction is committed or aborted, its state is set to finished.
        self.finished.set(true);
        self.drop_prefetched_records();
        self.version_change_old_version.set(None);
        self.version_change_old_object_stores
            .safe_borrow_mut(cx.no_gc())
            .take();
        self.notify_backend_transaction_finished();
        self.db.remove_transaction(self);
        if self.registered_in_global.get() {
            global
                .ensure_indexeddb_factory(cx)
                .unregister_indexeddb_transaction(self);
        }
    }

    fn send_abort_to_backend(&self) {
        let global = self.global();
        let _ = self
            .get_idb_thread()
            .send(IndexedDBThreadMsg::Sync(SyncOperation::Abort {
                origin: global.origin().immutable().clone(),
                db_name: self.db.name().clone(),
                txn: self.id,
            }));
    }

    /// Aborts the transaction in the backend when its global goes away, which then fires no
    /// more events: the backend rolls back what it wrote, and the transaction holds back no
    /// other transaction any more.
    pub(crate) fn abort_when_global_goes_away(&self) {
        if self.finished.get() {
            return;
        }
        self.finished.set(true);
        self.drop_prefetched_records();
        self.requests.borrow_mut().clear();
        self.index_populations.borrow_mut().clear();
        self.send_abort_to_backend();
        self.notify_backend_transaction_finished();
        self.db.remove_transaction(self);
    }

    fn notify_backend_transaction_finished(&self) {
        let global = self.global();
        let _ = self.get_idb_thread().send(IndexedDBThreadMsg::Sync(
            SyncOperation::TransactionFinished {
                origin: global.origin().immutable().clone(),
                db_name: self.db.name().clone(),
                txn: self.id,
            },
        ));
    }

    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>, the queued task of step 2.5,
    /// which is the task that delivered the backend's result.
    fn finalize_commit(&self, cx: &mut JSContext) {
        self.committing.set(false);
        self.commit_started.set(false);
        self.version_change_old_version.set(None);
        self.version_change_old_object_stores
            .safe_borrow_mut(cx.no_gc())
            .take();
        if self.mode == IDBTransactionMode::Versionchange {
            // Step 2.5.1: If transaction is an upgrade transaction, then set transaction’s
            // connection’s associated database’s upgrade transaction to null.
            self.db.clear_upgrade_transaction(self);
        }
        // Step 2.5.2: Set transaction’s state to finished.
        self.finished.set(true);
        self.drop_prefetched_records();
        let global = self.global();
        let event = Event::new(
            cx,
            &global,
            Atom::from("complete"),
            EventBubbles::DoesNotBubble,
            EventCancelable::NotCancelable,
        );
        // Step 2.5.3: Fire an event named complete at transaction.
        event.fire(cx, self.upcast());
        if self.mode == IDBTransactionMode::Versionchange {
            // Step 2.5.4: If transaction is an upgrade transaction, then let request be the
            // request associated with transaction and set request’s transaction to null.
            global
                .ensure_indexeddb_factory(cx)
                .clear_open_request_transaction_for_txn(self);
            let _ = self.get_idb_thread().send(IndexedDBThreadMsg::Sync(
                SyncOperation::UpgradeTransactionFinished {
                    origin: global.origin().immutable().clone(),
                    db_name: self.db.name().clone(),
                    txn: self.id,
                    committed: true,
                },
            ));
        }
        self.notify_backend_transaction_finished();
        self.db.remove_transaction(self);
        if self.registered_in_global.get() {
            global
                .ensure_indexeddb_factory(cx)
                .unregister_indexeddb_transaction(self);
        }
    }

    fn get_idb_thread(&self) -> GenericSender<IndexedDBThreadMsg> {
        self.global().storage_threads().sender()
    }
}

impl IDBTransactionMethods<crate::DomTypeHolder> for IDBTransaction {
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-db>
    fn Db(&self) -> DomRoot<IDBDatabase> {
        DomRoot::from_ref(&*self.db)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-objectstore>
    fn ObjectStore(
        &self,
        cx: &mut JSContext,
        name: DOMString,
    ) -> Fallible<DomRoot<IDBObjectStore>> {
        // Step 1: If transaction has finished, throw an "InvalidStateError" DOMException.
        if self.finished.get() || self.abort_initiated.get() {
            return Err(Error::InvalidState(None));
        }

        // Step 2: Let store be the object store named name in this’s scope, or throw a
        // "NotFoundError" DOMException if none.
        // For versionchange transactions, the scope is the connection's object store set,
        // which follows the changes of the upgrade.
        let Some(store) = self
            .db
            .object_store_named(&name_from_dom_string(&name))
            .filter(|store| {
                self.mode == IDBTransactionMode::Versionchange || self.scope.contains(&store.id)
            })
        else {
            return Err(Error::NotFound(None));
        };

        // Step 3: Each call to this method on the same
        // IDBTransaction instance with the same name
        // returns the same IDBObjectStore instance.
        if let Some(handle) = self.object_store_handle(store.id) {
            return Ok(handle);
        }

        // Step 4: Return an object store handle associated with store and this.
        let handle = IDBObjectStore::new(cx, &self.global(), &store, self);
        self.register_object_store_handle(&handle);
        Ok(handle)
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-commit>
    fn Commit(&self) -> Fallible<()> {
        // Step 1. If this’s state is not active, then throw an "InvalidStateError" DOMException.
        if !self.active.get() {
            return Err(Error::InvalidState(None));
        }

        // Step 2. Run commit a transaction with this.
        // https://w3c.github.io/IndexedDB/#commit-a-transaction
        // Step 1. Set transaction’s state to committing.
        // Step 2. Run the following steps in parallel:
        // Step 2.1. Wait until every item in transaction’s request list is processed.
        // The backend commits after the requests it has been handed. Script sends the commit once
        // all their results have been delivered, so that a request that fails can still abort the
        // transaction, see `maybe_commit`.
        self.set_active_flag(false);
        self.committing.set(true);
        if !self.commit_if_ready() {
            error!(
                "Failed to send the commit of IndexedDB transaction {}",
                self.id
            );
        }

        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-abort>
    fn Abort(&self, cx: &mut JSContext) -> Fallible<()> {
        // Step 1. If this’s state is committing or finished, then throw an "InvalidStateError"
        // DOMException.
        if self.finished.get() || self.abort_initiated.get() || self.committing.get() {
            return Err(Error::InvalidState(None));
        }
        // Step 2. Set this’s state to inactive and run abort a transaction with this and null.
        self.active.set(false);
        self.abort(cx, Error::Abort(None));

        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-objectstorenames>
    fn ObjectStoreNames(&self, cx: &mut JSContext) -> DomRoot<DOMStringList> {
        // Step 1. If this’s transaction is an upgrade transaction, return a list of names of the
        // object stores in this’s connection’s object store set, sorted.
        if self.mode == IDBTransactionMode::Versionchange {
            self.db.object_store_names(cx)
        } else {
            // Step 2. Otherwise, return a sorted list of names in this’s scope.
            self.object_store_names.as_rooted()
        }
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-mode>
    fn Mode(&self) -> IDBTransactionMode {
        self.mode
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-durability>
    fn Durability(&self) -> IDBTransactionDurability {
        self.durability
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-error>
    fn GetError(&self) -> Option<DomRoot<DOMException>> {
        self.error.get()
    }

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-onabort
    event_handler!(abort, GetOnabort, SetOnabort);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-oncomplete
    event_handler!(complete, GetOncomplete, SetOncomplete);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbtransaction-onerror
    event_handler!(error, GetOnerror, SetOnerror);
}
