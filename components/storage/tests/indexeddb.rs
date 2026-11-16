/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock, mpsc};
use std::time::Duration;

use profile::mem as profile_mem;
use profile_traits::generic_callback::GenericCallback as ProfileGenericCallback;
use servo_base::generic_channel::{self, GenericSend};
use servo_base::id::{
    PIPELINE_NAMESPACE, PipelineNamespace, PipelineNamespaceId, TEST_PAINTER_ID, WebViewId,
};
use servo_config::prefs;
use servo_url::{ImmutableOrigin, ServoUrl};
use storage::ClientStorageThreadFactory;
use storage_traits::StorageThreads;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, ConnectionEvent, ConnectionMsg, IndexPopulationEvent,
    IndexPopulationStep, IndexedDBIndexKey, IndexedDBIndexKeys, IndexedDBKeyRange,
    IndexedDBKeyType, IndexedDBObjectStore, IndexedDBRecord, IndexedDBRecordIndexKeys,
    IndexedDBThreadMsg, IndexedDBTxnId, IndexedDBTxnMode, IndexedDBValue, KeyPath, PutItemResult,
    RequestOutput, StorageBottleSource, SyncOperation, TransactionEvent,
};
use tempfile::TempDir;
use uuid::Uuid;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Defines a `#[test]` for each of the given functions and each storage backend compiled in, in
/// a module named after the backend. With both compiled in, the
/// `storage_ohos_rdb_backend_enabled` pref chooses one per database.
macro_rules! backend_tests {
    ($($test:ident),* $(,)?) => {
        #[cfg(feature = "sqlite-backend")]
        mod sqlite {
            $(
                #[test]
                fn $test() {
                    super::run_test(false, super::$test);
                }
            )*
        }

        #[cfg(ohos_rdb)]
        mod rdb {
            $(
                #[test]
                fn $test() {
                    super::run_test(true, super::$test);
                }
            )*
        }
    };
}

backend_tests!(
    test_indexeddb_roundtrip_and_metadata_persist,
    test_indexeddb_store_names_support_nul_bytes,
    test_indexeddb_version_upgrade_persists_metadata,
    test_indexeddb_duplicate_store_fails_the_upgrade,
    test_indexeddb_aborted_transaction_restores_pre_transaction_state,
    test_indexeddb_aborted_upgrade_reverts_version_and_stores,
    test_indexeddb_cross_type_key_sort_order,
    test_indexeddb_iterate_count_and_reverse,
    test_indexeddb_delete_item_range_respects_open_bounds,
    test_indexeddb_auto_increment_assigns_sequential_keys,
    test_indexeddb_batch_commit_failure_surfaces_an_error,
    test_indexeddb_aborted_put_that_succeeded_leaves_no_record,
    test_indexeddb_writers_with_disjoint_scopes_run_one_after_another,
    test_indexeddb_abort_while_a_batch_runs_frees_the_next_writer,
    test_indexeddb_finished_transaction_without_commit_is_rolled_back,
    test_indexeddb_upgrade_waits_for_no_versionchange_of_a_closed_connection,
    test_indexeddb_results_reach_the_connection_in_request_order,
    test_indexeddb_requests_and_commit_of_an_unknown_transaction_fail,
    test_indexeddb_connection_channel_goes_away_after_close_and_its_transactions,
    test_indexeddb_transaction_of_a_dropped_connection_holds_back_no_one,
    test_indexeddb_requests_outside_the_mode_or_scope_fail_the_transaction,
    test_indexeddb_versionchange_follows_the_open_result_on_one_channel,
    test_indexeddb_create_index_population_runs_before_later_requests,
    test_indexeddb_aborted_pending_upgrade_lets_the_next_open_run,
);

static INDEXEDDB_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn indexeddb_test_lock() -> &'static Mutex<()> {
    INDEXEDDB_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

/// Runs `test` on the RDB backend if `rdb_backend` is set and on SQLite otherwise, while no other
/// IndexedDB test runs.
fn run_test(rdb_backend: bool, test: fn()) {
    let _test_lock = indexeddb_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut preferences = prefs::get().clone();
    preferences.storage_ohos_rdb_backend_enabled = rdb_backend;
    prefs::set(preferences);
    test();
}

fn install_test_namespace() {
    if PIPELINE_NAMESPACE.get().is_none() {
        PipelineNamespace::install(PipelineNamespaceId(1));
    }
}

fn shutdown_storage_group(threads: &StorageThreads) {
    let (client_sender, client_receiver) = generic_channel::channel().unwrap();
    GenericSend::send(
        threads,
        storage_traits::client_storage::ClientStorageThreadMessage::Exit(client_sender),
    )
    .expect("failed to send client storage exit");
    client_receiver
        .recv()
        .expect("failed to receive client storage exit ack");

    let (idb_sender, idb_receiver) = generic_channel::channel().unwrap();
    GenericSend::send(
        threads,
        IndexedDBThreadMsg::Sync(SyncOperation::Exit(idb_sender)),
    )
    .expect("failed to send indexeddb exit");
    idb_receiver
        .recv()
        .expect("failed to receive indexeddb exit ack");

    let (web_storage_sender, web_storage_receiver) = generic_channel::channel().unwrap();
    GenericSend::send(
        threads,
        storage_traits::webstorage_thread::WebStorageThreadMsg::Exit(web_storage_sender),
    )
    .expect("failed to send web storage exit");
    web_storage_receiver
        .recv()
        .expect("failed to receive web storage exit ack");
}

struct IndexedDbTestContext {
    _client_handle: storage_traits::client_storage::ClientStorageThreadHandle,
    _temp_dir: TempDir,
    private_threads: StorageThreads,
    public_threads: StorageThreads,
    bottle_source: StorageBottleSource,
    origin: ImmutableOrigin,
    shutdown: Cell<bool>,
    /// The indexes that [`create_index`] created, by database and store, for which puts send
    /// index keys, as script's metadata has them.
    indexes: RefCell<HashMap<(String, i64), Vec<i64>>>,
}

impl IndexedDbTestContext {
    fn new() -> Self {
        install_test_namespace();
        let temp_dir = tempfile::tempdir().unwrap();
        let config_dir = temp_dir.path().to_path_buf();
        let client_handle: storage_traits::client_storage::ClientStorageThreadHandle =
            ClientStorageThreadFactory::new(Some(config_dir.clone()), false);
        let mem_profiler_chan = profile_mem::Profiler::create();
        let (private_threads, public_threads) =
            storage::new_storage_threads(mem_profiler_chan, Some(config_dir), false);
        let origin = ServoUrl::parse("https://example.com").unwrap().origin();
        let bottle_source = StorageBottleSource {
            client_storage: client_handle.clone(),
            webview: Some(WebViewId::new(TEST_PAINTER_ID)),
        };

        Self {
            _client_handle: client_handle,
            _temp_dir: temp_dir,
            private_threads,
            public_threads,
            bottle_source,
            origin,
            shutdown: Cell::new(false),
            indexes: Default::default(),
        }
    }

    fn shutdown(&self) {
        if self.shutdown.replace(true) {
            return;
        }

        shutdown_storage_group(&self.private_threads);
        shutdown_storage_group(&self.public_threads);
    }
}

impl Drop for IndexedDbTestContext {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn recv_callback_timeout<T>(receiver: &mpsc::Receiver<Option<T>>, what: &str) -> T {
    receiver
        .recv_timeout(TIMEOUT)
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("{what} callback failed"))
}

type RequestRoute = Box<dyn FnOnce(BackendResult<RequestOutput>) + Send>;

/// Where the results that the test connections' result channels carry go: request results by
/// (connection, transaction serial, request id), commit results and schema operation failures by
/// (connection, transaction serial). Results without a route are dropped.
#[derive(Default)]
struct Routes {
    requests: HashMap<(Uuid, u64, u64), RequestRoute>,
    commits: HashMap<(Uuid, u64), mpsc::Sender<Option<BackendResult<()>>>>,
    schema_failures: HashMap<(Uuid, u64), mpsc::Sender<Option<BackendError>>>,
    /// The index population events, by (connection, transaction serial).
    populations: HashMap<(Uuid, u64), mpsc::Sender<(i64, IndexPopulationEvent)>>,
    /// The transaction events of each connection, in the order they arrived, as (transaction
    /// serial, request id or `None` for a commit result).
    log: HashMap<Uuid, Vec<(u64, Option<u64>)>>,
    /// The open request's messages and the versionchange events on each result channel, in the
    /// order they arrived.
    events: HashMap<Uuid, Vec<String>>,
}

static ROUTES: LazyLock<Mutex<Routes>> = LazyLock::new(Default::default);

/// Serial numbers of the transactions that tests create, unique in the test process.
static NEXT_SERIAL: AtomicU64 = AtomicU64::new(1);

fn route(connection: Uuid, event: ConnectionEvent) {
    let mut routes = ROUTES.lock().unwrap();
    let (serial, event) = match event {
        ConnectionEvent::Transaction { serial, event } => (serial, event),
        other => {
            let entry = routes.events.entry(connection).or_default();
            return entry.push(format!("{other:?}"));
        },
    };
    let entry = match event {
        TransactionEvent::Request { request_id, .. } => Some(request_id),
        _ => None,
    };
    routes
        .log
        .entry(connection)
        .or_default()
        .push((serial, entry));
    match event {
        TransactionEvent::Request { request_id, result } => {
            if let Some(route) = routes.requests.remove(&(connection, serial, request_id)) {
                drop(routes);
                route(result);
            }
        },
        TransactionEvent::Committed(result) => {
            if let Some(sender) = routes.commits.remove(&(connection, serial)) {
                let _ = sender.send(Some(result));
            }
        },
        TransactionEvent::SchemaChangeFailed(error) => {
            if let Some(sender) = routes.schema_failures.get(&(connection, serial)) {
                let _ = sender.send(Some(error));
            }
        },
        TransactionEvent::IndexPopulation { index_id, event } => {
            if let Some(sender) = routes.populations.get(&(connection, serial)) {
                let _ = sender.send((index_id, event));
            }
        },
    }
}

/// The result of a request, as the kind of operation reports it.
trait TestReply: Sized + Send + 'static {
    fn from_output(output: RequestOutput) -> Self;
}

macro_rules! test_reply {
    ($type:ty, $variant:ident) => {
        impl TestReply for $type {
            fn from_output(output: RequestOutput) -> Self {
                match output {
                    RequestOutput::$variant(value) => value,
                    other => panic!("unexpected request output {other:?}"),
                }
            }
        }
    };
}
test_reply!(Option<IndexedDBValue>, Value);
test_reply!(Vec<IndexedDBValue>, Values);
test_reply!(Vec<IndexedDBKeyType>, Keys);
test_reply!(Vec<IndexedDBRecord>, Records);
test_reply!(PutItemResult, Put);

impl TestReply for () {
    fn from_output(output: RequestOutput) -> Self {
        assert_eq!(output, RequestOutput::Done);
    }
}

/// Sends the request `request_id` of `txn` and returns the receiver of its result.
fn send_request<T: TestReply>(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    request_id: u64,
    mode: IndexedDBTxnMode,
    operation: AsyncOperation,
) -> mpsc::Receiver<Option<BackendResult<T>>> {
    let (sender, receiver) = mpsc::channel();
    ROUTES.lock().unwrap().requests.insert(
        (txn.connection, txn.serial, request_id),
        Box::new(move |result| {
            let _ = sender.send(Some(result.map(T::from_output)));
        }),
    );
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Async(
            ctx.origin.clone(),
            db_name.into(),
            store,
            txn,
            request_id,
            mode,
            operation,
        ),
    )
    .unwrap();
    receiver
}

/// Opens `db_name` and returns the receiver of the open request's messages. The results of the
/// connection's transactions go to [`ROUTES`].
fn open_database(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    version: Option<u64>,
) -> mpsc::Receiver<Option<ConnectionMsg>> {
    open_database_with(ctx, db_name, version, ())
}

/// [`open_database`], whose result channel keeps `owned` until the backend drops the channel.
fn open_database_with(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    version: Option<u64>,
    owned: impl Send + 'static,
) -> mpsc::Receiver<Option<ConnectionMsg>> {
    let (forward, receiver) = mpsc::channel();
    let id = Uuid::new_v4();
    let results = ProfileGenericCallback::new(move |event| {
        let _ = &owned;
        match event {
            Ok(ConnectionEvent::Open(message)) => {
                ROUTES
                    .lock()
                    .unwrap()
                    .events
                    .entry(id)
                    .or_default()
                    .push(format!("{message:?}"));
                let _ = forward.send(Some(message));
            },
            Ok(event) => route(id, event),
            Err(_) => {
                let _ = forward.send(None);
            },
        }
    })
    .unwrap();
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::OpenDatabase(
            ctx.origin.clone(),
            db_name.into(),
            version,
            id,
            ctx.bottle_source.clone(),
            results,
        )),
    )
    .unwrap();
    receiver
}

/// The id script gives the first store of a database, in tests that have one store.
const STORE: i64 = 1;
/// The ids of the stores `a` and `b` of [`open_database_with_two_stores`].
const A: i64 = 1;
const B: i64 = 2;

/// Schema operations run through the versionchange transaction, exactly like
/// script does (`idbdatabase.rs`). Only a failure is reported, so a receiver
/// that stays empty means the operation succeeded. The failures of all the
/// transaction's schema operations go to the receiver returned last.
fn send_schema_operation(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    transaction: IndexedDBTxnId,
    operation: AsyncSchemaOperation,
) -> mpsc::Receiver<Option<BackendError>> {
    let (sender, receiver) = mpsc::channel();
    ROUTES
        .lock()
        .unwrap()
        .schema_failures
        .insert((transaction.connection, transaction.serial), sender);
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::AsyncSchemaOperation {
            origin: ctx.origin.clone(),
            database_name: db_name.into(),
            store_id: store,
            operation,
            transaction,
        },
    )
    .unwrap();
    receiver
}

/// Creates the store `store_name` with the id `store`, as script does.
fn create_object_store(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    store_name: &str,
    transaction: IndexedDBTxnId,
    key_path: Option<KeyPath>,
    auto_increment: bool,
) -> mpsc::Receiver<Option<BackendError>> {
    send_schema_operation(
        ctx,
        db_name,
        store,
        transaction,
        AsyncSchemaOperation::CreateObjectStore {
            name: store_name.into(),
            key_path,
            auto_increment,
        },
    )
}

fn create_index(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    transaction: IndexedDBTxnId,
    index_id: i64,
    index_name: &str,
    key_path: KeyPath,
    unique: bool,
    multi_entry: bool,
) -> mpsc::Receiver<Option<BackendError>> {
    ctx.indexes
        .borrow_mut()
        .entry((db_name.to_owned(), store))
        .or_default()
        .push(index_id);
    send_schema_operation(
        ctx,
        db_name,
        store,
        transaction,
        AsyncSchemaOperation::CreateIndex {
            index_id,
            name: index_name.into(),
            key_path,
            unique,
            multi_entry,
            populate: false,
        },
    )
}

/// The names of `stores`.
fn names(stores: &[IndexedDBObjectStore]) -> Vec<String> {
    stores.iter().map(|store| store.name.to_string()).collect()
}

/// The committed version and object stores of `db_name`, from the metadata snapshot that a new
/// connection gets. The connection is closed again.
fn committed_metadata(
    ctx: &IndexedDbTestContext,
    db_name: &str,
) -> (u64, Vec<IndexedDBObjectStore>) {
    match recv_callback_timeout(&open_database(ctx, db_name, None), "open reply") {
        ConnectionMsg::Connection {
            id,
            version,
            object_stores,
            ..
        } => {
            close_database(ctx, id, db_name);
            (version, object_stores)
        },
        other => panic!("unexpected open message: {other:?}"),
    }
}

/// Creates a transaction of the open connection `connection`, as script does without waiting
/// for a reply, and returns its id.
fn create_transaction(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    connection: Uuid,
    mode: IndexedDBTxnMode,
    scope: Vec<i64>,
) -> IndexedDBTxnId {
    let txn = IndexedDBTxnId {
        connection,
        serial: NEXT_SERIAL.fetch_add(1, Ordering::Relaxed),
    };
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::CreateTransaction {
            origin: ctx.origin.clone(),
            db_name: db_name.into(),
            txn,
            mode,
            scope,
        }),
    )
    .unwrap();
    txn
}

/// Asks the backend to commit `txn` and returns the receiver of the commit result. Script sends
/// the commit once the results of all requests have arrived; the tests send it earlier too, as
/// the backend commits after the requests it was handed anyway.
fn commit_transaction(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    txn: IndexedDBTxnId,
) -> mpsc::Receiver<Option<BackendResult<()>>> {
    let (sender, receiver) = mpsc::channel();
    ROUTES
        .lock()
        .unwrap()
        .commits
        .insert((txn.connection, txn.serial), sender);
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::Commit {
            origin: ctx.origin.clone(),
            db_name: db_name.into(),
            txn,
        }),
    )
    .unwrap();
    receiver
}

/// Mirror script's commit choreography for a versionchange transaction
/// (`idbtransaction.rs`): the upgrade is only reported finished once the
/// backend has confirmed the queued schema batch is durable.
fn commit_upgrade_transaction(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    transaction: IndexedDBTxnId,
) {
    let receiver = commit_transaction(ctx, db_name, transaction);
    recv_callback_timeout(&receiver, "upgrade commit reply")
        .expect("the upgrade transaction failed to commit");
    finish_upgrade_transaction(ctx, transaction, db_name, true);
    finish_transaction(ctx, db_name, transaction);
}

/// Mirror script's abort choreography for a versionchange transaction
/// (`idbtransaction.rs`): the backend abort is sent first, so the engine rolls
/// back after the transaction's requests and before the upgrade is finished.
fn abort_upgrade_transaction(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    transaction: IndexedDBTxnId,
) {
    abort_transaction(ctx, db_name, transaction);
    finish_upgrade_transaction(ctx, transaction, db_name, false);
    finish_transaction(ctx, db_name, transaction);
}

fn close_database(ctx: &IndexedDbTestContext, id: Uuid, db_name: &str) {
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::CloseDatabase(
            ctx.origin.clone(),
            id,
            db_name.into(),
        )),
    )
    .unwrap();
}

fn finish_upgrade_transaction(
    ctx: &IndexedDbTestContext,
    txn: IndexedDBTxnId,
    db_name: &str,
    committed: bool,
) {
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::UpgradeTransactionFinished {
            txn,
            db_name: db_name.into(),
            origin: ctx.origin.clone(),
            committed,
        }),
    )
    .unwrap();
}

fn finish_transaction(ctx: &IndexedDbTestContext, db_name: &str, txn: IndexedDBTxnId) {
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::TransactionFinished {
            origin: ctx.origin.clone(),
            db_name: db_name.into(),
            txn,
        }),
    )
    .unwrap();
}

fn put_item(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key: IndexedDBKeyType,
    value: Vec<u8>,
) -> mpsc::Receiver<Option<BackendResult<PutItemResult>>> {
    put_item_request(ctx, db_name, store, txn, 1, Some(key), value, true)
}

fn put_item_request(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    request_id: u64,
    key: Option<IndexedDBKeyType>,
    value: Vec<u8>,
    should_overwrite: bool,
) -> mpsc::Receiver<Option<BackendResult<PutItemResult>>> {
    let index_keys = ctx
        .indexes
        .borrow()
        .get(&(db_name.to_owned(), store))
        .into_iter()
        .flatten()
        .map(|&index_id| IndexedDBIndexKeys {
            index_id,
            keys: vec![],
        })
        .collect();
    indexed_put_request(
        ctx,
        db_name,
        store,
        txn,
        request_id,
        key,
        value,
        should_overwrite,
        index_keys,
    )
}

/// A put with the record's keys in the store's indexes.
#[allow(clippy::too_many_arguments)]
fn indexed_put_request(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    request_id: u64,
    key: Option<IndexedDBKeyType>,
    value: Vec<u8>,
    should_overwrite: bool,
    index_keys: Vec<IndexedDBIndexKeys>,
) -> mpsc::Receiver<Option<BackendResult<PutItemResult>>> {
    send_request(
        ctx,
        db_name,
        store,
        txn,
        request_id,
        IndexedDBTxnMode::Readwrite,
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
            key,
            value,
            should_overwrite,
            index_keys,
        }),
    )
}

fn get_all_keys_request(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key_range: IndexedDBKeyRange,
    request_id: u64,
) -> mpsc::Receiver<Option<BackendResult<Vec<IndexedDBKeyType>>>> {
    send_request(
        ctx,
        db_name,
        store,
        txn,
        request_id,
        IndexedDBTxnMode::Readonly,
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllKeys {
            index: None,
            key_range,
            count: None,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn iterate_request(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key_range: IndexedDBKeyRange,
    count: Option<u32>,
    reverse: bool,
    request_id: u64,
) -> mpsc::Receiver<Option<BackendResult<Vec<IndexedDBRecord>>>> {
    send_request(
        ctx,
        db_name,
        store,
        txn,
        request_id,
        IndexedDBTxnMode::Readonly,
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Iterate {
            key_range,
            count,
            reverse,
        }),
    )
}

fn get_item(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key: IndexedDBKeyType,
) -> mpsc::Receiver<Option<BackendResult<Option<IndexedDBValue>>>> {
    send_request(
        ctx,
        db_name,
        store,
        txn,
        1,
        IndexedDBTxnMode::Readonly,
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
            index: None,
            key_range: IndexedDBKeyRange::only(key),
        }),
    )
}

fn remove_item(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key: IndexedDBKeyType,
) -> mpsc::Receiver<Option<BackendResult<()>>> {
    remove_item_request(ctx, db_name, store, txn, IndexedDBKeyRange::only(key))
}

fn remove_item_request(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store: i64,
    txn: IndexedDBTxnId,
    key_range: IndexedDBKeyRange,
) -> mpsc::Receiver<Option<BackendResult<()>>> {
    send_request(
        ctx,
        db_name,
        store,
        txn,
        1,
        IndexedDBTxnMode::Readwrite,
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem { key_range }),
    )
}

fn expect_upgrade_and_initialize_database(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    store_name: &str,
    index_name: &str,
    store_key_path: Option<KeyPath>,
) -> Uuid {
    let receiver = open_database(ctx, db_name, Some(1));

    loop {
        match recv_callback_timeout(&receiver, "open database reply") {
            ConnectionMsg::Upgrade {
                id: _,
                name,
                version,
                old_version,
                transaction,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(version, 1);
                assert_eq!(old_version, 0);
                assert!(object_stores.is_empty());

                create_object_store(
                    ctx,
                    db_name,
                    STORE,
                    store_name,
                    transaction,
                    store_key_path.clone(),
                    false,
                );
                create_index(
                    ctx,
                    db_name,
                    STORE,
                    transaction,
                    1,
                    index_name,
                    KeyPath::String("author".to_string()),
                    false,
                    false,
                );
                commit_upgrade_transaction(ctx, db_name, transaction);
            },
            ConnectionMsg::Connection {
                id,
                name,
                version,
                upgraded,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(version, 1);
                assert!(upgraded);
                assert_eq!(names(&object_stores), vec![store_name]);
                return id;
            },
            other => panic!("unexpected connection message: {other:?}"),
        }
    }
}

fn reopen_database(ctx: &IndexedDbTestContext, db_name: &str, store_name: &str) -> Uuid {
    let receiver = open_database(ctx, db_name, None);
    loop {
        match recv_callback_timeout(&receiver, "reopen database reply") {
            ConnectionMsg::Connection {
                id,
                name,
                version,
                upgraded,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(version, 1);
                assert!(!upgraded);
                assert_eq!(names(&object_stores), vec![store_name]);
                return id;
            },
            other => panic!("unexpected reopen message: {other:?}"),
        }
    }
}

fn test_indexeddb_roundtrip_and_metadata_persist() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-roundtrip";
    let store_name = "books";
    let index_name = "by_author";
    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        index_name,
        Some(KeyPath::String("id".to_string())),
    );

    let (version, stores) = committed_metadata(&ctx, db_name);
    assert_eq!(version, 1);
    assert_eq!(stores.len(), 1);
    let store = &stores[0];
    assert_eq!(store.id, STORE);
    assert_eq!(store.name, store_name);
    assert_eq!(store.key_path, Some(KeyPath::String("id".to_string())));
    assert_eq!(store.has_key_generator, false);
    assert_eq!(store.indexes.len(), 1);
    assert_eq!(store.indexes[0].id, 1);
    assert_eq!(store.indexes[0].name, index_name);
    assert_eq!(
        store.indexes[0].key_path,
        KeyPath::String("author".to_string())
    );
    assert!(!store.indexes[0].multi_entry);
    assert!(!store.indexes[0].unique);
    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let put_result = put_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
        b"Moby Dick".to_vec(),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let put_msg = recv_callback_timeout(&put_result, "put item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert_eq!(
        put_msg,
        PutItemResult::Key(IndexedDBKeyType::String("book-1".into()))
    );
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let roundtrip = get_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let roundtrip_msg = recv_callback_timeout(&roundtrip, "get item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert_eq!(
        roundtrip_msg,
        Some(IndexedDBValue {
            primary_key: IndexedDBKeyType::String("book-1".into()),
            value: b"Moby Dick".to_vec(),
        })
    );
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let remove_result = remove_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let remove_msg = recv_callback_timeout(&remove_result, "remove item reply");
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert!(remove_msg.is_ok());
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let deleted = get_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let deleted_msg = recv_callback_timeout(&deleted, "get deleted item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert_eq!(deleted_msg, None);
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    close_database(&ctx, connection_id, db_name);
    let reopened_id = reopen_database(&ctx, db_name, store_name);
    assert_ne!(reopened_id, Uuid::nil());
    let (version, stores) = committed_metadata(&ctx, db_name);
    assert_eq!(version, 1);
    assert_eq!(stores[0].indexes.len(), 1);

    ctx.shutdown();
}

fn test_indexeddb_store_names_support_nul_bytes() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-nul";
    let store_name = "books\0archive";

    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        "by_author",
        Some(KeyPath::String("id".to_string())),
    );

    assert_eq!(
        names(&committed_metadata(&ctx, db_name).1),
        vec![store_name]
    );

    close_database(&ctx, connection_id, db_name);
    let reopened_id = reopen_database(&ctx, db_name, store_name);
    assert_ne!(reopened_id, Uuid::nil());
    assert_eq!(
        names(&committed_metadata(&ctx, db_name).1),
        vec![store_name]
    );

    ctx.shutdown();
}

fn test_indexeddb_version_upgrade_persists_metadata() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-upgrade";
    let store_name = "books";

    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        "by_author",
        Some(KeyPath::String("id".to_string())),
    );
    close_database(&ctx, connection_id, db_name);

    let receiver = open_database(&ctx, db_name, Some(2));
    let _upgraded_id = loop {
        match recv_callback_timeout(&receiver, "upgrade database reply") {
            ConnectionMsg::Upgrade {
                id: _,
                name,
                version,
                old_version,
                transaction,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(old_version, 1);
                assert_eq!(version, 2);
                assert_eq!(names(&object_stores), vec![store_name]);
                commit_upgrade_transaction(&ctx, db_name, transaction);
            },
            ConnectionMsg::Connection {
                id,
                name,
                version,
                upgraded,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(version, 2);
                assert!(upgraded);
                assert_eq!(names(&object_stores), vec![store_name]);
                break id;
            },
            other => panic!("unexpected upgrade message: {other:?}"),
        }
    };

    assert_eq!(committed_metadata(&ctx, db_name).0, 2);
    ctx.shutdown();
}

/// A store with a new id but the name of an existing store doesn't fit the metadata, so the
/// backend fails the upgrade transaction and its commit; script never sends it, as it checks the
/// name first.
fn test_indexeddb_duplicate_store_fails_the_upgrade() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-duplicate-store";
    let store_name = "books";
    let key_path = Some(KeyPath::String("id".to_string()));
    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        "by_author",
        key_path.clone(),
    );
    close_database(&ctx, connection_id, db_name);

    let receiver = open_database(&ctx, db_name, Some(2));
    match recv_callback_timeout(&receiver, "duplicate-store upgrade reply") {
        ConnectionMsg::Upgrade { transaction, .. } => {
            let duplicate = create_object_store(
                &ctx,
                db_name,
                STORE + 1,
                store_name,
                transaction,
                key_path.clone(),
                false,
            );
            let commit = commit_transaction(&ctx, db_name, transaction);
            assert!(recv_callback_timeout(&commit, "upgrade commit reply").is_err());
            assert!(matches!(
                recv_callback_timeout(&duplicate, "duplicate store error"),
                BackendError::DbErr(_)
            ));
            abort_upgrade_transaction(&ctx, db_name, transaction);
        },
        other => panic!("unexpected duplicate-store message: {other:?}"),
    }
    assert!(matches!(
        recv_callback_timeout(&receiver, "aborted open reply"),
        ConnectionMsg::AbortError { .. }
    ));

    let (version, stores) = committed_metadata(&ctx, db_name);
    assert_eq!((version, names(&stores)), (1, vec![store_name.to_owned()]));
    ctx.shutdown();
}

/// Aborts `txn`, which reports nothing.
fn abort_transaction(ctx: &IndexedDbTestContext, db_name: &str, txn: IndexedDBTxnId) {
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::Abort {
            origin: ctx.origin.clone(),
            db_name: db_name.into(),
            txn,
        }),
    )
    .unwrap();
}

fn test_indexeddb_aborted_transaction_restores_pre_transaction_state() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-abort-txn";
    let store_name = "books";
    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        "by_author",
        Some(KeyPath::String("id".to_string())),
    );

    // Write inside a transaction, then abort instead of committing. An earlier
    // live transaction over the same store keeps the aborted one from starting,
    // so its queued request never runs and gets no reply. A put that has run is
    // rolled back instead, see
    // test_indexeddb_aborted_put_that_succeeded_leaves_no_record.
    let blocking_txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let _put_result = put_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-aborted".into()),
        b"Never Committed".to_vec(),
    );
    abort_transaction(&ctx, db_name, txn);
    finish_transaction(&ctx, db_name, txn);
    finish_transaction(&ctx, db_name, blocking_txn);

    // <https://w3c.github.io/IndexedDB/#abort-a-transaction>
    // "... the implementation must undo (roll back) any changes that were
    // made to the database during that transaction."
    // The pre-transaction state is restored: the key is absent.
    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let lookup = get_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-aborted".into()),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let lookup_msg = recv_callback_timeout(&lookup, "get aborted item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert_eq!(lookup_msg, None);
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_aborted_upgrade_reverts_version_and_stores() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-abort-upgrade";
    let store_name = "books";
    let connection_id = expect_upgrade_and_initialize_database(
        &ctx,
        db_name,
        store_name,
        "by_author",
        Some(KeyPath::String("id".to_string())),
    );

    // Commit one row so the aborted upgrade can be shown to leave data intact.
    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let put_result = put_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
        b"Moby Dick".to_vec(),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    recv_callback_timeout(&put_result, "put item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);
    close_database(&ctx, connection_id, db_name);

    // Request an upgrade to version 2, create a store inside the upgrade
    // transaction, then finish it WITHOUT committing. Per
    // https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction the open
    // request is aborted and the database reverts to its pre-upgrade state.
    let receiver = open_database(&ctx, db_name, Some(2));
    let aborted;
    loop {
        match recv_callback_timeout(&receiver, "upgrade-to-v2 reply") {
            ConnectionMsg::Upgrade {
                id: _,
                name,
                version,
                old_version,
                transaction,
                object_stores,
            } => {
                assert_eq!(name, db_name);
                assert_eq!(version, 2);
                assert_eq!(old_version, 1);
                assert_eq!(names(&object_stores), vec![store_name]);
                create_object_store(
                    &ctx,
                    db_name,
                    STORE + 1,
                    "phantom",
                    transaction,
                    None,
                    false,
                );
                send_schema_operation(
                    &ctx,
                    db_name,
                    STORE,
                    transaction,
                    AsyncSchemaOperation::DeleteObjectStore,
                );
                abort_upgrade_transaction(&ctx, db_name, transaction);
            },
            ConnectionMsg::AbortError { name, id: _ } => {
                assert_eq!(name, db_name);
                aborted = true;
                break;
            },
            other => panic!("unexpected upgrade-abort message: {other:?}"),
        }
    }
    assert!(aborted, "aborted upgrade must abort the open request");

    // Version reverted, the phantom store is gone, and the original store,
    // deleted in the aborted upgrade, is back with its data.
    assert_eq!(committed_metadata(&ctx, db_name).0, 1);
    let connection_id = reopen_database(&ctx, db_name, store_name);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let lookup = get_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-1".into()),
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let lookup_msg = recv_callback_timeout(&lookup, "get preserved item reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "commit reply");
    assert_eq!(
        lookup_msg.map(|value| value.value),
        Some(b"Moby Dick".to_vec())
    );
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_cross_type_key_sort_order() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-sort-order";
    let store_name = "items";
    let connection_id =
        expect_upgrade_and_initialize_database(&ctx, db_name, store_name, "by_author", None);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let mut receivers = Vec::new();
    for (request_id, key, value) in [
        (1, IndexedDBKeyType::Number(1.0), b"number".to_vec()),
        (2, IndexedDBKeyType::Date(1.0), b"date".to_vec()),
        (3, IndexedDBKeyType::String("1".into()), b"string".to_vec()),
        (4, IndexedDBKeyType::Binary(vec![1]), b"binary".to_vec()),
        (
            5,
            IndexedDBKeyType::Array(vec![IndexedDBKeyType::Number(1.0)]),
            b"array".to_vec(),
        ),
    ] {
        receivers.push((
            request_id,
            put_item_request(
                &ctx,
                db_name,
                STORE,
                txn,
                request_id,
                Some(key),
                value,
                true,
            ),
        ));
    }
    let commit_result = commit_transaction(&ctx, db_name, txn);
    for (request_id, receiver) in receivers {
        let result = recv_callback_timeout(&receiver, "sorted put reply");
        assert!(result.is_ok(), "request {request_id} should succeed");
    }
    let commit_msg = recv_callback_timeout(&commit_result, "sorted commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let keys = get_all_keys_request(&ctx, db_name, STORE, txn, IndexedDBKeyRange::default(), 1);
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let keys = recv_callback_timeout(&keys, "sorted keys reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "sorted read commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    assert_eq!(
        keys,
        vec![
            IndexedDBKeyType::Number(1.0),
            IndexedDBKeyType::Date(1.0),
            IndexedDBKeyType::String("1".into()),
            IndexedDBKeyType::Binary(vec![1]),
            IndexedDBKeyType::Array(vec![IndexedDBKeyType::Number(1.0)]),
        ]
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_iterate_count_and_reverse() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-iterate";
    let store_name = "items";
    let connection_id =
        expect_upgrade_and_initialize_database(&ctx, db_name, store_name, "by_author", None);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let mut receivers = Vec::new();
    for request_id in 1..=5 {
        let key = IndexedDBKeyType::Number(request_id as f64);
        receivers.push((
            request_id,
            put_item_request(
                &ctx,
                db_name,
                STORE,
                txn,
                request_id,
                Some(key),
                vec![request_id as u8],
                true,
            ),
        ));
    }
    let commit_result = commit_transaction(&ctx, db_name, txn);
    for (request_id, receiver) in receivers {
        let result = recv_callback_timeout(&receiver, "iterate put reply");
        assert!(result.is_ok(), "request {request_id} should succeed");
    }
    let commit_msg = recv_callback_timeout(&commit_result, "iterate commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let number = |n: f64| IndexedDBKeyType::Number(n);
    let cases = [
        (
            IndexedDBKeyRange::default(),
            None,
            false,
            vec![1, 2, 3, 4, 5],
        ),
        (
            IndexedDBKeyRange::default(),
            None,
            true,
            vec![1, 2, 3, 4, 5],
        ),
        (
            IndexedDBKeyRange::lower_bound(number(2.0), true),
            Some(2),
            false,
            vec![3, 4],
        ),
        (
            IndexedDBKeyRange::upper_bound(number(4.0), true),
            Some(2),
            true,
            vec![2, 3],
        ),
        (
            IndexedDBKeyRange::new(Some(number(2.0)), Some(number(4.0)), false, false),
            Some(10),
            true,
            vec![2, 3, 4],
        ),
        (
            IndexedDBKeyRange::lower_bound(number(5.0), true),
            Some(1),
            false,
            vec![],
        ),
    ];

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let receivers: Vec<_> = cases
        .iter()
        .zip(1..)
        .map(|((key_range, count, reverse, _), request_id)| {
            iterate_request(
                &ctx,
                db_name,
                STORE,
                txn,
                key_range.clone(),
                *count,
                *reverse,
                request_id,
            )
        })
        .collect();
    let commit_result = commit_transaction(&ctx, db_name, txn);
    for (receiver, (key_range, count, reverse, expected)) in receivers.iter().zip(&cases) {
        let records = recv_callback_timeout(receiver, "iterate reply").unwrap();
        let keys: Vec<_> = records.iter().map(|record| record.key.clone()).collect();
        let values: Vec<_> = records.iter().map(|record| record.value.clone()).collect();
        assert_eq!(
            keys,
            expected
                .iter()
                .map(|n| number(*n as f64))
                .collect::<Vec<_>>(),
            "{key_range:?} count {count:?} reverse {reverse}"
        );
        assert_eq!(
            values,
            expected.iter().map(|n| vec![*n as u8]).collect::<Vec<_>>()
        );
    }
    let commit_msg = recv_callback_timeout(&commit_result, "iterate read commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_delete_item_range_respects_open_bounds() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-open-bounds";
    let store_name = "items";
    let connection_id =
        expect_upgrade_and_initialize_database(&ctx, db_name, store_name, "by_author", None);

    fn remaining_keys_after_delete(
        ctx: &IndexedDbTestContext,
        db_name: &str,
        connection: Uuid,
        store: i64,
        lower: i32,
        upper: i32,
        lower_open: bool,
        upper_open: bool,
    ) -> Vec<i32> {
        let txn = create_transaction(
            ctx,
            db_name,
            connection,
            IndexedDBTxnMode::Readwrite,
            vec![store],
        );
        let mut receivers = Vec::new();
        for key in 1..=10 {
            receivers.push((
                key,
                put_item_request(
                    ctx,
                    db_name,
                    store,
                    txn,
                    key as u64,
                    Some(IndexedDBKeyType::Number(key as f64)),
                    vec![key as u8],
                    true,
                ),
            ));
        }
        let commit_result = commit_transaction(ctx, db_name, txn);
        for (request_id, receiver) in receivers {
            let result = recv_callback_timeout(&receiver, "seed put reply");
            assert!(result.is_ok(), "seed request {request_id} should succeed");
        }
        let commit_msg = recv_callback_timeout(&commit_result, "seed commit reply");
        assert!(commit_msg.is_ok());
        finish_transaction(ctx, db_name, txn);

        let txn = create_transaction(
            ctx,
            db_name,
            connection,
            IndexedDBTxnMode::Readwrite,
            vec![store],
        );
        let delete = remove_item_request(
            ctx,
            db_name,
            store,
            txn,
            IndexedDBKeyRange::new(
                Some(IndexedDBKeyType::Number(lower as f64)),
                Some(IndexedDBKeyType::Number(upper as f64)),
                lower_open,
                upper_open,
            ),
        );
        let commit_result = commit_transaction(ctx, db_name, txn);
        let delete_msg = recv_callback_timeout(&delete, "range delete reply");
        assert!(delete_msg.is_ok());
        let commit_msg = recv_callback_timeout(&commit_result, "range delete commit reply");
        assert!(commit_msg.is_ok());
        finish_transaction(ctx, db_name, txn);

        let txn = create_transaction(
            ctx,
            db_name,
            connection,
            IndexedDBTxnMode::Readonly,
            vec![store],
        );
        let keys = get_all_keys_request(ctx, db_name, store, txn, IndexedDBKeyRange::default(), 1);
        let commit_result = commit_transaction(ctx, db_name, txn);
        let keys = recv_callback_timeout(&keys, "remaining keys reply").unwrap();
        let commit_msg = recv_callback_timeout(&commit_result, "remaining keys commit reply");
        assert!(commit_msg.is_ok());
        finish_transaction(ctx, db_name, txn);

        keys.into_iter()
            .map(|key| match key {
                IndexedDBKeyType::Number(number) => number as i32,
                other => panic!("Expected numeric key, got {other:?}"),
            })
            .collect()
    }

    assert_eq!(
        remaining_keys_after_delete(&ctx, db_name, connection_id, STORE, 3, 8, false, false),
        vec![1, 2, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete(&ctx, db_name, connection_id, STORE, 3, 8, true, false),
        vec![1, 2, 3, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete(&ctx, db_name, connection_id, STORE, 3, 8, false, true),
        vec![1, 2, 8, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete(&ctx, db_name, connection_id, STORE, 3, 8, true, true),
        vec![1, 2, 3, 8, 9, 10]
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_auto_increment_assigns_sequential_keys() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-auto-increment";
    let store_name = "items";

    let receiver = open_database(&ctx, db_name, Some(1));
    let connection_id = loop {
        match recv_callback_timeout(&receiver, "auto-increment open reply") {
            ConnectionMsg::Upgrade { transaction, .. } => {
                create_object_store(&ctx, db_name, STORE, store_name, transaction, None, true);
                commit_upgrade_transaction(&ctx, db_name, transaction);
            },
            ConnectionMsg::Connection { id, upgraded, .. } => {
                assert!(upgraded);
                break id;
            },
            other => panic!("unexpected auto-increment message: {other:?}"),
        }
    };

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let first = put_item_request(&ctx, db_name, STORE, txn, 1, None, b"first".to_vec(), true);
    let second = put_item_request(&ctx, db_name, STORE, txn, 2, None, b"second".to_vec(), true);
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let first_msg = recv_callback_timeout(&first, "first auto-increment put reply").unwrap();
    let second_msg = recv_callback_timeout(&second, "second auto-increment put reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "auto-increment commit reply");
    assert_eq!(first_msg, PutItemResult::Key(IndexedDBKeyType::Number(1.0)));
    assert_eq!(
        second_msg,
        PutItemResult::Key(IndexedDBKeyType::Number(2.0))
    );
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![STORE],
    );
    let keys = get_all_keys_request(&ctx, db_name, STORE, txn, IndexedDBKeyRange::default(), 1);
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let keys = recv_callback_timeout(&keys, "auto-increment keys reply").unwrap();
    let commit_msg = recv_callback_timeout(&commit_result, "auto-increment read commit reply");
    assert!(commit_msg.is_ok());
    finish_transaction(&ctx, db_name, txn);

    assert_eq!(
        keys,
        vec![IndexedDBKeyType::Number(1.0), IndexedDBKeyType::Number(2.0),]
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn test_indexeddb_batch_commit_failure_surfaces_an_error() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-batch-failure";
    let store_name = "items";
    let connection_id =
        expect_upgrade_and_initialize_database(&ctx, db_name, store_name, "by_author", None);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let failing = put_item_request(
        &ctx,
        db_name,
        STORE,
        txn,
        1,
        None,
        b"missing key".to_vec(),
        true,
    );
    let commit_result = commit_transaction(&ctx, db_name, txn);
    let failing_msg = recv_callback_timeout(&failing, "missing key batch put reply");
    assert!(failing_msg.is_err());
    let _ = recv_callback_timeout(&commit_result, "batch commit reply");
    finish_transaction(&ctx, db_name, txn);

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

fn get_item_in_new_readonly_transaction(
    ctx: &IndexedDbTestContext,
    db_name: &str,
    connection: Uuid,
    store: i64,
    key: IndexedDBKeyType,
) -> Option<Vec<u8>> {
    let txn = create_transaction(
        ctx,
        db_name,
        connection,
        IndexedDBTxnMode::Readonly,
        vec![store],
    );
    let lookup = get_item(ctx, db_name, store, txn, key);
    let commit_result = commit_transaction(ctx, db_name, txn);
    let value = recv_callback_timeout(&lookup, "get item reply")
        .unwrap()
        .map(|value| value.value);
    assert!(recv_callback_timeout(&commit_result, "readonly commit reply").is_ok());
    finish_transaction(ctx, db_name, txn);
    value
}

/// <https://w3c.github.io/IndexedDB/#abort-a-transaction>: "All the changes made to the
/// database by the transaction are reverted", also those of requests that have succeeded.
/// Before the storage transaction spanned the whole IndexedDB transaction, a put that had
/// replied was durable already and survived the abort.
fn test_indexeddb_aborted_put_that_succeeded_leaves_no_record() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-aborted-put";
    let store_name = "books";
    let connection_id =
        expect_upgrade_and_initialize_database(&ctx, db_name, store_name, "by_author", None);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![STORE],
    );
    let put_result = put_item(
        &ctx,
        db_name,
        STORE,
        txn,
        IndexedDBKeyType::String("book-aborted".into()),
        b"Never Committed".to_vec(),
    );
    assert_eq!(
        recv_callback_timeout(&put_result, "put item reply").unwrap(),
        PutItemResult::Key(IndexedDBKeyType::String("book-aborted".into()))
    );

    abort_transaction(&ctx, db_name, txn);
    finish_transaction(&ctx, db_name, txn);

    assert_eq!(
        get_item_in_new_readonly_transaction(
            &ctx,
            db_name,
            connection_id,
            STORE,
            IndexedDBKeyType::String("book-aborted".into()),
        ),
        None
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// Each readwrite transaction is the one storage transaction of the database's connection while
/// it runs, so a second writer waits for the first to finish even if their scopes don't
/// overlap. Implementations may do that: the spec only allows such writers to run in parallel.
fn test_indexeddb_writers_with_disjoint_scopes_run_one_after_another() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-serial-writers";
    let receiver = open_database(&ctx, db_name, Some(1));
    let connection_id = loop {
        match recv_callback_timeout(&receiver, "serial writers open reply") {
            ConnectionMsg::Upgrade { transaction, .. } => {
                for (store, store_name) in [(A, "a"), (B, "b")] {
                    create_object_store(&ctx, db_name, store, store_name, transaction, None, false);
                }
                commit_upgrade_transaction(&ctx, db_name, transaction);
            },
            ConnectionMsg::Connection { id, .. } => break id,
            other => panic!("unexpected serial writers message: {other:?}"),
        }
    };

    let first = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    let second = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![B],
    );
    let key = || IndexedDBKeyType::Number(1.0);
    let first_put = put_item(&ctx, db_name, A, first, key(), b"first".to_vec());
    recv_callback_timeout(&first_put, "first writer put reply").unwrap();

    let second_put = put_item(&ctx, db_name, B, second, key(), b"second".to_vec());
    assert!(
        second_put.recv_timeout(Duration::from_millis(200)).is_err(),
        "The second writer must not start while the first is live"
    );

    let commit_result = commit_transaction(&ctx, db_name, first);
    assert!(recv_callback_timeout(&commit_result, "first writer commit reply").is_ok());
    finish_transaction(&ctx, db_name, first);

    recv_callback_timeout(&second_put, "second writer put reply").unwrap();
    let commit_result = commit_transaction(&ctx, db_name, second);
    assert!(recv_callback_timeout(&commit_result, "second writer commit reply").is_ok());
    finish_transaction(&ctx, db_name, second);

    for (store, value) in [(A, &b"first"[..]), (B, &b"second"[..])] {
        assert_eq!(
            get_item_in_new_readonly_transaction(&ctx, db_name, connection_id, store, key()),
            Some(value.to_vec())
        );
    }

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// Opens `db_name` at version 1 with the stores `a` and `b`.
fn open_database_with_two_stores(ctx: &IndexedDbTestContext, db_name: &str) -> Uuid {
    let receiver = open_database(ctx, db_name, Some(1));
    loop {
        match recv_callback_timeout(&receiver, "two stores open reply") {
            ConnectionMsg::Upgrade { transaction, .. } => {
                for (store, store_name) in [(A, "a"), (B, "b")] {
                    create_object_store(ctx, db_name, store, store_name, transaction, None, false);
                }
                commit_upgrade_transaction(ctx, db_name, transaction);
            },
            ConnectionMsg::Connection { id, .. } => break id,
            other => panic!("unexpected two stores message: {other:?}"),
        }
    }
}

/// Runs a put into `store` in a new readwrite transaction of `connection` and commits it.
fn put_and_commit(ctx: &IndexedDbTestContext, db_name: &str, connection: Uuid, store: i64) {
    let txn = create_transaction(
        ctx,
        db_name,
        connection,
        IndexedDBTxnMode::Readwrite,
        vec![store],
    );
    let put = put_item(
        ctx,
        db_name,
        store,
        txn,
        IndexedDBKeyType::Number(1.0),
        b"next".to_vec(),
    );
    assert!(
        recv_callback_timeout(&put, "next writer put reply").is_ok(),
        "The next writer's put must not meet the earlier writer's SQL transaction"
    );
    let commit_result = commit_transaction(ctx, db_name, txn);
    assert!(recv_callback_timeout(&commit_result, "next writer commit reply").is_ok());
    finish_transaction(ctx, db_name, txn);
}

/// Sends `count` puts into `a` in the readwrite transaction `txn`, which keep the engine busy
/// with a batch of them.
fn send_puts(ctx: &IndexedDbTestContext, db_name: &str, txn: IndexedDBTxnId, count: u64) {
    for key in 0..count {
        let _ = put_item_request(
            ctx,
            db_name,
            A,
            txn,
            key,
            Some(IndexedDBKeyType::Number(key as f64)),
            vec![0; 1024],
            true,
        );
    }
}

/// An abort that arrives while a batch of the transaction runs rolls the transaction back
/// before the next writer runs, also when the transaction finishes right after the abort, as it
/// does when its global goes away.
fn test_indexeddb_abort_while_a_batch_runs_frees_the_next_writer() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-abort-running-batch";
    let connection_id = open_database_with_two_stores(&ctx, db_name);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    send_puts(&ctx, db_name, txn, 2000);
    abort_transaction(&ctx, db_name, txn);
    finish_transaction(&ctx, db_name, txn);

    put_and_commit(&ctx, db_name, connection_id, B);
    assert_eq!(
        get_item_in_new_readonly_transaction(
            &ctx,
            db_name,
            connection_id,
            A,
            IndexedDBKeyType::Number(0.0)
        ),
        None
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// A transaction that finishes without a commit or an abort, like one of a global that went
/// away, is rolled back and holds back no other writer.
fn test_indexeddb_finished_transaction_without_commit_is_rolled_back() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-finished-without-commit";
    let connection_id = open_database_with_two_stores(&ctx, db_name);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    let put = put_item(
        &ctx,
        db_name,
        A,
        txn,
        IndexedDBKeyType::Number(0.0),
        b"dead".to_vec(),
    );
    recv_callback_timeout(&put, "dead writer put reply").unwrap();
    finish_transaction(&ctx, db_name, txn);

    put_and_commit(&ctx, db_name, connection_id, B);
    assert_eq!(
        get_item_in_new_readonly_transaction(
            &ctx,
            db_name,
            connection_id,
            A,
            IndexedDBKeyType::Number(0.0)
        ),
        None
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// A connection that closes before it reports its versionchange event, as one whose global went
/// away does, holds back no upgrade; when its closing leaves no event to wait for while another
/// connection stays open, the upgrade is blocked.
fn test_indexeddb_upgrade_waits_for_no_versionchange_of_a_closed_connection() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-upgrade-closed-connection";
    let gone = open_database_with_two_stores(&ctx, db_name);
    let stays = match recv_callback_timeout(&open_database(&ctx, db_name, None), "second open") {
        ConnectionMsg::Connection { id, .. } => id,
        other => panic!("unexpected second open message: {other:?}"),
    };

    let upgrade = open_database(&ctx, db_name, Some(2));
    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::NotifyEndOfVersionChange {
            name: db_name.into(),
            id: stays,
            old_version: 1,
            origin: ctx.origin.clone(),
        }),
    )
    .unwrap();
    close_database(&ctx, gone, db_name);
    match recv_callback_timeout(&upgrade, "blocked event") {
        ConnectionMsg::Blocked {
            version,
            old_version,
            ..
        } => assert_eq!((version, old_version), (2, 1)),
        other => panic!("unexpected upgrade message: {other:?}"),
    }

    close_database(&ctx, stays, db_name);
    let transaction = match recv_callback_timeout(&upgrade, "upgrade") {
        ConnectionMsg::Upgrade { transaction, .. } => transaction,
        other => panic!("unexpected upgrade message: {other:?}"),
    };
    commit_upgrade_transaction(&ctx, db_name, transaction);
    let upgraded = match recv_callback_timeout(&upgrade, "upgraded connection") {
        ConnectionMsg::Connection { id, version, .. } => {
            assert_eq!(version, 2);
            id
        },
        other => panic!("unexpected upgrade message: {other:?}"),
    };

    close_database(&ctx, upgraded, db_name);
    ctx.shutdown();
}

/// The results of a connection's requests reach its result channel in request order, also a
/// large result before the small results of later requests, and each commit result after the
/// results of its transaction.
fn test_indexeddb_results_reach_the_connection_in_request_order() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-result-order";
    let connection_id = open_database_with_two_stores(&ctx, db_name);

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A, B],
    );
    let puts: Vec<_> = (0..8)
        .map(|key| {
            put_item_request(
                &ctx,
                db_name,
                A,
                txn,
                key,
                Some(IndexedDBKeyType::Number(key as f64)),
                vec![key as u8; 512 * 1024],
                true,
            )
        })
        .collect();
    let small_put = put_item_request(
        &ctx,
        db_name,
        B,
        txn,
        8,
        Some(IndexedDBKeyType::Number(1.0)),
        b"small".to_vec(),
        true,
    );
    let commit = commit_transaction(&ctx, db_name, txn);
    for put in puts.iter().chain([&small_put]) {
        recv_callback_timeout(put, "put reply").unwrap();
    }
    recv_callback_timeout(&commit, "commit reply").unwrap();
    finish_transaction(&ctx, db_name, txn);

    let reads = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![A, B],
    );
    let large = send_request::<Vec<IndexedDBValue>>(
        &ctx,
        db_name,
        A,
        reads,
        0,
        IndexedDBTxnMode::Readonly,
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllItems {
            index: None,
            key_range: IndexedDBKeyRange::default(),
            count: None,
        }),
    );
    let small = get_item(&ctx, db_name, B, reads, IndexedDBKeyType::Number(1.0));
    let keys = get_all_keys_request(&ctx, db_name, B, reads, IndexedDBKeyRange::default(), 2);
    let commit = commit_transaction(&ctx, db_name, reads);
    recv_callback_timeout(&commit, "readonly commit reply").unwrap();
    let values = recv_callback_timeout(&large, "large getAll reply").unwrap();
    assert_eq!(values.len(), 8);
    assert!(values.iter().all(|value| value.value.len() == 512 * 1024));
    assert!(recv_callback_timeout(&small, "small get reply").is_ok());
    assert!(recv_callback_timeout(&keys, "keys reply").is_ok());
    finish_transaction(&ctx, db_name, reads);

    let mut log = ROUTES.lock().unwrap().log.remove(&connection_id).unwrap();
    log.retain(|(serial, _)| *serial != 0);
    let mut expected: Vec<_> = (0..9).map(|request| (txn.serial, Some(request))).collect();
    expected.push((txn.serial, None));
    expected.extend([
        (reads.serial, Some(0)),
        (reads.serial, Some(1)),
        (reads.serial, Some(2)),
        (reads.serial, None),
    ]);
    assert_eq!(log, expected);

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// A request and the commit of a transaction that the backend doesn't know, such as one created
/// while the database didn't exist, fail on its connection's result channel instead of waiting
/// forever.
fn test_indexeddb_requests_and_commit_of_an_unknown_transaction_fail() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-unknown-transaction";
    let connection_id = open_database_with_two_stores(&ctx, db_name);

    let unknown = IndexedDBTxnId {
        connection: connection_id,
        serial: NEXT_SERIAL.fetch_add(1, Ordering::Relaxed),
    };
    let get = get_item(&ctx, db_name, A, unknown, IndexedDBKeyType::Number(1.0));
    assert_eq!(
        recv_callback_timeout(&get, "unknown transaction reply"),
        Err(BackendError::DbNotFound)
    );
    let commit = commit_transaction(&ctx, db_name, unknown);
    assert_eq!(
        recv_callback_timeout(&commit, "unknown transaction commit reply"),
        Err(BackendError::DbNotFound)
    );

    close_database(&ctx, connection_id, db_name);
    ctx.shutdown();
}

/// Sends `()` on its sender when dropped.
struct DropSignal(mpsc::Sender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// The backend keeps a connection's result channel while the connection is open or one of its
/// transactions hasn't finished, and drops it then, which removes its route in multiprocess
/// mode and releases what script's callback holds.
fn test_indexeddb_connection_channel_goes_away_after_close_and_its_transactions() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-channel-teardown";
    open_database_with_two_stores(&ctx, db_name);

    let (dropped_sender, dropped) = mpsc::channel();
    let receiver = open_database_with(&ctx, db_name, None, DropSignal(dropped_sender));
    let connection_id = match recv_callback_timeout(&receiver, "second open") {
        ConnectionMsg::Connection { id, .. } => id,
        other => panic!("unexpected open message: {other:?}"),
    };

    let txn = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    close_database(&ctx, connection_id, db_name);
    let put = put_item(
        &ctx,
        db_name,
        A,
        txn,
        IndexedDBKeyType::Number(1.0),
        b"after close".to_vec(),
    );
    assert!(
        recv_callback_timeout(&put, "put after close").is_ok(),
        "A transaction of a closed connection still gets its results"
    );
    let commit = commit_transaction(&ctx, db_name, txn);
    recv_callback_timeout(&commit, "commit after close").unwrap();
    assert!(
        dropped.recv_timeout(Duration::from_millis(200)).is_err(),
        "The channel stays while a transaction of the connection is live"
    );

    finish_transaction(&ctx, db_name, txn);
    dropped
        .recv_timeout(TIMEOUT)
        .expect("The backend drops the channel of a closed connection without transactions");
    ctx.shutdown();
}

/// A transaction created on a connection that the backend dropped, as script could do with the
/// connection of an aborted upgrade, isn't registered: it holds back no other transaction, and
/// its request and commit fail with no one to tell. Registered without a result channel, its
/// readwrite SQL transaction started and was never finished, which blocked every later writer
/// and every reader of its scope.
fn test_indexeddb_transaction_of_a_dropped_connection_holds_back_no_one() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-dropped-connection";
    let first = open_database_with_two_stores(&ctx, db_name);
    close_database(&ctx, first, db_name);

    let upgrade = open_database(&ctx, db_name, Some(2));
    let (dropped, transaction) = match recv_callback_timeout(&upgrade, "upgrade") {
        ConnectionMsg::Upgrade {
            id, transaction, ..
        } => (id, transaction),
        other => panic!("unexpected upgrade message: {other:?}"),
    };
    abort_upgrade_transaction(&ctx, db_name, transaction);
    assert!(matches!(
        recv_callback_timeout(&upgrade, "aborted open reply"),
        ConnectionMsg::AbortError { .. }
    ));

    let stale = create_transaction(&ctx, db_name, dropped, IndexedDBTxnMode::Readwrite, vec![A]);
    let stale_put = put_item(
        &ctx,
        db_name,
        A,
        stale,
        IndexedDBKeyType::Number(2.0),
        b"stale".to_vec(),
    );
    let stale_commit = commit_transaction(&ctx, db_name, stale);

    let fresh = match recv_callback_timeout(&open_database(&ctx, db_name, None), "fresh open") {
        ConnectionMsg::Connection { id, version, .. } => {
            assert_eq!(version, 1);
            id
        },
        other => panic!("unexpected open message: {other:?}"),
    };
    put_and_commit(&ctx, db_name, fresh, A);
    assert_eq!(
        get_item_in_new_readonly_transaction(
            &ctx,
            db_name,
            fresh,
            A,
            IndexedDBKeyType::Number(2.0)
        ),
        None
    );
    assert!(stale_put.recv_timeout(Duration::from_millis(200)).is_err());
    assert!(stale_commit.try_recv().is_err());

    close_database(&ctx, fresh, db_name);
    ctx.shutdown();
}

/// Script sends a request only if its transaction's mode and scope allow it, and a schema
/// operation only in an upgrade transaction. The backend checks too: a request that breaks
/// these rules fails and aborts its transaction, whose later requests and commit fail.
fn test_indexeddb_requests_outside_the_mode_or_scope_fail_the_transaction() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-mode-and-scope";
    let connection_id = open_database_with_two_stores(&ctx, db_name);

    let reads = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readonly,
        vec![A],
    );
    let put = put_item(
        &ctx,
        db_name,
        A,
        reads,
        IndexedDBKeyType::Number(1.0),
        b"readonly".to_vec(),
    );
    assert!(matches!(
        recv_callback_timeout(&put, "put in a readonly transaction"),
        Err(BackendError::DbErr(_))
    ));
    let get = get_item(&ctx, db_name, A, reads, IndexedDBKeyType::Number(1.0));
    assert_eq!(
        recv_callback_timeout(&get, "get in the aborted transaction"),
        Err(BackendError::Abort)
    );
    let commit = commit_transaction(&ctx, db_name, reads);
    assert_eq!(
        recv_callback_timeout(&commit, "commit of the aborted transaction"),
        Err(BackendError::Abort)
    );
    finish_transaction(&ctx, db_name, reads);

    let writes = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    let put = put_item(
        &ctx,
        db_name,
        B,
        writes,
        IndexedDBKeyType::Number(1.0),
        b"out of scope".to_vec(),
    );
    assert!(matches!(
        recv_callback_timeout(&put, "put outside the scope"),
        Err(BackendError::DbErr(_))
    ));
    finish_transaction(&ctx, db_name, writes);

    let writes = create_transaction(
        &ctx,
        db_name,
        connection_id,
        IndexedDBTxnMode::Readwrite,
        vec![A],
    );
    let create = create_object_store(&ctx, db_name, B + 1, "c", writes, None, false);
    assert!(matches!(
        recv_callback_timeout(&create, "schema operation outside an upgrade"),
        BackendError::DbErr(_)
    ));
    finish_transaction(&ctx, db_name, writes);

    for store in [A, B] {
        assert_eq!(
            get_item_in_new_readonly_transaction(
                &ctx,
                db_name,
                connection_id,
                store,
                IndexedDBKeyType::Number(1.0)
            ),
            None
        );
    }
    put_and_commit(&ctx, db_name, connection_id, B);
    close_database(&ctx, connection_id, db_name);
    let (_, stores) = committed_metadata(&ctx, db_name);
    assert_eq!(names(&stores), vec!["a", "b"]);
    ctx.shutdown();
}

/// Waits until the result channel of `connection` has carried `count` open request messages
/// and versionchange events, and returns them.
fn wait_for_events(connection: Uuid, count: usize) -> Vec<String> {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let events = ROUTES
            .lock()
            .unwrap()
            .events
            .get(&connection)
            .cloned()
            .unwrap_or_default();
        if events.len() >= count {
            return events;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Only {events:?} arrived on the result channel"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Of two opens sent back to back, the first succeeds without an upgrade, and the second makes
/// the backend fire versionchange at the first's connection right after. Both go on the first
/// request's result channel, so the success, which creates the connection in script, comes
/// first; on two channels, the event could overtake it under multiprocess and be lost.
fn test_indexeddb_versionchange_follows_the_open_result_on_one_channel() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-versionchange-order";
    let first = open_database_with_two_stores(&ctx, db_name);
    close_database(&ctx, first, db_name);

    let opened = open_database(&ctx, db_name, None);
    let upgrade = open_database(&ctx, db_name, Some(2));
    let connection_id = match recv_callback_timeout(&opened, "first open") {
        ConnectionMsg::Connection { id, .. } => id,
        other => panic!("unexpected open message: {other:?}"),
    };
    let events = wait_for_events(connection_id, 2);
    assert!(events[0].starts_with("Connection {"), "{events:?}");
    assert!(
        events[1].starts_with("VersionChange { old_version: 1, new_version: 2 }"),
        "{events:?}"
    );

    close_database(&ctx, connection_id, db_name);
    let transaction = match recv_callback_timeout(&upgrade, "upgrade") {
        ConnectionMsg::Upgrade { transaction, .. } => transaction,
        other => panic!("unexpected upgrade message: {other:?}"),
    };
    commit_upgrade_transaction(&ctx, db_name, transaction);
    let upgraded = match recv_callback_timeout(&upgrade, "upgraded connection") {
        ConnectionMsg::Connection { id, version, .. } => {
            assert_eq!(version, 2);
            id
        },
        other => panic!("unexpected upgrade message: {other:?}"),
    };
    close_database(&ctx, upgraded, db_name);
    ctx.shutdown();
}

/// An index that an upgrade transaction creates on a store with records is populated by script's
/// steps, and the transaction's requests that came after its creation wait until it is done and
/// see the whole index.
fn test_indexeddb_create_index_population_runs_before_later_requests() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-create-index-population";
    let receiver = open_database(&ctx, db_name, Some(1));
    let ConnectionMsg::Upgrade { transaction, .. } =
        recv_callback_timeout(&receiver, "upgrade message")
    else {
        panic!("expected an upgrade");
    };
    let (population_sender, populations) = mpsc::channel();
    ROUTES.lock().unwrap().populations.insert(
        (transaction.connection, transaction.serial),
        population_sender,
    );
    let failures = create_object_store(&ctx, db_name, STORE, "store", transaction, None, false);
    let before: Vec<_> = (1..=3)
        .map(|key| {
            indexed_put_request(
                &ctx,
                db_name,
                STORE,
                transaction,
                key,
                Some(IndexedDBKeyType::Number(key as f64)),
                vec![key as u8],
                false,
                vec![],
            )
        })
        .collect();
    let index = 1;
    send_schema_operation(
        &ctx,
        db_name,
        STORE,
        transaction,
        AsyncSchemaOperation::CreateIndex {
            index_id: index,
            name: "unique".into(),
            key_path: KeyPath::String("k".to_owned()),
            unique: true,
            multi_entry: false,
            populate: true,
        },
    );
    let string_key = |key: &str| IndexedDBIndexKeys {
        index_id: index,
        keys: vec![IndexedDBIndexKey::Key(IndexedDBKeyType::String(key.into()))],
    };
    let after = [(4, "d"), (5, "a")].map(|(key, index_key)| {
        indexed_put_request(
            &ctx,
            db_name,
            STORE,
            transaction,
            key,
            Some(IndexedDBKeyType::Number(key as f64)),
            vec![key as u8],
            false,
            vec![string_key(index_key)],
        )
    });
    for (key, receiver) in (1..).zip(&before) {
        assert_eq!(
            recv_callback_timeout(receiver, "put before createIndex"),
            Ok(PutItemResult::Key(IndexedDBKeyType::Number(key as f64)))
        );
    }

    let (event_index, event) = populations.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(event_index, index);
    let IndexPopulationEvent::Records { records, last } = event else {
        panic!("expected the store's records: {event:?}");
    };
    assert!(last);
    assert_eq!(records.len(), 3);
    assert!(
        after[0].recv_timeout(Duration::from_millis(200)).is_err(),
        "the put after createIndex waits for the population"
    );
    let records = records
        .into_iter()
        .map(|record| IndexedDBRecordIndexKeys {
            keys: vec![IndexedDBIndexKey::Key(IndexedDBKeyType::String(
                ["a", "b", "c"][record.value[0] as usize - 1].into(),
            ))],
            primary_key: record.primary_key,
        })
        .collect();
    send_schema_operation(
        &ctx,
        db_name,
        STORE,
        transaction,
        AsyncSchemaOperation::PopulateIndex {
            index_id: index,
            step: IndexPopulationStep::SetIndexKeys {
                records,
                done: true,
            },
        },
    );
    assert!(matches!(
        populations.recv_timeout(TIMEOUT).unwrap(),
        (1, IndexPopulationEvent::Done(Ok(())))
    ));
    assert_eq!(
        recv_callback_timeout(&after[0], "put after createIndex"),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(4.0)))
    );
    assert_eq!(
        recv_callback_timeout(&after[1], "conflicting put after createIndex"),
        Ok(PutItemResult::UniqueIndexKeyExists(index))
    );
    assert!(failures.try_recv().is_err());

    commit_upgrade_transaction(&ctx, db_name, transaction);
    let ConnectionMsg::Connection {
        id, object_stores, ..
    } = recv_callback_timeout(&receiver, "connection message")
    else {
        panic!("expected the connection");
    };
    assert_eq!(object_stores[0].indexes.len(), 1);
    close_database(&ctx, id, db_name);
    ctx.shutdown();
}

/// When the global of an open request whose upgrade runs goes away, the open requests queued
/// behind it go on.
fn test_indexeddb_aborted_pending_upgrade_lets_the_next_open_run() {
    let ctx = IndexedDbTestContext::new();
    let db_name = "indexeddb-aborted-pending-upgrade";
    let first = open_database(&ctx, db_name, Some(1));
    let ConnectionMsg::Upgrade { id, .. } = recv_callback_timeout(&first, "first upgrade") else {
        panic!("expected an upgrade");
    };
    let second = open_database(&ctx, db_name, None);
    assert!(
        second.recv_timeout(Duration::from_millis(200)).is_err(),
        "the second open waits for the first"
    );

    GenericSend::send(
        &ctx.private_threads,
        IndexedDBThreadMsg::Sync(SyncOperation::AbortPendingUpgrades {
            pending_upgrades: HashMap::from([(db_name.into(), HashSet::from([id]))]),
            origin: ctx.origin.clone(),
        }),
    )
    .unwrap();
    let ConnectionMsg::Upgrade {
        version,
        old_version,
        transaction,
        ..
    } = recv_callback_timeout(&second, "second upgrade")
    else {
        panic!("expected an upgrade of the database that the first upgrade didn't create");
    };
    assert_eq!((version, old_version), (1, 0));
    commit_upgrade_transaction(&ctx, db_name, transaction);
    let ConnectionMsg::Connection { id, .. } = recv_callback_timeout(&second, "connection") else {
        panic!("expected the connection");
    };
    close_database(&ctx, id, db_name);
    ctx.shutdown();
}
