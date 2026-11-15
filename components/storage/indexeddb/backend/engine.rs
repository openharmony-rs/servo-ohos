/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};

use log::{error, warn};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use rustc_hash::FxHashMap;
use servo_base::threadpool::ThreadPool;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, IndexedDBObjectStore, IndexedDBObjectStoreId, IndexedDBTxnId,
    IndexedDBTxnMode, RequestOutput, TransactionEvent,
};

use super::executor::{SqlError, SqlExecutor, TransactionBehavior};
use super::queries::{self, ObjectStore};
use super::schema;
use crate::indexeddb::IndexedDBDescription;
use crate::indexeddb::engines::{KvsEngine, KvsOperation, KvsTransaction, TransactionResults};

/// The IndexedDB engine on a [`SqlExecutor`]: an actor that owns the database's only connection
/// and runs everything on it one job at a time, on the thread pool, in the order the manager
/// hands the work over.
///
/// A readwrite or versionchange transaction is one SQL transaction: its first request begins it
/// with `BEGIN IMMEDIATE`, and it stays open across batches until the transaction commits or
/// aborts. Readonly requests run without one, inside the open SQL transaction if there is one;
/// the manager never runs a readonly transaction alongside a writer with an overlapping scope, and
/// starts at most one writer at a time.
///
/// The manager's metadata reads answer from the metadata as of the last commit, without waiting
/// for the work handed over before.
pub(crate) struct SqlEngine<X: SqlExecutor> {
    actor: Arc<Actor<X>>,
    metadata: Arc<Mutex<Metadata>>,
    created_db_path: bool,
}

impl<X: SqlExecutor> SqlEngine<X> {
    pub(crate) fn new(
        path: PathBuf,
        created: bool,
        db_info: &IndexedDBDescription,
        pool: Arc<ThreadPool>,
    ) -> Result<Self, SqlError> {
        let (mut executor, created_database) = schema::open_database::<X>(&path, db_info)?;
        let metadata = Arc::new(Mutex::new(Metadata::load(&mut executor)?));
        Ok(Self {
            actor: Arc::new(Actor {
                pool,
                jobs: Mutex::new(Jobs {
                    waiting: VecDeque::new(),
                    running: false,
                }),
                database: Mutex::new(Some(Database {
                    executor,
                    writer: None,
                    upgrade: None,
                    versionchange: None,
                    metadata: metadata.clone(),
                    failed: FxHashMap::default(),
                })),
            }),
            metadata,
            created_db_path: created || created_database,
        })
    }

    /// Returns whether the physical db was created as part of `new`, which includes recreating
    /// a database of another schema version.
    pub(crate) fn created_db_path(&self) -> bool {
        self.created_db_path
    }

    /// The current number of the key generator of the store `store`, read on the connection
    /// after the work handed over before, inside the open SQL transaction if there is one.
    #[cfg(test)]
    pub(crate) fn key_generator_current_number(
        &self,
        store: IndexedDBObjectStoreId,
    ) -> Option<i64> {
        self.actor
            .call(move |database| {
                queries::key_generator_current_number(&mut database.executor, store)
            })
            .and_then(|result| result)
            .unwrap_or_else(|error| {
                warn!("IndexedDB key generator lookup failed: {error:?}");
                None
            })
    }

    /// The connection, for tests to use while no work is handed over.
    #[cfg(test)]
    pub(super) fn connection(&self) -> TestConnection<'_, X> {
        TestConnection(lock(&self.actor.database))
    }

    /// Reloads the committed metadata, after a test changed the database through
    /// [`Self::connection`].
    #[cfg(test)]
    pub(super) fn reload_metadata(&self) {
        *lock(&self.metadata) = Metadata::load(&mut *self.connection()).unwrap();
    }

    /// Runs `job` on the connection after the work handed over before, without waiting for it.
    #[cfg(test)]
    pub(super) fn spawn_on_connection(&self, job: impl FnOnce(&mut X) + Send + 'static) {
        self.actor
            .spawn(move |database| job(&mut database.executor));
    }
}

impl<X: SqlExecutor> Drop for SqlEngine<X> {
    /// Closes the connection once the work handed over has run, so that the database files can
    /// be deleted right after.
    fn drop(&mut self) {
        self.actor.close();
    }
}

/// The connection of a [`SqlEngine`], borrowed by a test.
#[cfg(test)]
pub(super) struct TestConnection<'a, X>(MutexGuard<'a, Option<Database<X>>>);

#[cfg(test)]
impl<X> std::ops::Deref for TestConnection<'_, X> {
    type Target = X;

    fn deref(&self) -> &X {
        &self.0.as_ref().expect("The database is open").executor
    }
}

#[cfg(test)]
impl<X> std::ops::DerefMut for TestConnection<'_, X> {
    fn deref_mut(&mut self) -> &mut X {
        &mut self.0.as_mut().expect("The database is open").executor
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

type Work<X> = Box<dyn FnOnce(&mut Database<X>) + Send>;

/// Work handed over to an [`Actor`].
enum Job<X> {
    Run(Work<X>),
    /// Closes the connection and reports when it is closed. Later jobs are dropped unrun.
    Close(mpsc::Sender<()>),
}

struct Jobs<X> {
    waiting: VecDeque<Job<X>>,
    /// Whether a thread is running jobs. No job waits while none is.
    running: bool,
}

/// Runs the jobs of one database one at a time, in the order they were handed over. A pool task
/// runs them while there are any, so an idle database holds no thread.
struct Actor<X> {
    pool: Arc<ThreadPool>,
    jobs: Mutex<Jobs<X>>,
    /// `None` once closed.
    database: Mutex<Option<Database<X>>>,
}

impl<X: SqlExecutor> Actor<X> {
    /// Hands `job` over. It runs on the pool after every job handed over before it, or, if
    /// `inline` is set and there is no such job, right away on this thread.
    fn submit(self: &Arc<Self>, job: Job<X>, inline: bool) {
        {
            let mut jobs = lock(&self.jobs);
            if jobs.running || !inline {
                jobs.waiting.push_back(job);
                if !jobs.running {
                    jobs.running = true;
                    self.start();
                }
                return;
            }
            jobs.running = true;
        }
        self.run(job);
        let mut jobs = lock(&self.jobs);
        if jobs.waiting.is_empty() {
            jobs.running = false;
        } else {
            self.start();
        }
    }

    fn start(self: &Arc<Self>) {
        let actor = self.clone();
        self.pool.spawn(move || {
            loop {
                let job = {
                    let mut jobs = lock(&actor.jobs);
                    let Some(job) = jobs.waiting.pop_front() else {
                        jobs.running = false;
                        return;
                    };
                    job
                };
                actor.run(job);
            }
        });
    }

    fn run(&self, job: Job<X>) {
        let mut database = lock(&self.database);
        match job {
            Job::Run(job) => {
                if let Some(database) = database.as_mut() {
                    job(database);
                    database.notice_ended_transaction(ended_early);
                }
            },
            Job::Close(closed) => {
                database.take();
                let _ = closed.send(());
            },
        }
    }

    fn spawn(self: &Arc<Self>, job: impl FnOnce(&mut Database<X>) + Send + 'static) {
        self.submit(Job::Run(Box::new(job)), false);
    }

    /// Runs `job` after the work handed over before, and waits for its result. Runs it on this
    /// thread if the actor is idle.
    #[cfg(test)]
    fn call<T: Send + 'static>(
        self: &Arc<Self>,
        job: impl FnOnce(&mut Database<X>) -> T + Send + 'static,
    ) -> Result<T, SqlError> {
        let (sender, receiver) = mpsc::channel();
        let job = move |database: &mut Database<X>| {
            let _ = sender.send(job(database));
        };
        self.submit(Job::Run(Box::new(job)), true);
        receiver
            .recv()
            .map_err(|_| SqlError::TransactionState("the database is closed"))
    }

    fn close(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::channel();
        self.submit(Job::Close(sender), true);
        let _ = receiver.recv();
    }

    /// The connection's memory, unless a job is using it.
    fn heap_size(&self) -> usize {
        self.database
            .try_lock()
            .ok()
            .and_then(|database| {
                database
                    .as_ref()
                    .map(|database| database.executor.heap_size())
            })
            .unwrap_or(0)
    }
}

/// The connection of a database and the state of the transactions on it, which only the jobs of
/// its actor touch.
pub(super) struct Database<X> {
    executor: X,
    /// The readwrite or versionchange transaction whose SQL transaction is open.
    writer: Option<IndexedDBTxnId>,
    /// The upgrade transaction that hasn't begun yet, and the version it sets when it begins.
    upgrade: Option<(IndexedDBTxnId, u64)>,
    /// The upgrade transaction, whose commit changes the metadata.
    versionchange: Option<IndexedDBTxnId>,
    /// The metadata as of the last commit, which the [`SqlEngine`] reads.
    metadata: Arc<Mutex<Metadata>>,
    /// Transactions whose changes were rolled back by a failure, with the error that their later
    /// requests and their commit report, until the transaction aborts.
    failed: FxHashMap<IndexedDBTxnId, BackendError>,
}

impl<X: SqlExecutor> Database<X> {
    /// Runs the requests of a batch in order and sends each result to `results` as it has run.
    fn run_batch(
        &mut self,
        transaction: IndexedDBTxnId,
        batch: KvsTransaction,
        results: &TransactionResults,
    ) {
        let writes = batch.mode != IndexedDBTxnMode::Readonly;
        for request in batch.requests {
            if let Some(error) = self.failed.get(&transaction) {
                results.send(request.failure_event(error.clone()));
                continue;
            }
            if writes && let Err(error) = self.begin(transaction) {
                results.send(request.failure_event(error));
                continue;
            }
            let (event, failure) = run_request(&mut self.executor, request);
            if let Some(event) = event {
                results.send(event);
            }
            self.notice_ended_transaction(|| {
                failure
                    .as_ref()
                    .map_or_else(ended_early, |failure| failure.error().clone())
            });
            if let Some(Failure::Write(error)) = failure {
                self.fail(transaction, error);
            }
        }
    }

    /// Fails the writer if its SQL transaction has ended without the engine's commit or rollback.
    /// SQLite ends it by itself after some errors, and the executors end it after the errors that
    /// may have, also of a statement that isn't the writer's own, such as a readonly request that
    /// ran inside its transaction. Its writes are gone then, and later statements would run in
    /// autocommit.
    fn notice_ended_transaction(&mut self, error: impl FnOnce() -> BackendError) {
        if let Some(writer) = self.writer &&
            !self.executor.in_transaction()
        {
            self.writer = None;
            self.failed.insert(writer, error());
        }
    }

    /// Begins the SQL transaction of the readwrite or versionchange `transaction`, unless it is
    /// open already. An upgrade transaction sets the database's version when it begins. On
    /// failure, the transaction has failed.
    fn begin(&mut self, transaction: IndexedDBTxnId) -> Result<(), BackendError> {
        self.notice_ended_transaction(ended_early);
        if let Some(error) = self.failed.get(&transaction) {
            return Err(error.clone());
        }
        if self.writer == Some(transaction) {
            return Ok(());
        }
        if let Some(writer) = self.writer {
            error!("IndexedDB transaction {transaction} began while {writer} writes");
            let error = BackendError::DbErr("Another transaction is writing".to_owned());
            self.failed.insert(transaction, error.clone());
            return Err(error);
        }
        if let Err(error) = self.executor.begin(TransactionBehavior::Immediate) {
            let error = error.to_backend_error();
            self.failed.insert(transaction, error.clone());
            return Err(error);
        }
        self.writer = Some(transaction);
        if let Some((upgrade, version)) = self.upgrade &&
            upgrade == transaction
        {
            self.upgrade = None;
            if let Err(error) = queries::set_version(&mut self.executor, version) {
                let error = error.to_backend_error();
                self.fail(transaction, error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>
    fn commit(&mut self, transaction: IndexedDBTxnId) -> BackendResult<()> {
        let changes_metadata = self
            .versionchange
            .take_if(|versionchange| *versionchange == transaction)
            .is_some();
        if let Some(error) = self.failed.remove(&transaction) {
            return Err(error);
        }
        // An upgrade transaction without requests still sets the version.
        if self
            .upgrade
            .is_some_and(|(upgrade, _)| upgrade == transaction)
        {
            self.begin(transaction).inspect_err(|_| {
                self.failed.remove(&transaction);
            })?;
        }
        if self.writer != Some(transaction) {
            return Ok(());
        }
        let metadata = if changes_metadata {
            match Metadata::load(&mut self.executor) {
                Ok(metadata) => Some(metadata),
                Err(error) => {
                    self.roll_back(transaction);
                    return Err(error.to_backend_error());
                },
            }
        } else {
            None
        };
        self.writer = None;
        // Step 2.3. Attempt to write any outstanding changes made by transaction to the database.
        // Step 2.4. If an error occurs while writing the changes to the database, then run abort a
        // transaction with transaction and an appropriate type for the error, for example a
        // QuotaExceededError or an "UnknownError" DOMException, and terminate these steps.
        self.executor.commit().map_err(|error| {
            warn!("Failed to commit IndexedDB transaction {transaction}: {error:?}");
            error.to_backend_error()
        })?;
        if let Some(metadata) = metadata {
            *lock(&self.metadata) = metadata;
        }
        Ok(())
    }

    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>
    fn abort(&mut self, transaction: IndexedDBTxnId) {
        self.failed.remove(&transaction);
        if self
            .upgrade
            .is_some_and(|(upgrade, _)| upgrade == transaction)
        {
            self.upgrade = None;
        }
        if self.versionchange == Some(transaction) {
            self.versionchange = None;
        }
        // Step 2. All the changes made to the database by the transaction are reverted. For
        // upgrade transactions this includes changes to the set of object stores and indexes, as
        // well as the change to the version.
        self.roll_back(transaction);
    }

    /// Rolls back `transaction`, and fails its later requests and its commit with `error`.
    fn fail(&mut self, transaction: IndexedDBTxnId, error: BackendError) {
        self.roll_back(transaction);
        self.failed.insert(transaction, error);
    }

    fn roll_back(&mut self, transaction: IndexedDBTxnId) {
        if self.writer != Some(transaction) {
            return;
        }
        self.writer = None;
        if let Err(error) = self.executor.rollback() {
            warn!("Failed to roll back IndexedDB transaction {transaction}: {error:?}");
        }
    }
}

impl<X: SqlExecutor> MallocSizeOf for SqlEngine<X> {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.created_db_path.size_of(ops) + self.actor.heap_size()
    }
}

impl<X: SqlExecutor> KvsEngine for SqlEngine<X> {
    type Error = SqlError;

    fn close_store(&self, _store_name: &str) -> Result<(), Self::Error> {
        // TODO: do something
        Ok(())
    }

    fn process_transaction(
        &self,
        transaction: IndexedDBTxnId,
        batch: KvsTransaction,
        results: TransactionResults,
        on_complete: Box<dyn FnOnce() + Send + 'static>,
    ) {
        self.actor.spawn(move |database| {
            database.run_batch(transaction, batch, &results);
            on_complete();
        });
    }

    fn upgrade_version(&self, transaction: IndexedDBTxnId, version: u64) {
        self.actor.spawn(move |database| {
            database.upgrade = Some((transaction, version));
            database.versionchange = Some(transaction);
        });
    }

    fn commit(
        &self,
        transaction: IndexedDBTxnId,
        on_done: Box<dyn FnOnce(BackendResult<()>) + Send>,
    ) {
        self.actor
            .spawn(move |database| on_done(database.commit(transaction)));
    }

    fn rollback(&self, transaction: IndexedDBTxnId, on_done: Box<dyn FnOnce() + Send>) {
        self.actor.spawn(move |database| {
            database.abort(transaction);
            on_done();
        });
    }

    fn object_stores(&self) -> Vec<IndexedDBObjectStore> {
        lock(&self.metadata).stores.clone()
    }

    fn version(&self) -> Result<u64, Self::Error> {
        Ok(lock(&self.metadata).version)
    }
}

/// The version of a database and its object stores. Only upgrade transactions change them.
struct Metadata {
    version: u64,
    /// In the order the stores were created.
    stores: Vec<IndexedDBObjectStore>,
}

impl Metadata {
    /// Reads the metadata on `executor`, inside its open SQL transaction if there is one.
    fn load<X: SqlExecutor>(executor: &mut X) -> Result<Self, SqlError> {
        Ok(Self {
            version: queries::version(executor)?,
            stores: queries::object_stores(executor)?,
        })
    }
}

fn ended_early() -> BackendError {
    BackendError::DbErr("The storage transaction ended early".to_owned())
}

/// How a request failed in the database.
enum Failure {
    /// A read failed. The transaction goes on, unless the failure ended its SQL transaction.
    Read(BackendError),
    /// A write failed, possibly after some of its statements had applied, so the transaction
    /// fails.
    Write(BackendError),
}

impl Failure {
    fn error(&self) -> &BackendError {
        match self {
            Self::Read(error) | Self::Write(error) => error,
        }
    }
}

/// The result of a request, and how it failed if it failed in the database.
fn outcome<T>(
    result: Result<T, SqlError>,
    output: fn(T) -> RequestOutput,
    failure: fn(BackendError) -> Failure,
) -> (BackendResult<RequestOutput>, Option<Failure>) {
    match result {
        Ok(value) => (Ok(output(value)), None),
        Err(error) => {
            let error = error.to_backend_error();
            (Err(error.clone()), Some(failure(error)))
        },
    }
}

/// Runs a request, and returns what to report about it, if anything, and how it failed if it
/// failed in the database. A request that fails before reading or writing anything, such as one
/// for a missing object store, fails only itself; a schema operation that fails fails its
/// transaction, and is the only kind of operation that reports nothing when it succeeds.
fn run_request<X: SqlExecutor>(
    executor: &mut X,
    request: KvsOperation,
) -> (Option<TransactionEvent>, Option<Failure>) {
    let KvsOperation {
        store,
        request_id,
        operation,
    } = request;

    let operation = match operation {
        AsyncOperation::Schema(operation) => {
            let failure = run_schema(executor, store, operation);
            let event = failure
                .as_ref()
                .map(|failure| TransactionEvent::SchemaChangeFailed(failure.error().clone()));
            return (event, failure);
        },
        operation => operation,
    };
    let Some(request_id) = request_id else {
        error!("IndexedDB request without a request id");
        return (None, None);
    };

    let (result, failure) = match queries::object_store(executor, store) {
        Ok(Some(store)) => match operation {
            AsyncOperation::ReadOnly(operation) => run_read_only(executor, &store, operation),
            AsyncOperation::ReadWrite(operation) => run_read_write(executor, &store, operation),
            AsyncOperation::Schema(_) => unreachable!("Schema operations run above"),
        },
        Ok(None) => (Err(BackendError::StoreNotFound), None),
        Err(error) => {
            let error = error.to_backend_error();
            (Err(error.clone()), Some(Failure::Read(error)))
        },
    };
    (
        Some(TransactionEvent::Request { request_id, result }),
        failure,
    )
}

fn run_read_only<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    operation: AsyncReadOnlyOperation,
) -> (BackendResult<RequestOutput>, Option<Failure>) {
    match operation {
        AsyncReadOnlyOperation::GetKey { key_range } => outcome(
            queries::get_key(executor, store, &key_range),
            RequestOutput::Key,
            Failure::Read,
        ),
        AsyncReadOnlyOperation::GetItem { key_range } => outcome(
            queries::get_value(executor, store, &key_range),
            RequestOutput::Value,
            Failure::Read,
        ),
        AsyncReadOnlyOperation::GetAllKeys { key_range, count } => outcome(
            queries::get_all_keys(executor, store, &key_range, count),
            RequestOutput::Keys,
            Failure::Read,
        ),
        AsyncReadOnlyOperation::GetAllItems { key_range, count } => outcome(
            queries::get_all_values(executor, store, &key_range, count),
            RequestOutput::Values,
            Failure::Read,
        ),
        AsyncReadOnlyOperation::Count { key_range } => outcome(
            queries::count(executor, store, &key_range),
            RequestOutput::Count,
            Failure::Read,
        ),
        AsyncReadOnlyOperation::Iterate {
            key_range,
            count,
            reverse,
        } => outcome(
            queries::get_records(executor, store, &key_range, count, reverse),
            RequestOutput::Records,
            Failure::Read,
        ),
    }
}

fn run_read_write<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    operation: AsyncReadWriteOperation,
) -> (BackendResult<RequestOutput>, Option<Failure>) {
    match operation {
        AsyncReadWriteOperation::PutItem {
            key,
            value,
            should_overwrite,
            index_keys,
        } => {
            if key.is_none() && store.auto_increment == 0 {
                let error = BackendError::DbErr("Missing key for PutItem request".into());
                return (Err(error), None);
            }
            outcome(
                queries::put_record(executor, store, key, &value, should_overwrite, index_keys),
                RequestOutput::Put,
                Failure::Write,
            )
        },
        AsyncReadWriteOperation::RemoveItem { key_range } => outcome(
            queries::delete_records(executor, store, &key_range),
            |()| RequestOutput::Done,
            Failure::Write,
        ),
        AsyncReadWriteOperation::Clear => outcome(
            queries::clear(executor, store),
            |()| RequestOutput::Done,
            Failure::Write,
        ),
    }
}

/// Runs a schema operation, and returns how it failed if it failed.
fn run_schema<X: SqlExecutor>(
    executor: &mut X,
    store: IndexedDBObjectStoreId,
    operation: AsyncSchemaOperation,
) -> Option<Failure> {
    let result = match operation {
        AsyncSchemaOperation::CreateObjectStore {
            name,
            key_path,
            auto_increment,
        } => {
            queries::create_object_store(executor, store, &name, key_path.as_ref(), auto_increment)
        },
        AsyncSchemaOperation::DeleteObjectStore => queries::delete_object_store(executor, store),
        AsyncSchemaOperation::RenameObjectStore { new_name } => {
            queries::rename_object_store(executor, store, &new_name)
        },
        AsyncSchemaOperation::CreateIndex {
            index_id,
            name,
            key_path,
            unique,
            multi_entry,
        } => queries::create_index(
            executor,
            store,
            index_id,
            &name,
            &key_path,
            unique,
            multi_entry,
        ),
        AsyncSchemaOperation::RenameIndex { index_id, new_name } => {
            queries::rename_index(executor, store, index_id, &new_name)
        },
        AsyncSchemaOperation::DeleteIndex { index_id } => {
            queries::delete_index(executor, store, index_id)
        },
    };
    Some(Failure::Write(result.err()?.to_backend_error()))
}
