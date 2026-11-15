/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};

use log::warn;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use profile_traits::generic_callback::GenericCallback;
use serde::Serialize;
use serde::de::DeserializeOwned;
use servo_base::threadpool::ThreadPool;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, CreateObjectResult, IndexedDBIndex, IndexedDBKeyType,
    IndexedDBTxnMode, KeyPath,
};

use super::executor::{SqlError, SqlExecutor, TransactionBehavior, with_transaction};
use super::queries::{self, ObjectStore};
use super::schema;
use crate::indexeddb::IndexedDBDescription;
use crate::indexeddb::engines::{KvsEngine, KvsOperation, KvsTransaction};

/// The IndexedDB engine on a [`SqlExecutor`]: an actor that owns the database's only connection
/// and runs everything on it one job at a time, on the thread pool, in the order the manager
/// hands the work over: the manager thread's metadata calls, each in a storage transaction of its
/// own, and the batches of requests, each in one storage transaction.
pub(crate) struct SqlEngine<X: SqlExecutor> {
    actor: Arc<Actor<X>>,
    created_db_path: bool,
}

impl<X: SqlExecutor> SqlEngine<X> {
    pub(crate) fn new(
        path: PathBuf,
        created: bool,
        db_info: &IndexedDBDescription,
        pool: Arc<ThreadPool>,
    ) -> Result<Self, SqlError> {
        let (executor, created_database) = schema::open_database::<X>(&path, db_info)?;
        Ok(Self {
            actor: Arc::new(Actor {
                pool,
                jobs: Mutex::new(Jobs {
                    waiting: VecDeque::new(),
                    running: false,
                }),
                database: Mutex::new(Some(Database { executor })),
            }),
            created_db_path: created || created_database,
        })
    }

    /// Returns whether the physical db was created as part of `new`, which includes recreating
    /// a database of another schema version.
    pub(crate) fn created_db_path(&self) -> bool {
        self.created_db_path
    }

    /// Runs `f` on the connection in a transaction of its own, after the work handed over before,
    /// and waits for it.
    fn run<T: Send + 'static>(
        &self,
        behavior: TransactionBehavior,
        f: impl FnOnce(&mut X) -> Result<T, SqlError> + Send + 'static,
    ) -> Result<T, SqlError> {
        self.actor
            .call(move |database| with_transaction(&mut database.executor, behavior, f))?
    }

    /// The connection, for tests to use while no work is handed over.
    #[cfg(test)]
    pub(super) fn connection(&self) -> TestConnection<'_, X> {
        TestConnection(lock(&self.actor.database))
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
    /// thread if the actor is idle, which saves the manager thread a round trip for its
    /// metadata reads.
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

/// The connection of a database, which only the jobs of its actor touch.
pub(super) struct Database<X> {
    executor: X,
}

impl<X: SqlExecutor> MallocSizeOf for SqlEngine<X> {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.created_db_path.size_of(ops) + self.actor.heap_size()
    }
}

impl<X: SqlExecutor> KvsEngine for SqlEngine<X> {
    type Error = SqlError;

    fn create_store(
        &self,
        store_name: &str,
        key_path: Option<KeyPath>,
        auto_increment: bool,
    ) -> Result<CreateObjectResult, Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::create_object_store(connection, &store_name, key_path.as_ref(), auto_increment)
        })
    }

    fn delete_store(&self, store_name: &str) -> Result<(), Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::delete_object_store(connection, &store_name)
        })
    }

    fn close_store(&self, _store_name: &str) -> Result<(), Self::Error> {
        // TODO: do something
        Ok(())
    }

    fn process_transaction(
        &self,
        transaction: KvsTransaction,
        on_complete: Box<dyn FnOnce() + Send + 'static>,
    ) {
        self.actor.spawn(move |database| {
            run_batch(&mut database.executor, transaction);
            on_complete();
        });
    }

    fn key_generator_current_number(&self, store_name: &str) -> Option<i64> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Deferred, move |connection| {
            queries::key_generator_current_number(connection, &store_name)
        })
        .unwrap_or_else(|error| {
            warn!("IndexedDB key generator lookup failed: {error:?}");
            None
        })
    }

    fn set_key_generator_current_number(
        &self,
        store_name: &str,
        current_number: i64,
    ) -> Result<(), Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::set_key_generator_current_number(connection, &store_name, current_number)
        })
    }

    fn key_path(&self, store_name: &str) -> Option<KeyPath> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Deferred, move |connection| {
            queries::key_path(connection, &store_name)
        })
        .unwrap_or_else(|error| {
            warn!("IndexedDB key path lookup failed: {error:?}");
            None
        })
    }

    fn object_store_names(&self) -> Result<Vec<String>, Self::Error> {
        self.run(TransactionBehavior::Deferred, queries::object_store_names)
    }

    fn indexes(&self, store_name: &str) -> Result<Vec<IndexedDBIndex>, Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Deferred, move |connection| {
            queries::indexes(connection, &store_name)
        })
    }

    fn create_index(
        &self,
        store_name: &str,
        index_name: String,
        key_path: KeyPath,
        unique: bool,
        multi_entry: bool,
    ) -> Result<CreateObjectResult, Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::create_index(
                connection,
                &store_name,
                &index_name,
                &key_path,
                unique,
                multi_entry,
            )
        })
    }

    fn delete_index(&self, store_name: &str, index_name: String) -> Result<(), Self::Error> {
        let store_name = store_name.to_owned();
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::delete_index(connection, &store_name, &index_name)
        })
    }

    fn version(&self) -> Result<u64, Self::Error> {
        self.run(TransactionBehavior::Deferred, queries::version)
    }

    fn set_version(&self, version: u64) -> Result<(), Self::Error> {
        self.run(TransactionBehavior::Immediate, move |connection| {
            queries::set_version(connection, version)
        })
    }
}

fn db_error(error: SqlError) -> BackendError {
    BackendError::DbErr(format!("{error:?}"))
}

/// The reply to a request of a batch, sent when the batch's storage transaction has ended.
struct Reply {
    /// Sends the request's result after the transaction committed, or right away in a readonly
    /// batch, if there is one to send.
    on_commit: Option<Box<dyn FnOnce() + Send>>,
    /// Sends an error instead, after the transaction was rolled back or failed to commit.
    on_failure: Option<Box<dyn FnOnce(BackendError) + Send>>,
}

impl Reply {
    fn result<T>(callback: GenericCallback<BackendResult<T>>, result: BackendResult<T>) -> Self
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let failure_callback = callback.clone();
        Self {
            on_commit: Some(Box::new(move || {
                let _ = callback.send(result);
            })),
            on_failure: Some(Box::new(move |error| {
                let _ = failure_callback.send(Err(error));
            })),
        }
    }

    /// A schema operation's callback only ever carries an error, so `error` of `None` means the
    /// operation succeeded and nothing is sent unless the transaction fails.
    fn schema(callback: GenericCallback<BackendError>, error: Option<BackendError>) -> Self {
        let failure_callback = callback.clone();
        Self {
            on_commit: error.map(|error| -> Box<dyn FnOnce() + Send> {
                Box::new(move || {
                    let _ = callback.send(error);
                })
            }),
            on_failure: Some(Box::new(move |error| {
                let _ = failure_callback.send(error);
            })),
        }
    }

    /// The reply to an operation that failed, or never ran, with `error`.
    fn error(operation: AsyncOperation, error: BackendError) -> Self {
        match operation {
            // Mirrors `AsyncSchemaOperation::notify_error`: only the two object store operations
            // report back.
            AsyncOperation::Schema(
                AsyncSchemaOperation::CreateIndex { .. } |
                AsyncSchemaOperation::RenameIndex { .. } |
                AsyncSchemaOperation::DeleteIndex { .. },
            ) => Self {
                on_commit: None,
                on_failure: None,
            },
            AsyncOperation::Schema(
                AsyncSchemaOperation::CreateObjectStore { callback, .. } |
                AsyncSchemaOperation::DeleteObjectStore { callback },
            ) => Self::schema(callback, Some(error)),
            AsyncOperation::ReadOnly(operation) => match operation {
                AsyncReadOnlyOperation::GetKey { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadOnlyOperation::GetItem { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadOnlyOperation::GetAllKeys { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadOnlyOperation::GetAllItems { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadOnlyOperation::Count { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadOnlyOperation::Iterate { callback, .. } => {
                    Self::result(callback, Err(error))
                },
            },
            AsyncOperation::ReadWrite(operation) => match operation {
                AsyncReadWriteOperation::PutItem { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadWriteOperation::RemoveItem { callback, .. } => {
                    Self::result(callback, Err(error))
                },
                AsyncReadWriteOperation::Clear(callback) => Self::result(callback, Err(error)),
            },
        }
    }

    fn commit(self) {
        if let Some(on_commit) = self.on_commit {
            on_commit();
        }
    }

    fn fail(self, error: BackendError) {
        if let Some(on_failure) = self.on_failure {
            on_failure(error);
        }
    }
}

/// The reply to a request, and the error that aborts the batch, if the request failed in a way
/// that may have left some of its statements applied.
type Outcome = (Reply, Option<BackendError>);

/// Runs one batch of requests in one storage transaction. The replies of a readwrite or
/// versionchange batch are held back until the transaction ends, so no success is reported for
/// a write that never became durable; a readonly batch has nothing to make durable, so each of
/// its replies goes out as soon as its request has run. A request that fails while writing
/// aborts the batch: the transaction is rolled back and every request of the batch reports the
/// error. A request that fails before writing anything, such as one for a missing object store,
/// only fails itself.
fn run_batch<X: SqlExecutor>(connection: &mut X, transaction: KvsTransaction) {
    let readonly = transaction.mode == IndexedDBTxnMode::Readonly;
    let behavior = if readonly {
        TransactionBehavior::Deferred
    } else {
        TransactionBehavior::Immediate
    };
    if let Err(error) = connection.begin(behavior) {
        let error = db_error(error);
        for request in transaction.requests {
            request.operation.notify_error(error.clone());
        }
        return;
    }

    let mut replies = Vec::with_capacity(transaction.requests.len());
    let mut batch_error = None;
    let mut requests = transaction.requests.into_iter();
    for request in requests.by_ref() {
        let (reply, error) = run_request(connection, request);
        if readonly {
            reply.commit();
        } else {
            replies.push(reply);
        }
        // SQLite ends a transaction by itself after some errors; the rest of the batch must not
        // run outside of it.
        batch_error = error.or_else(|| {
            (!connection.in_transaction())
                .then(|| BackendError::DbErr("The storage transaction ended early".to_owned()))
        });
        if batch_error.is_some() {
            break;
        }
    }

    if let Some(error) = batch_error {
        if let Err(rollback_error) = connection.rollback() {
            warn!("Failed to roll back IndexedDB transaction: {rollback_error:?}");
        }
        replies.extend(requests.map(|request| Reply::error(request.operation, error.clone())));
        for reply in replies {
            reply.fail(error.clone());
        }
        return;
    }

    match connection.commit() {
        Ok(()) => {
            for reply in replies {
                reply.commit();
            }
        },
        Err(error) => {
            warn!("Failed to commit IndexedDB transaction: {error:?}");
            let error = db_error(error);
            for reply in replies {
                reply.fail(error.clone());
            }
        },
    }
}

fn run_request<X: SqlExecutor>(connection: &mut X, request: KvsOperation) -> Outcome {
    let KvsOperation {
        store_name,
        operation,
    } = request;

    // Runs before the store lookup below, because the store it creates does not exist yet.
    if let AsyncOperation::Schema(AsyncSchemaOperation::CreateObjectStore {
        callback,
        key_path,
        auto_increment,
    }) = operation
    {
        let error = queries::create_object_store(
            connection,
            &store_name,
            key_path.as_ref(),
            auto_increment,
        )
        .err()
        .map(db_error);
        return (Reply::schema(callback, error.clone()), error);
    }

    let store = match queries::object_store(connection, &store_name) {
        Ok(Some(store)) => store,
        Ok(None) => return (Reply::error(operation, BackendError::StoreNotFound), None),
        Err(error) => return (Reply::error(operation, db_error(error)), None),
    };

    match operation {
        AsyncOperation::ReadOnly(operation) => (run_read_only(connection, &store, operation), None),
        AsyncOperation::ReadWrite(operation) => run_read_write(connection, &store, operation),
        AsyncOperation::Schema(operation) => run_schema(connection, &store_name, operation),
    }
}

fn run_read_only<X: SqlExecutor>(
    connection: &mut X,
    store: &ObjectStore,
    operation: AsyncReadOnlyOperation,
) -> Reply {
    match operation {
        AsyncReadOnlyOperation::GetKey {
            callback,
            key_range,
        } => Reply::result(
            callback,
            queries::get_key(connection, store, &key_range).map_err(db_error),
        ),
        AsyncReadOnlyOperation::GetItem {
            callback,
            key_range,
        } => Reply::result(
            callback,
            queries::get_value(connection, store, &key_range).map_err(db_error),
        ),
        AsyncReadOnlyOperation::GetAllKeys {
            callback,
            key_range,
            count,
        } => Reply::result(
            callback,
            queries::get_all_keys(connection, store, &key_range, count).map_err(db_error),
        ),
        AsyncReadOnlyOperation::GetAllItems {
            callback,
            key_range,
            count,
        } => Reply::result(
            callback,
            queries::get_all_values(connection, store, &key_range, count).map_err(db_error),
        ),
        AsyncReadOnlyOperation::Count {
            callback,
            key_range,
        } => Reply::result(
            callback,
            queries::count(connection, store, &key_range).map_err(db_error),
        ),
        AsyncReadOnlyOperation::Iterate {
            callback,
            key_range,
            count,
            reverse,
        } => Reply::result(
            callback,
            queries::get_records(connection, store, &key_range, count, reverse).map_err(db_error),
        ),
    }
}

/// The reply to a write, which aborts the batch if it failed.
fn write_outcome<T>(
    callback: GenericCallback<BackendResult<T>>,
    result: BackendResult<T>,
) -> Outcome
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    let batch_error = result.as_ref().err().cloned();
    (Reply::result(callback, result), batch_error)
}

fn run_read_write<X: SqlExecutor>(
    connection: &mut X,
    store: &ObjectStore,
    operation: AsyncReadWriteOperation,
) -> Outcome {
    match operation {
        AsyncReadWriteOperation::PutItem {
            callback,
            key,
            value,
            should_overwrite,
            key_generator_current_number,
        } => {
            let (key, key_generator_current_number) = match key {
                Some(key) => (key, key_generator_current_number),
                None => {
                    if store.auto_increment == 0 {
                        let error = BackendError::DbErr("Missing key for PutItem request".into());
                        return (Reply::result(callback, Err(error)), None);
                    }
                    let Some(next_key_generator_current_number) =
                        store.auto_increment.checked_add(1)
                    else {
                        let error = BackendError::DbErr("Key generator overflow".into());
                        return (Reply::result(callback, Err(error)), None);
                    };
                    (
                        IndexedDBKeyType::Number(store.auto_increment as f64),
                        Some(next_key_generator_current_number),
                    )
                },
            };
            let result = queries::put_record(
                connection,
                store,
                key,
                &value,
                should_overwrite,
                key_generator_current_number,
            );
            write_outcome(callback, result.map_err(db_error))
        },
        AsyncReadWriteOperation::RemoveItem {
            callback,
            key_range,
        } => write_outcome(
            callback,
            queries::delete_records(connection, store, &key_range).map_err(db_error),
        ),
        AsyncReadWriteOperation::Clear(callback) => write_outcome(
            callback,
            queries::clear(connection, store).map_err(db_error),
        ),
    }
}

/// Schema operations are transactional too, so a failing one aborts the batch.
fn run_schema<X: SqlExecutor>(
    connection: &mut X,
    store_name: &str,
    operation: AsyncSchemaOperation,
) -> Outcome {
    let (callback, result) = match operation {
        AsyncSchemaOperation::CreateIndex {
            callback,
            index_name,
            key_path,
            unique,
            multi_entry,
        } => (
            callback,
            queries::create_index(
                connection,
                store_name,
                &index_name,
                &key_path,
                unique,
                multi_entry,
            )
            .map(|_| ()),
        ),
        AsyncSchemaOperation::RenameIndex {
            callback,
            index_name,
            new_name,
        } => (
            callback,
            queries::rename_index(connection, store_name, &index_name, &new_name),
        ),
        AsyncSchemaOperation::DeleteIndex {
            callback,
            index_name,
        } => (
            callback,
            queries::delete_index(connection, store_name, &index_name),
        ),
        AsyncSchemaOperation::DeleteObjectStore { callback } => (
            callback,
            queries::delete_object_store(connection, store_name),
        ),
        AsyncSchemaOperation::CreateObjectStore { .. } => {
            unreachable!("Should be handled above");
        },
    };
    let error = result.err().map(db_error);
    (Reply::schema(callback, error.clone()), error)
}
