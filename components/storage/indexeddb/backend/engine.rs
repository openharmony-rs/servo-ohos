/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

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

/// The IndexedDB engine on a [`SqlExecutor`].
///
/// The manager thread's calls run on one connection, each in its own storage transaction. Each
/// batch of requests runs on a new connection from the thread pool, in one storage transaction;
/// the batches run one at a time, in the order the manager hands them over.
pub(crate) struct SqlEngine<X: SqlExecutor> {
    connection: RefCell<X>,
    connector: Arc<dyn Fn() -> Result<X, SqlError> + Send + Sync>,
    batches: Arc<Mutex<BatchQueue>>,
    read_pool: Arc<ThreadPool>,
    write_pool: Arc<ThreadPool>,
    created_db_path: bool,
}

impl<X: SqlExecutor> SqlEngine<X> {
    // TODO: intake dual pools
    pub(crate) fn new(
        path: PathBuf,
        created: bool,
        db_info: &IndexedDBDescription,
        pool: Arc<ThreadPool>,
    ) -> Result<Self, SqlError> {
        let mut connection = X::open(&path)?;
        schema::init_database(&mut connection, db_info)?;
        Ok(Self {
            connector: Arc::from(connection.connector()),
            connection: RefCell::new(connection),
            batches: Default::default(),
            read_pool: pool.clone(),
            write_pool: pool,
            created_db_path: created,
        })
    }

    /// Returns whether the physical db was created as part of `new`.
    pub(crate) fn created_db_path(&self) -> bool {
        self.created_db_path
    }

    /// Runs `f` on the manager thread's connection, in a transaction of its own.
    fn run<T>(
        &self,
        behavior: TransactionBehavior,
        f: impl FnOnce(&mut X) -> Result<T, SqlError>,
    ) -> Result<T, SqlError> {
        with_transaction(&mut *self.connection.borrow_mut(), behavior, f)
    }

    #[cfg(test)]
    pub(super) fn connection(&self) -> std::cell::RefMut<'_, X> {
        self.connection.borrow_mut()
    }
}

/// A batch that waits for its turn, with what to call when it has run.
type QueuedBatch = (KvsTransaction, Box<dyn FnOnce() + Send + 'static>);

/// The batches of an engine that wait for their turn. Running them one at a time keeps the
/// results of overlapping readonly transactions in the order the transactions began, which
/// parallel batches on separate connections didn't (idbtransaction-oncomplete).
#[derive(Default)]
struct BatchQueue {
    waiting: VecDeque<QueuedBatch>,
    /// Whether a pool task is running the batches.
    running: bool,
}

/// Takes the next batch, or marks the queue as not running if there is none.
fn next_batch(batches: &Mutex<BatchQueue>) -> Option<QueuedBatch> {
    let mut queue = batches.lock().unwrap_or_else(PoisonError::into_inner);
    let batch = queue.waiting.pop_front();
    queue.running = batch.is_some();
    batch
}

impl<X: SqlExecutor> MallocSizeOf for SqlEngine<X> {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.created_db_path.size_of(ops) + self.connection.borrow().heap_size()
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
        self.run(TransactionBehavior::Immediate, |connection| {
            queries::create_object_store(connection, store_name, key_path.as_ref(), auto_increment)
        })
    }

    fn delete_store(&self, store_name: &str) -> Result<(), Self::Error> {
        self.run(TransactionBehavior::Immediate, |connection| {
            queries::delete_object_store(connection, store_name)
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
        let spawning_pool = if transaction.mode == IndexedDBTxnMode::Readonly {
            self.read_pool.clone()
        } else {
            self.write_pool.clone()
        };
        {
            let mut queue = self.batches.lock().unwrap_or_else(PoisonError::into_inner);
            queue.waiting.push_back((transaction, on_complete));
            if queue.running {
                return;
            }
            queue.running = true;
        }
        let batches = self.batches.clone();
        let connector = self.connector.clone();
        spawning_pool.spawn(move || {
            while let Some((transaction, on_complete)) = next_batch(&batches) {
                match connector() {
                    Ok(mut connection) => run_batch(&mut connection, transaction),
                    Err(error) => {
                        for request in transaction.requests {
                            request
                                .operation
                                .notify_error(BackendError::DbErr(format!("{error:?}")));
                        }
                    },
                }
                on_complete();
            }
        });
    }

    fn key_generator_current_number(&self, store_name: &str) -> Option<i64> {
        self.run(TransactionBehavior::Deferred, |connection| {
            queries::key_generator_current_number(connection, store_name)
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
        self.run(TransactionBehavior::Immediate, |connection| {
            queries::set_key_generator_current_number(connection, store_name, current_number)
        })
    }

    fn key_path(&self, store_name: &str) -> Option<KeyPath> {
        self.run(TransactionBehavior::Deferred, |connection| {
            queries::key_path(connection, store_name)
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
        self.run(TransactionBehavior::Deferred, |connection| {
            queries::indexes(connection, store_name)
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
        self.run(TransactionBehavior::Immediate, |connection| {
            queries::create_index(
                connection,
                store_name,
                &index_name,
                &key_path,
                unique,
                multi_entry,
            )
        })
    }

    fn delete_index(&self, store_name: &str, index_name: String) -> Result<(), Self::Error> {
        self.run(TransactionBehavior::Immediate, |connection| {
            queries::delete_index(connection, store_name, &index_name)
        })
    }

    fn version(&self) -> Result<u64, Self::Error> {
        self.run(TransactionBehavior::Deferred, queries::version)
    }

    fn set_version(&self, version: u64) -> Result<(), Self::Error> {
        self.run(TransactionBehavior::Immediate, |connection| {
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
