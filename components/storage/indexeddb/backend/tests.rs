/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Tests of the executors and of the engine, run on every executor compiled in.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::time::Duration;

use profile_traits::generic_callback::GenericCallback;
use serde::{Deserialize, Serialize};
use servo_base::threadpool::ThreadPool;
use servo_url::ImmutableOrigin;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, CreateObjectResult, IndexedDBKeyRange, IndexedDBKeyType,
    IndexedDBTxnMode, KeyPath, PutItemResult,
};
use tempfile::TempDir;
use url::Host;

use super::engine::SqlEngine;
use super::executor::{
    Connector, OnRow, SqlError, SqlExecutor, SqlParam, TransactionBehavior, query_row, query_rows,
};
use super::queries;
use crate::indexeddb::engines::{KvsEngine, KvsOperation, KvsTransaction};
use crate::indexeddb::{BackendErrorMapper, IndexedDBDescription};

/// Defines a `#[test]` for every executor compiled in, for each of the given generic functions,
/// in a module named after the executor.
macro_rules! executor_tests {
    ($($test:ident),* $(,)?) => {
        #[cfg(feature = "sqlite-backend")]
        mod sqlite {
            $(
                #[test]
                fn $test() {
                    super::$test::<$crate::indexeddb::backend::sqlite::SqliteExecutor>();
                }
            )*
        }

        #[cfg(ohos_rdb)]
        mod rdb {
            $(
                #[test]
                fn $test() {
                    super::$test::<$crate::indexeddb::backend::rdb::RdbExecutor>();
                }
            )*
        }
    };
}
pub(crate) use executor_tests;

executor_tests!(
    executor_commit_keeps_and_rollback_discards,
    executor_transaction_is_isolated_until_commit,
    executor_rejects_misplaced_transaction_calls,
    executor_round_trips_params,
    executor_counts_changed_rows,
    executor_disk_full_maps_to_quota_exceeded,
    test_cycle,
    test_create_store,
    test_create_store_empty_name,
    test_injection,
    test_key_path,
    test_delete_store,
    test_delete_store_removes_store_records,
    test_async_operations,
    test_delete_item_range_respects_open_bounds,
    test_failed_write_rolls_back_the_batch,
    test_missing_store_fails_only_its_request,
    executor_rejects_what_rdb_rejects,
    test_batch_begin_failure_fails_every_request,
    test_batch_statement_failure_that_ends_the_transaction,
    test_batch_commit_failure,
    test_batches_reply_in_dispatch_order,
);

const TIMEOUT: Duration = Duration::from_secs(10);

fn test_origin() -> ImmutableOrigin {
    ImmutableOrigin::Tuple(
        "test_origin".to_string(),
        Host::Domain("localhost".to_string()),
        80,
    )
}

fn test_description() -> IndexedDBDescription {
    IndexedDBDescription {
        name: "test_db".to_string(),
        origin: test_origin(),
    }
}

fn open_engine<X: SqlExecutor>(dir: &TempDir) -> SqlEngine<X> {
    SqlEngine::new(
        dir.path().to_path_buf(),
        true,
        &test_description(),
        ThreadPool::global(),
    )
    .expect("Opening a database should not fail")
}

/// An executor on a new database with a table `t (k INTEGER PRIMARY KEY, v BLOB)`.
fn open_executor<X: SqlExecutor>(dir: &Path) -> X {
    let mut executor = X::open(dir).unwrap();
    executor
        .execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v BLOB)", &[])
        .unwrap();
    executor
}

fn insert_into_t<X: SqlExecutor>(executor: &mut X, key: i64) {
    executor
        .execute("INSERT INTO t (k) VALUES (?)", &[SqlParam::Integer(key)])
        .unwrap();
}

fn keys_in_t<X: SqlExecutor>(executor: &mut X) -> Vec<i64> {
    query_rows(executor, "SELECT k FROM t ORDER BY k", &[], |row| {
        row.integer(0)
    })
    .unwrap()
}

fn executor_commit_keeps_and_rollback_discards<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    assert!(!executor.in_transaction());

    executor.begin(TransactionBehavior::Immediate).unwrap();
    assert!(executor.in_transaction());
    insert_into_t(&mut executor, 1);
    executor.commit().unwrap();
    assert!(!executor.in_transaction());

    // A transaction stays open across calls and sees its own writes.
    executor.begin(TransactionBehavior::Immediate).unwrap();
    insert_into_t(&mut executor, 2);
    assert_eq!(keys_in_t(&mut executor), vec![1, 2]);
    executor.rollback().unwrap();
    assert!(!executor.in_transaction());
    assert_eq!(keys_in_t(&mut executor), vec![1]);

    executor.begin(TransactionBehavior::Deferred).unwrap();
    insert_into_t(&mut executor, 3);
    executor.commit().unwrap();
    assert_eq!(keys_in_t(&mut executor), vec![1, 3]);
}

fn executor_transaction_is_isolated_until_commit<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = open_executor::<X>(dir.path());
    let mut reader = writer.connector()().unwrap();

    writer.begin(TransactionBehavior::Immediate).unwrap();
    insert_into_t(&mut writer, 1);
    assert_eq!(keys_in_t(&mut reader), Vec::<i64>::new());
    writer.commit().unwrap();
    assert_eq!(keys_in_t(&mut reader), vec![1]);
}

fn executor_rejects_misplaced_transaction_calls<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    assert!(executor.commit().is_err());
    executor.rollback().unwrap();

    executor.begin(TransactionBehavior::Deferred).unwrap();
    assert!(executor.begin(TransactionBehavior::Deferred).is_err());
    assert!(executor.in_transaction());
    executor.commit().unwrap();
    assert!(!executor.in_transaction());
}

fn executor_round_trips_params<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    executor
        .execute("CREATE TABLE p (i INTEGER, t TEXT, b BLOB, n BLOB)", &[])
        .unwrap();
    executor
        .execute(
            "INSERT INTO p (i, t, b, n) VALUES (?, ?, ?, ?)",
            &[
                SqlParam::Integer(i64::MIN),
                SqlParam::Text("a\0b"),
                SqlParam::Blob(&[0, 1, 255]),
                SqlParam::Null,
            ],
        )
        .unwrap();
    let row = query_row(&mut executor, "SELECT i, t, b, n FROM p", &[], |row| {
        Ok((
            row.integer(0)?,
            row.text(1)?,
            row.blob(2)?,
            row.optional_blob(3)?,
        ))
    })
    .unwrap();
    assert_eq!(
        row,
        Some((i64::MIN, "a\0b".to_owned(), vec![0, 1, 255], None))
    );
    assert!(
        query_row(&mut executor, "SELECT n FROM p", &[], |row| row.blob(0)).is_err(),
        "a NULL must not read as a value"
    );
}

fn executor_counts_changed_rows<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    for key in 1..=3 {
        insert_into_t(&mut executor, key);
    }
    let update = "UPDATE t SET v = ? WHERE k >= ?";
    assert_eq!(
        executor
            .execute(update, &[SqlParam::Blob(&[1]), SqlParam::Integer(2)])
            .unwrap(),
        2
    );

    executor.begin(TransactionBehavior::Immediate).unwrap();
    assert_eq!(
        executor
            .execute(update, &[SqlParam::Blob(&[2]), SqlParam::Integer(3)])
            .unwrap(),
        1
    );
    assert_eq!(
        executor
            .execute("DELETE FROM t WHERE k = ?", &[SqlParam::Integer(5)])
            .unwrap(),
        0
    );
    executor.commit().unwrap();

    assert_eq!(executor.execute("DELETE FROM t", &[]).unwrap(), 3);
}

fn executor_disk_full_maps_to_quota_exceeded<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    // RDB runs a transaction on a connection of its own, so the limit is set inside it.
    executor.begin(TransactionBehavior::Immediate).unwrap();
    executor.execute("PRAGMA max_page_count = 2", &[]).unwrap();
    let value = vec![0u8; 65536];
    let error = (0..128)
        .find_map(|key| {
            executor
                .execute(
                    "INSERT INTO t (k, v) VALUES (?, ?)",
                    &[SqlParam::Integer(key), SqlParam::Blob(&value)],
                )
                .err()
        })
        .or_else(|| executor.commit().err())
        .expect("The database should have filled up");
    let _ = executor.rollback();
    assert_eq!(error.into_backend_error(), BackendError::QuotaExceeded);
}

fn test_cycle<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    // Test create
    let db = open_engine::<X>(&dir);
    drop(db);

    // Test open
    let db = open_engine::<X>(&dir);
    let version = db.version().expect("Failed to get version");
    assert_eq!(version, 0);
    db.set_version(5).unwrap();
    let new_version = db.version().expect("Failed to get new version");
    assert_eq!(new_version, 5);
    drop(db);

    let db = open_engine::<X>(&dir);
    assert_eq!(db.version().unwrap(), 5);
}

fn test_create_store<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    let result = db.create_store(store_name, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
    // Try to create the same store again
    let result = db.create_store(store_name, None, false);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::AlreadyExists);
    // Ensure store was not overwritten
    assert!(db.key_generator_current_number(store_name).is_some());
}

fn test_create_store_empty_name<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "";
    let result = db.create_store(store_name, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
}

fn test_injection<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    // Create a normal store
    let store_name1 = "test_store";
    let result = db.create_store(store_name1, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
    // Injection
    let store_name2 = "' OR 1=1 -- -";
    let result = db.create_store(store_name2, None, false);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
    assert_eq!(
        db.object_store_names().unwrap(),
        vec![store_name1.to_owned(), store_name2.to_owned()]
    );
}

fn test_key_path<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    let result = db.create_store(store_name, Some(KeyPath::String("test".to_string())), true);
    assert!(result.is_ok());
    assert_eq!(
        db.key_path(store_name),
        Some(KeyPath::String("test".to_string()))
    );
    assert_eq!(db.key_path("missing"), None);
}

fn test_delete_store<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    db.create_store("test_store", None, false)
        .expect("Failed to create store");
    // Delete the store
    db.delete_store("test_store")
        .expect("Failed to delete store");
    // Try to delete the same store again
    let result = db.delete_store("test_store");
    assert!(result.is_err());
    // Try to delete a non-existing store
    let result = db.delete_store("test_store");
    // Should work as per spec
    assert!(result.is_err());
}

fn record_count<X: SqlExecutor>(executor: &mut X, store_id: i64) -> i64 {
    query_row(
        executor,
        "SELECT COUNT(*) FROM object_data WHERE object_store_id = ?",
        &[SqlParam::Integer(store_id)],
        |row| row.integer(0),
    )
    .unwrap()
    .unwrap()
}

fn test_delete_store_removes_store_records<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);

    db.create_store("test_store", None, false)
        .expect("Failed to create store");
    let store_id = {
        let mut connection = db.connection();
        let object_store = queries::object_store(&mut *connection, "test_store")
            .expect("Failed to fetch store metadata")
            .expect("The store should exist");
        queries::put_record(
            &mut *connection,
            &object_store,
            IndexedDBKeyType::Number(1.0),
            &[1, 2, 3],
            true,
            None,
        )
        .expect("Failed to insert item");
        assert_eq!(record_count(&mut *connection, object_store.id), 1);
        object_store.id
    };

    db.delete_store("test_store")
        .expect("Failed to delete store");

    assert_eq!(record_count(&mut *db.connection(), store_id), 0);
}

fn get_callback<T>() -> (GenericCallback<T>, mpsc::Receiver<T>)
where
    T: for<'de> Deserialize<'de> + Serialize + Send + Sync + 'static,
{
    let (sender, receiver) = mpsc::channel();
    let callback = GenericCallback::new(move |result| {
        assert!(sender.send(result.unwrap()).is_ok());
    })
    .expect("Could not construct callback");
    (callback, receiver)
}

fn request(store_name: &str, operation: AsyncOperation) -> KvsOperation {
    KvsOperation {
        store_name: store_name.to_owned(),
        operation,
    }
}

fn put_request(
    store_name: &str,
    callback: GenericCallback<BackendResult<PutItemResult>>,
    key: IndexedDBKeyType,
    value: Vec<u8>,
    should_overwrite: bool,
) -> KvsOperation {
    request(
        store_name,
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
            callback,
            key: Some(key),
            value,
            should_overwrite,
            key_generator_current_number: None,
        }),
    )
}

/// Runs a batch and waits until it has completed.
fn run_batch<X: SqlExecutor>(
    db: &SqlEngine<X>,
    mode: IndexedDBTxnMode,
    requests: Vec<KvsOperation>,
) {
    let (done_sender, done_receiver) = mpsc::channel();
    db.process_transaction(
        KvsTransaction {
            mode,
            requests: VecDeque::from(requests),
        },
        Box::new(move || {
            let _ = done_sender.send(());
        }),
    );
    done_receiver
        .recv_timeout(TIMEOUT)
        .expect("The batch should complete");
}

/// Receives the one reply of a request, and checks that no second one follows.
fn recv<T>(receiver: &mpsc::Receiver<T>) -> T {
    let reply = receiver
        .recv_timeout(TIMEOUT)
        .expect("The request should reply");
    assert!(
        receiver.recv_timeout(Duration::from_millis(50)).is_err(),
        "The request should reply only once"
    );
    reply
}

fn test_async_operations<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    db.create_store(store_name, None, false)
        .expect("Failed to create store");
    let put = get_callback();
    let put2 = get_callback();
    let put3 = get_callback();
    let put_dup = get_callback();
    let put_overwrite = get_callback();
    let get_item_some = get_callback();
    let get_item_none = get_callback();
    let get_all_items = get_callback();
    let count = get_callback();
    let remove = get_callback();
    let clear = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        vec![
            put_request(
                store_name,
                put.0,
                IndexedDBKeyType::Number(1.0),
                vec![1, 2, 3],
                false,
            ),
            put_request(
                store_name,
                put2.0,
                IndexedDBKeyType::String("2.0".to_string()),
                vec![4, 5, 6],
                false,
            ),
            put_request(
                store_name,
                put3.0,
                IndexedDBKeyType::Array(vec![
                    IndexedDBKeyType::String("3".to_string()),
                    IndexedDBKeyType::Number(0.0),
                ]),
                vec![7, 8, 9],
                false,
            ),
            // Try to put a duplicate key without overwrite
            put_request(
                store_name,
                put_dup.0,
                IndexedDBKeyType::Number(1.0),
                vec![10, 11, 12],
                false,
            ),
            put_request(
                store_name,
                put_overwrite.0,
                IndexedDBKeyType::Number(1.0),
                vec![13, 14, 15],
                true,
            ),
            request(
                store_name,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                    callback: get_item_some.0,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_name,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                    callback: get_item_none.0,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(5.0)),
                }),
            ),
            request(
                store_name,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllItems {
                    callback: get_all_items.0,
                    key_range: IndexedDBKeyRange::lower_bound(IndexedDBKeyType::Number(0.0), false),
                    count: None,
                }),
            ),
            request(
                store_name,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                    callback: count.0,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_name,
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem {
                    callback: remove.0,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_name,
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear(clear.0)),
            ),
        ],
    );
    recv(&put.1).unwrap();
    recv(&put2.1).unwrap();
    recv(&put3.1).unwrap();
    let err = recv(&put_dup.1).unwrap();
    assert_eq!(err, PutItemResult::CannotOverwrite);
    let overwritten = recv(&put_overwrite.1).unwrap();
    assert_eq!(
        overwritten,
        PutItemResult::Key(IndexedDBKeyType::Number(1.0))
    );
    let value = recv(&get_item_some.1).unwrap();
    assert_eq!(value, Some(vec![13, 14, 15]));
    let value = recv(&get_item_none.1).unwrap();
    assert_eq!(value, None);
    let all_items = recv(&get_all_items.1).unwrap();
    assert_eq!(all_items.len(), 3);
    // Check that all three items are present
    assert!(all_items.contains(&vec![13, 14, 15]));
    assert!(all_items.contains(&vec![4, 5, 6]));
    assert!(all_items.contains(&vec![7, 8, 9]));
    let amount = recv(&count.1).unwrap();
    assert_eq!(amount, 1);
    recv(&remove.1).unwrap();
    recv(&clear.1).unwrap();
}

fn test_delete_item_range_respects_open_bounds<X: SqlExecutor>() {
    fn remaining_keys_after_delete<X: SqlExecutor>(
        lower: i32,
        upper: i32,
        lower_open: bool,
        upper_open: bool,
    ) -> Vec<i32> {
        let dir = tempfile::tempdir().unwrap();
        let db = open_engine::<X>(&dir);
        let store_name = "test_store";
        db.create_store(store_name, None, false)
            .expect("Failed to create store");
        let mut connection = db.connection();
        let store = queries::object_store(&mut *connection, store_name)
            .expect("Failed to get object store")
            .expect("The store should exist");

        for key in 1..=10 {
            queries::put_record(
                &mut *connection,
                &store,
                IndexedDBKeyType::Number(key as f64),
                &[key as u8],
                false,
                None,
            )
            .expect("Failed to seed object store");
        }

        queries::delete_records(
            &mut *connection,
            &store,
            &IndexedDBKeyRange::new(
                Some(IndexedDBKeyType::Number(lower as f64)),
                Some(IndexedDBKeyType::Number(upper as f64)),
                lower_open,
                upper_open,
            ),
        )
        .expect("Failed to delete key range");

        queries::get_all_keys(
            &mut *connection,
            &store,
            &IndexedDBKeyRange::default(),
            None,
        )
        .expect("Failed to read remaining keys")
        .into_iter()
        .map(|key| match key {
            IndexedDBKeyType::Number(number) => number as i32,
            other => panic!("Expected numeric key, got {other:?}"),
        })
        .collect()
    }

    assert_eq!(
        remaining_keys_after_delete::<X>(3, 8, false, false),
        vec![1, 2, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete::<X>(3, 8, true, false),
        vec![1, 2, 3, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete::<X>(3, 8, false, true),
        vec![1, 2, 8, 9, 10]
    );
    assert_eq!(
        remaining_keys_after_delete::<X>(3, 8, true, true),
        vec![1, 2, 3, 8, 9, 10]
    );
}

fn stored_keys<X: SqlExecutor>(db: &SqlEngine<X>, store_name: &str) -> Vec<IndexedDBKeyType> {
    let mut connection = db.connection();
    let store = queries::object_store(&mut *connection, store_name)
        .unwrap()
        .unwrap();
    queries::get_all_keys(
        &mut *connection,
        &store,
        &IndexedDBKeyRange::default(),
        None,
    )
    .unwrap()
}

/// A write that fails in the database rolls back the whole batch, and every request of the batch
/// reports an error.
fn test_failed_write_rolls_back_the_batch<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    db.create_store(store_name, None, false).unwrap();
    for index_name in ["a", "b"] {
        db.create_index(
            store_name,
            index_name.to_owned(),
            KeyPath::String(index_name.to_owned()),
            false,
            false,
        )
        .unwrap();
    }

    let put = get_callback();
    let rename = get_callback();
    let put_after = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Versionchange,
        vec![
            put_request(
                store_name,
                put.0,
                IndexedDBKeyType::Number(1.0),
                vec![1],
                false,
            ),
            // Index names are unique per store, so this fails in the database.
            request(
                store_name,
                AsyncOperation::Schema(AsyncSchemaOperation::RenameIndex {
                    callback: rename.0,
                    index_name: "a".to_owned(),
                    new_name: "b".to_owned(),
                }),
            ),
            put_request(
                store_name,
                put_after.0,
                IndexedDBKeyType::Number(2.0),
                vec![2],
                false,
            ),
        ],
    );

    assert!(recv(&put.1).is_err());
    assert!(matches!(recv(&rename.1), BackendError::DbErr(_)));
    assert!(recv(&put_after.1).is_err());
    assert_eq!(stored_keys(&db, store_name), vec![]);
    let index_names: Vec<String> = db
        .indexes(store_name)
        .unwrap()
        .into_iter()
        .map(|index| index.name)
        .collect();
    assert_eq!(index_names, vec!["a".to_owned(), "b".to_owned()]);
}

/// A request for a store that does not exist fails without effects, so the batch goes on.
fn test_missing_store_fails_only_its_request<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    db.create_store(store_name, None, false).unwrap();

    let missing = get_callback();
    let put = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        vec![
            put_request(
                "missing",
                missing.0,
                IndexedDBKeyType::Number(1.0),
                vec![1],
                false,
            ),
            put_request(
                store_name,
                put.0,
                IndexedDBKeyType::Number(2.0),
                vec![2],
                false,
            ),
        ],
    );

    assert_eq!(recv(&missing.1), Err(BackendError::StoreNotFound));
    assert_eq!(
        recv(&put.1),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(2.0)))
    );
    assert_eq!(
        stored_keys(&db, store_name),
        vec![IndexedDBKeyType::Number(2.0)]
    );
}

/// The SQLite executor refuses what RDB's executor refuses, so that the host tests catch SQL that
/// would only fail on a device.
fn executor_rejects_what_rdb_rejects<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    for in_transaction in [false, true] {
        if in_transaction {
            executor.begin(TransactionBehavior::Immediate).unwrap();
        }
        for statement in [
            "SELECT 1",
            "WITH w AS (SELECT 1) INSERT INTO t (k) SELECT * FROM w",
            "-- comment\nINSERT INTO t (k) VALUES (1)",
            "RELEASE s",
        ] {
            assert!(
                executor.execute(statement, &[]).is_err(),
                "{statement:?} in a transaction: {in_transaction}"
            );
        }
        if in_transaction {
            executor.commit().unwrap();
        }
    }
    for statement in [
        "INSERT INTO t (k) VALUES (2) RETURNING k",
        "PRAGMA journal_mode",
    ] {
        assert!(
            query_row(&mut executor, statement, &[], |row| row.integer(0)).is_err(),
            "{statement:?}"
        );
    }
    assert_eq!(keys_in_t(&mut executor), Vec::<i64>::new());
}

/// Where a [`Faulty`] executor fails.
#[derive(Default)]
struct Faults {
    /// Fail every `begin`.
    begin: bool,
    /// Fail every `commit`, after rolling the transaction back.
    commit: bool,
    /// Fail the statement or query whose SQL contains this text, after letting the given number
    /// of them through, and end the open transaction the way SQLite does after SQLITE_FULL.
    statement: Option<(&'static str, usize)>,
}

/// The faults of the [`Faulty`] executors opened on each directory.
static FAULTS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<Faults>>>>> =
    LazyLock::new(Default::default);

fn faults_for(directory: &Path) -> Arc<Mutex<Faults>> {
    FAULTS
        .lock()
        .unwrap()
        .entry(directory.to_owned())
        .or_default()
        .clone()
}

/// An executor that wraps another and fails where the [`Faults`] of its directory say.
struct Faulty<X> {
    inner: X,
    faults: Arc<Mutex<Faults>>,
}

impl<X: SqlExecutor> Faulty<X> {
    fn inject(&mut self, sql: &str) -> Result<(), SqlError> {
        let mut faults = self.faults.lock().unwrap();
        match &mut faults.statement {
            Some((text, 0)) if sql.contains(*text) => {
                faults.statement = None;
                drop(faults);
                let _ = self.inner.rollback();
                Err(SqlError::TransactionState("injected statement failure"))
            },
            Some((text, before)) if sql.contains(*text) => {
                *before -= 1;
                Ok(())
            },
            _ => Ok(()),
        }
    }
}

impl<X: SqlExecutor> SqlExecutor for Faulty<X> {
    fn open(directory: &Path) -> Result<Self, SqlError> {
        Ok(Self {
            inner: X::open(directory)?,
            faults: faults_for(directory),
        })
    }

    fn connector(&self) -> Connector<Self> {
        let connector = self.inner.connector();
        let faults = self.faults.clone();
        Box::new(move || {
            Ok(Self {
                inner: connector()?,
                faults: faults.clone(),
            })
        })
    }

    fn execute(&mut self, sql: &str, params: &[SqlParam<'_>]) -> Result<u64, SqlError> {
        self.inject(sql)?;
        self.inner.execute(sql, params)
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError> {
        self.inject(sql)?;
        self.inner.query(sql, params, on_row)
    }

    fn begin(&mut self, behavior: TransactionBehavior) -> Result<(), SqlError> {
        if self.faults.lock().unwrap().begin {
            return Err(SqlError::TransactionState("injected begin failure"));
        }
        self.inner.begin(behavior)
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        if self.faults.lock().unwrap().commit {
            self.inner.rollback()?;
            return Err(SqlError::TransactionState("injected commit failure"));
        }
        self.inner.commit()
    }

    fn rollback(&mut self) -> Result<(), SqlError> {
        self.inner.rollback()
    }

    fn in_transaction(&self) -> bool {
        self.inner.in_transaction()
    }

    fn heap_size(&self) -> usize {
        self.inner.heap_size()
    }
}

/// An engine on [`Faulty`] executors with a store `test_store` that holds the key 1.
fn open_faulty_engine<X: SqlExecutor>(dir: &TempDir) -> SqlEngine<Faulty<X>> {
    let db = open_engine::<Faulty<X>>(dir);
    db.create_store("test_store", None, false).unwrap();
    let put = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        vec![put_request(
            "test_store",
            put.0,
            IndexedDBKeyType::Number(1.0),
            vec![1],
            false,
        )],
    );
    assert!(recv(&put.1).is_ok());
    db
}

fn get_request(
    callback: GenericCallback<BackendResult<Option<Vec<u8>>>>,
    key: f64,
) -> KvsOperation {
    request(
        "test_store",
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
            callback,
            key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(key)),
        }),
    )
}

/// If the storage transaction can't begin, every request of the batch fails once.
fn test_batch_begin_failure_fails_every_request<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    faults_for(dir.path()).lock().unwrap().begin = true;

    let put = get_callback();
    let get = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        vec![
            put_request(
                "test_store",
                put.0,
                IndexedDBKeyType::Number(2.0),
                vec![2],
                false,
            ),
            get_request(get.0, 1.0),
        ],
    );
    assert!(recv(&put.1).is_err());
    assert!(recv(&get.1).is_err());

    faults_for(dir.path()).lock().unwrap().begin = false;
    assert_eq!(
        stored_keys(&db, "test_store"),
        vec![IndexedDBKeyType::Number(1.0)]
    );
}

/// A statement that ends the storage transaction, as SQLite does after SQLITE_FULL, fails the rest
/// of the batch. A readwrite batch fails as a whole; the reads of a readonly batch that ran
/// before have replied already.
fn test_batch_statement_failure_that_ends_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);

    faults_for(dir.path()).lock().unwrap().statement = Some(("INSERT INTO object_data", 1));
    let puts: Vec<_> = (0..3).map(|_| get_callback()).collect();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        puts.iter()
            .zip([2.0, 3.0, 4.0])
            .map(|(put, key)| {
                put_request(
                    "test_store",
                    put.0.clone(),
                    IndexedDBKeyType::Number(key),
                    vec![1],
                    false,
                )
            })
            .collect(),
    );
    for put in &puts {
        assert!(recv(&put.1).is_err());
    }
    assert_eq!(
        stored_keys(&db, "test_store"),
        vec![IndexedDBKeyType::Number(1.0)]
    );

    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_data", 1));
    let gets: Vec<_> = (0..3).map(|_| get_callback()).collect();
    run_batch(
        &db,
        IndexedDBTxnMode::Readonly,
        gets.iter()
            .map(|get| get_request(get.0.clone(), 1.0))
            .collect(),
    );
    assert_eq!(recv(&gets[0].1), Ok(Some(vec![1])));
    assert!(recv(&gets[1].1).is_err());
    assert!(recv(&gets[2].1).is_err());
}

/// If the commit fails, no write of the batch is reported as done; the replies of a readonly
/// batch went out before.
fn test_batch_commit_failure<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    faults_for(dir.path()).lock().unwrap().commit = true;

    let puts: Vec<_> = (0..2).map(|_| get_callback()).collect();
    run_batch(
        &db,
        IndexedDBTxnMode::Readwrite,
        puts.iter()
            .zip([2.0, 3.0])
            .map(|(put, key)| {
                put_request(
                    "test_store",
                    put.0.clone(),
                    IndexedDBKeyType::Number(key),
                    vec![1],
                    false,
                )
            })
            .collect(),
    );
    for put in &puts {
        assert!(recv(&put.1).is_err());
    }

    let get = get_callback();
    run_batch(
        &db,
        IndexedDBTxnMode::Readonly,
        vec![get_request(get.0, 1.0)],
    );
    assert_eq!(recv(&get.1), Ok(Some(vec![1])));

    faults_for(dir.path()).lock().unwrap().commit = false;
    assert_eq!(
        stored_keys(&db, "test_store"),
        vec![IndexedDBKeyType::Number(1.0)]
    );
}

/// Batches reply in the order the manager hands them over, also those of overlapping readonly
/// transactions, which ran on parallel connections before and could overtake each other.
fn test_batches_reply_in_dispatch_order<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    db.create_store("test_store", None, false).unwrap();

    let (order_sender, order) = mpsc::channel();
    let (done_sender, done) = mpsc::channel();
    for id in 0..8 {
        let order_sender = order_sender.clone();
        let callback = GenericCallback::new(move |_| {
            let _ = order_sender.send(id);
        })
        .unwrap();
        let done_sender = done_sender.clone();
        db.process_transaction(
            KvsTransaction {
                mode: IndexedDBTxnMode::Readonly,
                requests: VecDeque::from([get_request(callback, 1.0)]),
            },
            Box::new(move || {
                let _ = done_sender.send(());
            }),
        );
    }
    for _ in 0..8 {
        done.recv_timeout(TIMEOUT)
            .expect("The batch should complete");
    }
    let replies: Vec<i32> = (0..8)
        .map(|_| {
            order
                .recv_timeout(TIMEOUT)
                .expect("The request should reply")
        })
        .collect();
    assert_eq!(replies, (0..8).collect::<Vec<_>>());
}

#[test]
fn key_path_round_trip() {
    let key_path = KeyPath::String("field".to_string());
    let encoded = queries::encode_key_path(&key_path).unwrap();
    let decoded = queries::decode_key_path(&encoded).unwrap();
    assert_eq!(decoded, key_path);
}

#[test]
fn key_path_decode_rejects_invalid_bytes() {
    assert!(queries::decode_key_path(&[0xff]).is_err());
}

#[test]
fn has_enospc_detects_enospc_and_nothing_else() {
    use crate::shared::has_enospc;

    let full = std::io::Error::from_raw_os_error(libc::ENOSPC);
    assert!(has_enospc(Some(&full)));
    let denied = std::io::Error::from_raw_os_error(libc::EACCES);
    assert!(!has_enospc(Some(&denied)));
    assert!(!has_enospc(None));
}

/// With both executors compiled in, the pref decides which one a database is opened on.
#[cfg(all(feature = "sqlite-backend", ohos_rdb))]
#[test]
fn pref_selects_the_executor() {
    use servo_config::prefs::{self, Preferences};

    use super::twin::TwinExecutor;

    for rdb_enabled in [true, false] {
        prefs::set(Preferences {
            storage_ohos_rdb_backend_enabled: rdb_enabled,
            ..Preferences::default()
        });
        let dir = tempfile::tempdir().unwrap();
        let executor = TwinExecutor::open(dir.path()).unwrap();
        assert_eq!(matches!(executor, TwinExecutor::Rdb(_)), rdb_enabled);
    }
}
