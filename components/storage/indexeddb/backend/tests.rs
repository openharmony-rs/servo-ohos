/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Tests of the executors and of the engine, run on every executor compiled in.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::time::{Duration, Instant};

use profile_traits::generic_callback::GenericCallback;
use serde::{Deserialize, Serialize};
use servo_base::threadpool::ThreadPool;
use servo_url::ImmutableOrigin;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, CreateObjectResult, IndexedDBKeyRange, IndexedDBKeyType,
    IndexedDBTxnId, IndexedDBTxnMode, IndexedDBValue, KeyPath, PutItemResult,
};
use tempfile::TempDir;
use url::Host;
use uuid::Uuid;

use super::engine::SqlEngine;
use super::executor::{
    Connector, OnRow, SqlError, SqlExecutor, SqlParam, TransactionBehavior, query_row, query_rows,
    with_transaction,
};
use super::queries;
use super::schema::{self, SCHEMA_VERSION};
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
    executor_disk_full_ends_the_transaction,
    executor_failing_query_reports_the_transaction_state,
    executor_constraint_violation_keeps_the_transaction,
    executor_immediate_locks_at_begin_and_deferred_reads_a_snapshot,
    executor_runs_each_kind_of_statement,
    executor_user_version_is_transactional_and_persistent,
    executor_delete_removes_the_database,
    schema_version_is_stamped_on_a_new_database,
    schema_version_match_keeps_the_database,
    schema_version_mismatch_resets_the_database,
    indexeddb_version_is_independent_of_the_schema_version,
    schema_reset_that_keeps_the_old_database_fails,
    test_cycle,
    test_create_store,
    test_create_store_empty_name,
    test_injection,
    test_key_path,
    test_delete_store,
    test_delete_store_removes_store_records,
    test_async_operations,
    test_delete_item_range_respects_open_bounds,
    test_failed_write_fails_the_transaction,
    test_missing_store_fails_only_its_request,
    test_key_generator_follows_generated_and_explicit_keys,
    test_key_generator_generates_up_to_2_pow_53,
    test_large_explicit_keys_and_the_key_generator,
    test_failed_add_changes_nothing,
    test_abort_reverts_the_key_generator,
    test_abort_rolls_back_data_schema_and_version,
    test_transaction_spanning_batches_commits_atomically,
    test_requests_run_in_order,
    test_two_databases_progress_independently,
    executor_rejects_what_rdb_rejects,
    test_begin_failure_fails_the_transaction,
    test_statement_failure_that_ends_the_transaction_fails_it,
    test_readonly_failure_that_ends_the_sql_transaction_fails_the_writer,
    test_commit_failure_rolls_back,
    test_batches_reply_in_dispatch_order,
    test_job_that_ends_the_sql_transaction_fails_the_writer,
    test_metadata_reads_do_not_wait_for_the_connection,
    test_upgrade_without_requests_sets_the_version_at_commit,
);

const TIMEOUT: Duration = Duration::from_secs(10);

/// The transaction numbered `serial` of a test connection.
pub(crate) fn txn(serial: u64) -> IndexedDBTxnId {
    IndexedDBTxnId {
        connection: Uuid::nil(),
        serial,
    }
}

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

fn open_engine_in<X: SqlExecutor>(dir: &Path, created: bool) -> SqlEngine<X> {
    SqlEngine::new(
        dir.to_path_buf(),
        created,
        &test_description(),
        ThreadPool::global(),
    )
    .expect("Opening a database should not fail")
}

fn schema_version_of<X: SqlExecutor>(db: &SqlEngine<X>) -> i64 {
    schema::stored_schema_version(&mut *db.connection()).unwrap()
}

/// Runs `f` on the engine's connection in a transaction of its own, to set up a database
/// without going through script's requests.
fn in_own_transaction<X: SqlExecutor, T>(
    db: &SqlEngine<X>,
    f: impl FnOnce(&mut X) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let result = with_transaction(&mut *db.connection(), TransactionBehavior::Immediate, f);
    db.reload_metadata();
    result
}

fn create_store<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store_name: &str,
    key_path: Option<KeyPath>,
    auto_increment: bool,
) -> Result<CreateObjectResult, SqlError> {
    in_own_transaction(db, |executor| {
        queries::create_object_store(executor, store_name, key_path.as_ref(), auto_increment)
    })
}

fn delete_store<X: SqlExecutor>(db: &SqlEngine<X>, store_name: &str) -> Result<(), SqlError> {
    in_own_transaction(db, |executor| {
        queries::delete_object_store(executor, store_name)
    })
}

fn create_index<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store_name: &str,
    index_name: String,
    key_path: KeyPath,
    unique: bool,
    multi_entry: bool,
) -> Result<CreateObjectResult, SqlError> {
    in_own_transaction(db, |executor| {
        queries::create_index(
            executor,
            store_name,
            &index_name,
            &key_path,
            unique,
            multi_entry,
        )
    })
}

fn set_version<X: SqlExecutor>(db: &SqlEngine<X>, version: u64) -> Result<(), SqlError> {
    in_own_transaction(db, |executor| queries::set_version(executor, version))
}

fn has_table<X: SqlExecutor>(executor: &mut X, name: &str) -> bool {
    query_row(
        executor,
        "SELECT 1 FROM sqlite_master WHERE name = ?",
        &[SqlParam::Text(name)],
        |_| Ok(()),
    )
    .unwrap()
    .is_some()
}

/// An executor on a new database with a table `t (k INTEGER PRIMARY KEY, v BLOB)`.
fn open_executor<X: SqlExecutor>(dir: &Path) -> X {
    let mut executor = X::open(dir).unwrap();
    executor
        .execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v BLOB)", &[])
        .unwrap();
    executor
}

/// An executor like [`open_executor`]'s, in WAL mode like the engine's databases.
fn open_wal_executor<X: SqlExecutor>(dir: &Path) -> X {
    let mut executor = open_executor::<X>(dir);
    executor.execute("PRAGMA journal_mode = WAL", &[]).unwrap();
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

fn executor_disk_full_ends_the_transaction<X: SqlExecutor>() {
    let value = vec![0u8; 65536];
    let statements = [
        "INSERT INTO t (k, v) VALUES (?, ?)",
        // A statement that can fail half-way, after which SQLite keeps the transaction open.
        "INSERT INTO t (k, v) SELECT ? + n, ? FROM (SELECT 0 AS n UNION ALL SELECT 1000)",
    ];
    for statement in statements {
        let dir = tempfile::tempdir().unwrap();
        let mut executor = open_executor::<X>(dir.path());
        insert_into_t(&mut executor, 1);
        executor.begin(TransactionBehavior::Immediate).unwrap();
        insert_into_t(&mut executor, 2);
        executor.execute("PRAGMA max_page_count = 8", &[]).unwrap();
        let error = (10..100)
            .find_map(|key| {
                executor
                    .execute(statement, &[SqlParam::Integer(key), SqlParam::Blob(&value)])
                    .err()
            })
            .expect("The database should have filled up");
        assert_eq!(error.into_backend_error(), BackendError::QuotaExceeded);
        assert!(!executor.in_transaction(), "{statement}");
        assert!(executor.commit().is_err());
        executor.rollback().unwrap();
        assert_eq!(keys_in_t(&mut executor), vec![1], "{statement}");
    }
}

/// After a failing query, the executor reports a transaction as open only if it still is: then
/// rolling it back discards what it wrote, and if not, nothing of it is left.
fn executor_failing_query_reports_the_transaction_state<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    insert_into_t(&mut executor, 1);
    executor.begin(TransactionBehavior::Immediate).unwrap();
    insert_into_t(&mut executor, 2);
    assert!(
        query_row(&mut executor, "SELECT k FROM missing", &[], |row| {
            row.integer(0)
        })
        .is_err()
    );
    let open = executor.in_transaction();
    insert_into_t(&mut executor, 3);
    if open {
        executor.rollback().unwrap();
        assert_eq!(keys_in_t(&mut executor), vec![1]);
    } else {
        assert_eq!(keys_in_t(&mut executor), vec![1, 3]);
    }
}

fn executor_constraint_violation_keeps_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    insert_into_t(&mut executor, 1);
    executor.begin(TransactionBehavior::Immediate).unwrap();
    insert_into_t(&mut executor, 2);
    assert!(
        executor
            .execute("INSERT INTO t (k) VALUES (1)", &[])
            .is_err()
    );
    assert!(executor.in_transaction());
    insert_into_t(&mut executor, 3);
    executor.commit().unwrap();
    assert_eq!(keys_in_t(&mut executor), vec![1, 2, 3]);
}

fn executor_immediate_locks_at_begin_and_deferred_reads_a_snapshot<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_wal_executor::<X>(dir.path());
    let mut b = a.connector()().unwrap();
    // A second writer fails once the busy timeout has run out. RDB's transactions wait 2 s
    // whatever this sets.
    b.execute("PRAGMA busy_timeout = 100", &[]).unwrap();
    insert_into_t(&mut a, 1);

    // The write lock is taken before the transaction writes, so a second writer can't begin.
    a.begin(TransactionBehavior::Immediate).unwrap();
    assert!(b.begin(TransactionBehavior::Immediate).is_err());
    assert!(!b.in_transaction());
    b.begin(TransactionBehavior::Deferred).unwrap();
    assert_eq!(keys_in_t(&mut b), vec![1]);
    b.commit().unwrap();
    a.commit().unwrap();

    // A deferred transaction reads one snapshot, taken by its first read.
    a.begin(TransactionBehavior::Deferred).unwrap();
    insert_into_t(&mut b, 2);
    assert_eq!(keys_in_t(&mut a), vec![1, 2]);
    insert_into_t(&mut b, 3);
    assert_eq!(keys_in_t(&mut a), vec![1, 2]);
    a.commit().unwrap();
    assert_eq!(keys_in_t(&mut a), vec![1, 2, 3]);
}

/// The kinds of statement that the IndexedDB layer runs, inside a transaction and outside one.
fn executor_runs_each_kind_of_statement<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    for in_transaction in [false, true] {
        if in_transaction {
            executor.begin(TransactionBehavior::Immediate).unwrap();
        }
        for statement in [
            "\n  CREATE TABLE s (x INTEGER PRIMARY KEY)",
            "CREATE INDEX s_x ON s (x)",
            "ALTER TABLE s ADD COLUMN y INTEGER",
            "INSERT INTO s (x) VALUES (1)",
            "INSERT INTO s (x) VALUES (2)",
            "UPDATE s SET y = 3",
            "DELETE FROM s WHERE x = 2",
            "PRAGMA user_version = 4",
            "DROP INDEX s_x",
        ] {
            if let Err(error) = executor.execute(statement, &[]) {
                panic!("{statement:?} in a transaction: {in_transaction}: {error:?}");
            }
        }
        let rows = query_rows(&mut executor, "SELECT x, y FROM s", &[], |row| {
            Ok((row.integer(0)?, row.integer(1)?))
        })
        .unwrap();
        assert_eq!(rows, vec![(1, 3)]);
        assert_eq!(schema::stored_schema_version(&mut executor).unwrap(), 4);
        executor.execute("DROP TABLE s", &[]).unwrap();
        if in_transaction {
            executor.commit().unwrap();
        }
        assert!(!has_table(&mut executor, "s"));
    }
}

fn executor_user_version_is_transactional_and_persistent<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let mut executor = open_executor::<X>(dir.path());
    let version = |executor: &mut X| schema::stored_schema_version(executor).unwrap();
    assert_eq!(version(&mut executor), 0);

    executor.begin(TransactionBehavior::Immediate).unwrap();
    executor.execute("PRAGMA user_version = 5", &[]).unwrap();
    assert_eq!(version(&mut executor), 5);
    executor.rollback().unwrap();
    assert_eq!(version(&mut executor), 0);

    executor.begin(TransactionBehavior::Immediate).unwrap();
    executor.execute("PRAGMA user_version = 6", &[]).unwrap();
    executor.commit().unwrap();
    assert_eq!(version(&mut executor), 6);

    // RDB runs each transaction, and each statement outside one, on a pool connection.
    let mut other = executor.connector()().unwrap();
    assert_eq!(version(&mut other), 6);
    other.begin(TransactionBehavior::Deferred).unwrap();
    assert_eq!(version(&mut other), 6);
    other.commit().unwrap();
    drop(other);
    drop(executor);

    let mut executor = X::open(dir.path()).unwrap();
    assert_eq!(version(&mut executor), 6);
}

fn executor_delete_removes_the_database<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    for in_transaction in [false, true] {
        let mut executor = open_executor::<X>(dir.path());
        insert_into_t(&mut executor, 1);
        executor.execute("PRAGMA user_version = 3", &[]).unwrap();
        if in_transaction {
            executor.begin(TransactionBehavior::Immediate).unwrap();
            insert_into_t(&mut executor, 2);
        }
        executor.delete().unwrap();

        let mut executor = X::open(dir.path()).unwrap();
        assert!(!has_table(&mut executor, "t"));
        assert_eq!(schema::stored_schema_version(&mut executor).unwrap(), 0);
    }
}

fn schema_version_is_stamped_on_a_new_database<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    // The directory may exist already when the database file doesn't.
    let db = open_engine_in::<X>(dir.path(), false);
    assert!(db.created_db_path());
    assert_eq!(schema_version_of(&db), SCHEMA_VERSION);
    assert_eq!(db.version().unwrap(), 0);
    assert!(db.object_store_names().unwrap().is_empty());
}

fn schema_version_match_keeps_the_database<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine_in::<X>(dir.path(), true);
    create_store(&db, "store", None, false).unwrap();
    set_version(&db, 3).unwrap();
    drop(db);

    let db = open_engine_in::<X>(dir.path(), false);
    assert!(!db.created_db_path());
    assert_eq!(db.object_store_names().unwrap(), vec!["store".to_owned()]);
    assert_eq!(db.version().unwrap(), 3);
    assert_eq!(schema_version_of(&db), SCHEMA_VERSION);
}

fn schema_version_mismatch_resets_the_database<X: SqlExecutor>() {
    // 0 is what a database from before schema versions has.
    for stored_version in [0, SCHEMA_VERSION + 1] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_engine_in::<X>(dir.path(), true);
        create_store(&db, "store", None, false).unwrap();
        set_version(&db, 3).unwrap();
        {
            let mut connection = db.connection();
            connection.execute("CREATE TABLE extra (x)", &[]).unwrap();
            connection
                .execute(&format!("PRAGMA user_version = {stored_version}"), &[])
                .unwrap();
        }
        drop(db);

        let db = open_engine_in::<X>(dir.path(), false);
        assert!(db.created_db_path(), "stored version {stored_version}");
        assert!(db.object_store_names().unwrap().is_empty());
        assert_eq!(db.version().unwrap(), 0);
        assert_eq!(schema_version_of(&db), SCHEMA_VERSION);
        assert!(
            !has_table(&mut *db.connection(), "extra"),
            "The database file should have been deleted"
        );
    }
}

fn indexeddb_version_is_independent_of_the_schema_version<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine_in::<X>(dir.path(), true);
    // https://w3c.github.io/IndexedDB/#dom-idbfactory-open: the largest version.
    let largest_version = (1 << 53) - 1;
    set_version(&db, largest_version).unwrap();
    assert_eq!(schema_version_of(&db), SCHEMA_VERSION);
    drop(db);

    let db = open_engine_in::<X>(dir.path(), false);
    assert!(!db.created_db_path());
    assert_eq!(db.version().unwrap(), largest_version);
    assert_eq!(schema_version_of(&db), SCHEMA_VERSION);
}

fn schema_reset_that_keeps_the_old_database_fails<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine_in::<X>(dir.path(), true);
    create_store(&db, "store", None, false).unwrap();
    db.connection()
        .execute(
            &format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1),
            &[],
        )
        .unwrap();
    drop(db);

    faults_for(dir.path()).lock().unwrap().keep_on_delete = true;
    let open = SqlEngine::<Faulty<X>>::new(
        dir.path().to_path_buf(),
        false,
        &test_description(),
        ThreadPool::global(),
    );
    assert!(matches!(open, Err(SqlError::ResetFailed)));

    faults_for(dir.path()).lock().unwrap().keep_on_delete = false;
    let db = open_engine_in::<Faulty<X>>(dir.path(), false);
    assert!(db.created_db_path());
    assert!(db.object_store_names().unwrap().is_empty());
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
    set_version(&db, 5).unwrap();
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
    let result = create_store(&db, store_name, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
    // Try to create the same store again
    let result = create_store(&db, store_name, None, false);
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
    let result = create_store(&db, store_name, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
}

fn test_injection<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    // Create a normal store
    let store_name1 = "test_store";
    let result = create_store(&db, store_name1, None, true);
    assert!(result.is_ok());
    let create_result = result.unwrap();
    assert_eq!(create_result, CreateObjectResult::Created);
    // Injection
    let store_name2 = "' OR 1=1 -- -";
    let result = create_store(&db, store_name2, None, false);
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
    let result = create_store(
        &db,
        store_name,
        Some(KeyPath::String("test".to_string())),
        true,
    );
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
    create_store(&db, "test_store", None, false).expect("Failed to create store");
    // Delete the store
    delete_store(&db, "test_store").expect("Failed to delete store");
    // Try to delete the same store again
    let result = delete_store(&db, "test_store");
    assert!(result.is_err());
    // Try to delete a non-existing store
    let result = delete_store(&db, "test_store");
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

    create_store(&db, "test_store", None, false).expect("Failed to create store");
    let store_id = {
        let mut connection = db.connection();
        let object_store = queries::object_store(&mut *connection, "test_store")
            .expect("Failed to fetch store metadata")
            .expect("The store should exist");
        queries::put_record(
            &mut *connection,
            &object_store,
            Some(IndexedDBKeyType::Number(1.0)),
            &[1, 2, 3],
            true,
        )
        .expect("Failed to insert item");
        assert_eq!(record_count(&mut *connection, object_store.id), 1);
        object_store.id
    };

    delete_store(&db, "test_store").expect("Failed to delete store");

    assert_eq!(record_count(&mut *db.connection(), store_id), 0);
}

/// A callback and the receiver of what it is called with.
type Callback<T> = (GenericCallback<T>, mpsc::Receiver<T>);

fn get_callback<T>() -> Callback<T>
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
        }),
    )
}

/// Runs `puts` (an explicit key or `None` for a generated one, and the value) in order as one
/// batch of the readwrite `transaction` on `store_name`, and returns their replies.
fn run_puts<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    store_name: &str,
    puts: Vec<(Option<IndexedDBKeyType>, Vec<u8>)>,
    should_overwrite: bool,
) -> Vec<BackendResult<PutItemResult>> {
    let (requests, receivers): (Vec<_>, Vec<_>) = puts
        .into_iter()
        .map(|(key, value)| {
            let (callback, receiver) = get_callback();
            let operation = AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                callback,
                key,
                value,
                should_overwrite,
            });
            (request(store_name, operation), receiver)
        })
        .unzip();
    run_batch(db, transaction, IndexedDBTxnMode::Readwrite, requests);
    receivers.iter().map(recv).collect()
}

/// The reply to a put that stored its record under the number `key`.
fn key_reply(key: f64) -> BackendResult<PutItemResult> {
    Ok(PutItemResult::Key(IndexedDBKeyType::Number(key)))
}

/// Runs a batch of `transaction` and waits until it has run.
fn run_batch<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    mode: IndexedDBTxnMode,
    requests: Vec<KvsOperation>,
) {
    let (done_sender, done_receiver) = mpsc::channel();
    db.process_transaction(
        txn(transaction),
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

/// Commits `transaction` and waits for the result.
fn commit<X: SqlExecutor>(db: &SqlEngine<X>, transaction: u64) -> BackendResult<()> {
    let (sender, receiver) = mpsc::channel();
    KvsEngine::commit(
        db,
        txn(transaction),
        Box::new(move |result| {
            let _ = sender.send(result);
        }),
    );
    receiver
        .recv_timeout(TIMEOUT)
        .expect("The commit should complete")
}

/// Aborts `transaction` and waits until it is rolled back.
fn abort<X: SqlExecutor>(db: &SqlEngine<X>, transaction: u64) {
    let (sender, receiver) = mpsc::channel();
    db.rollback(
        txn(transaction),
        Box::new(move || {
            let _ = sender.send(());
        }),
    );
    receiver
        .recv_timeout(TIMEOUT)
        .expect("The abort should complete");
}

/// The keys of the store `store_name`, read on `executor`.
fn keys_of<X: SqlExecutor>(executor: &mut X, store_name: &str) -> Vec<IndexedDBKeyType> {
    let store = queries::object_store(executor, store_name)
        .unwrap()
        .unwrap();
    queries::get_all_keys(executor, &store, &IndexedDBKeyRange::default(), None).unwrap()
}

fn number_keys(keys: impl IntoIterator<Item = i32>) -> Vec<IndexedDBKeyType> {
    keys.into_iter()
        .map(|key| IndexedDBKeyType::Number(key.into()))
        .collect()
}

/// The reply to reading `value` stored under the number `key`.
fn stored_value(key: f64, value: Vec<u8>) -> IndexedDBValue {
    IndexedDBValue {
        primary_key: IndexedDBKeyType::Number(key),
        value,
    }
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
    create_store(&db, store_name, None, false).expect("Failed to create store");
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
        1,
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
    assert_eq!(value, Some(stored_value(1.0, vec![13, 14, 15])));
    let value = recv(&get_item_none.1).unwrap();
    assert_eq!(value, None);
    let all_items = recv(&get_all_items.1).unwrap();
    assert_eq!(all_items.len(), 3);
    // Check that all three items are present
    assert!(all_items.contains(&stored_value(1.0, vec![13, 14, 15])));
    assert!(all_items.contains(&IndexedDBValue {
        primary_key: IndexedDBKeyType::String("2.0".to_string()),
        value: vec![4, 5, 6],
    }));
    assert!(all_items.contains(&IndexedDBValue {
        primary_key: IndexedDBKeyType::Array(vec![
            IndexedDBKeyType::String("3".to_string()),
            IndexedDBKeyType::Number(0.0),
        ]),
        value: vec![7, 8, 9],
    }));
    let amount = recv(&count.1).unwrap();
    assert_eq!(amount, 1);
    recv(&remove.1).unwrap();
    recv(&clear.1).unwrap();
    assert_eq!(commit(&db, 1), Ok(()));
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
        create_store(&db, store_name, None, false).expect("Failed to create store");
        let mut connection = db.connection();
        let store = queries::object_store(&mut *connection, store_name)
            .expect("Failed to get object store")
            .expect("The store should exist");

        for key in 1..=10 {
            queries::put_record(
                &mut *connection,
                &store,
                Some(IndexedDBKeyType::Number(key as f64)),
                &[key as u8],
                false,
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
    keys_of(&mut *db.connection(), store_name)
}

/// The version and the object store names in the database, also inside its open SQL
/// transaction, unlike the engine's metadata reads, which answer as of the last commit.
pub(crate) fn stored_version_and_store_names<X: SqlExecutor>(
    db: &SqlEngine<X>,
) -> (u64, Vec<String>) {
    let mut connection = db.connection();
    (
        queries::version(&mut *connection).unwrap(),
        queries::object_store_names(&mut *connection).unwrap(),
    )
}

/// A write that fails in the database aborts the transaction: everything it wrote is rolled back,
/// and its later requests and its commit fail.
fn test_failed_write_fails_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    create_store(&db, store_name, None, false).unwrap();
    for index_name in ["a", "b"] {
        create_index(
            &db,
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
        1,
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
    assert_eq!(
        recv(&put.1),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(1.0)))
    );
    assert!(matches!(recv(&rename.1), BackendError::DbErr(_)));
    assert!(recv(&put_after.1).is_err());

    let put_later = get_callback();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Versionchange,
        vec![put_request(
            store_name,
            put_later.0,
            IndexedDBKeyType::Number(3.0),
            vec![3],
            false,
        )],
    );
    assert!(recv(&put_later.1).is_err());
    assert!(commit(&db, 1).is_err());

    assert_eq!(stored_keys(&db, store_name), vec![]);
    let index_names: Vec<String> = db
        .indexes(store_name)
        .unwrap()
        .into_iter()
        .map(|index| index.name)
        .collect();
    assert_eq!(index_names, vec!["a".to_owned(), "b".to_owned()]);
}

/// A request for a store that does not exist fails without effects, so the transaction goes on.
fn test_missing_store_fails_only_its_request<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_name = "test_store";
    create_store(&db, store_name, None, false).unwrap();

    let missing = get_callback();
    let put = get_callback();
    run_batch(
        &db,
        1,
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
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(stored_keys(&db, store_name), number_keys([2]));
}

/// <https://w3c.github.io/IndexedDB/#key-generator-construct>: generated keys start at 1 and
/// count up; explicit number keys at or above the current number move it past their integer part,
/// other keys leave it alone.
fn test_key_generator_follows_generated_and_explicit_keys<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, true).unwrap();
    let number = |key: f64| Some(IndexedDBKeyType::Number(key));

    let replies = run_puts(
        &db,
        1,
        "test_store",
        vec![
            (None, vec![1]),
            (None, vec![2]),
            (number(1000.0), vec![3]),
            (None, vec![4]),
            (Some(IndexedDBKeyType::String("9999".to_owned())), vec![5]),
            (Some(IndexedDBKeyType::Date(9999.0)), vec![6]),
            (
                Some(IndexedDBKeyType::Array(vec![IndexedDBKeyType::Number(
                    9999.0,
                )])),
                vec![7],
            ),
            (None, vec![8]),
            (number(1002.5), vec![9]),
            (number(-5.0), vec![10]),
            (None, vec![11]),
            (number(1004.7), vec![12]),
            (None, vec![13]),
        ],
        false,
    );
    assert_eq!(replies[0], key_reply(1.0));
    assert_eq!(replies[1], key_reply(2.0));
    assert_eq!(replies[3], key_reply(1001.0));
    assert_eq!(replies[7], key_reply(1002.0));
    assert_eq!(replies[10], key_reply(1003.0));
    assert_eq!(replies[12], key_reply(1005.0));
    assert!(
        replies
            .iter()
            .all(|reply| matches!(reply, Ok(PutItemResult::Key(_))))
    );
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(db.key_generator_current_number("test_store"), Some(1006));
}

/// A key generator whose current number is above 2^53 fails to generate keys, which fails only
/// that request, stores nothing and leaves the current number as it is.
fn test_key_generator_generates_up_to_2_pow_53<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, true).unwrap();
    let max = 9_007_199_254_740_992.0;

    let replies = run_puts(
        &db,
        1,
        "test_store",
        vec![
            (Some(IndexedDBKeyType::Number(max - 1.0)), vec![1]),
            (None, vec![2]),
            (None, vec![3]),
            (Some(IndexedDBKeyType::Number(2.0)), vec![4]),
            (None, vec![5]),
        ],
        false,
    );
    assert_eq!(replies[1], key_reply(max));
    assert_eq!(replies[2], Ok(PutItemResult::KeyGeneratorExhausted));
    assert_eq!(replies[3], key_reply(2.0));
    assert_eq!(replies[4], Ok(PutItemResult::KeyGeneratorExhausted));
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(
        stored_keys(&db, "test_store"),
        vec![
            IndexedDBKeyType::Number(2.0),
            IndexedDBKeyType::Number(max - 1.0),
            IndexedDBKeyType::Number(max),
        ]
    );
    assert_eq!(
        db.key_generator_current_number("test_store"),
        Some((1 << 53) + 1)
    );
}

/// An explicit key of 2^53 or more uses up the key generator; a key below every current number
/// leaves it alone.
fn test_large_explicit_keys_and_the_key_generator<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let keys = [
        9_007_199_254_740_992.0,
        2f64.powi(60),
        2f64.powi(63),
        2f64.powi(64),
        2f64.powi(70),
        f64::INFINITY,
        -2f64.powi(60),
        -2f64.powi(64),
        -2f64.powi(70),
        f64::NEG_INFINITY,
    ];
    for (transaction, key) in (1..).zip(keys) {
        let store_name = format!("store {key}");
        create_store(&db, &store_name, None, true).unwrap();
        let replies = run_puts(
            &db,
            transaction,
            &store_name,
            vec![
                (Some(IndexedDBKeyType::Number(key)), vec![1]),
                (None, vec![2]),
            ],
            false,
        );
        assert_eq!(replies[0], key_reply(key));
        if key > 0.0 {
            assert_eq!(replies[1], Ok(PutItemResult::KeyGeneratorExhausted));
        } else {
            assert_eq!(replies[1], key_reply(1.0));
        }
        assert_eq!(commit(&db, transaction), Ok(()));
    }
}

/// An `add` of an existing key fails without changing anything, and the transaction goes on.
fn test_failed_add_changes_nothing<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, true).unwrap();
    let replies = run_puts(&db, 1, "test_store", vec![(None, vec![1])], false);
    assert_eq!(replies[0], key_reply(1.0));

    let replies = run_puts(
        &db,
        1,
        "test_store",
        vec![
            (Some(IndexedDBKeyType::Number(1.0)), vec![2]),
            (None, vec![3]),
        ],
        false,
    );
    assert_eq!(replies[0], Ok(PutItemResult::CannotOverwrite));
    assert_eq!(replies[1], key_reply(2.0));
    assert_eq!(commit(&db, 1), Ok(()));

    let get = get_callback();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![get_request(get.0, 1.0)],
    );
    assert_eq!(recv(&get.1), Ok(Some(stored_value(1.0, vec![1]))));
    assert_eq!(db.key_generator_current_number("test_store"), Some(3));
}

/// Aborting a transaction reverts what its generated and its explicit keys did to the key
/// generator.
fn test_abort_reverts_the_key_generator<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, true).unwrap();

    let replies = run_puts(
        &db,
        1,
        "test_store",
        vec![
            (None, vec![1]),
            (Some(IndexedDBKeyType::Number(10.0)), vec![2]),
            (None, vec![3]),
        ],
        false,
    );
    assert_eq!(replies[2], key_reply(11.0));
    abort(&db, 1);
    assert_eq!(db.key_generator_current_number("test_store"), Some(1));

    let replies = run_puts(&db, 2, "test_store", vec![(None, vec![4])], true);
    assert_eq!(replies[0], key_reply(1.0));
    assert_eq!(commit(&db, 2), Ok(()));
    assert_eq!(db.key_generator_current_number("test_store"), Some(2));
}

/// Aborting a transaction rolls back everything it did in all its batches, and for an upgrade
/// transaction also its schema changes and the version.
fn test_abort_rolls_back_data_schema_and_version<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    set_version(&db, 1).unwrap();
    for store_name in ["kept", "deleted"] {
        create_store(&db, store_name, None, false).unwrap();
    }
    create_index(
        &db,
        "kept",
        "by_a".to_owned(),
        KeyPath::String("a".to_owned()),
        false,
        false,
    )
    .unwrap();
    let seeds: Vec<_> = (0..2).map(|_| get_callback()).collect();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        ["kept", "deleted"]
            .into_iter()
            .zip(&seeds)
            .map(|(store_name, seed)| {
                put_request(
                    store_name,
                    seed.0.clone(),
                    IndexedDBKeyType::Number(1.0),
                    vec![1],
                    false,
                )
            })
            .collect(),
    );
    assert_eq!(commit(&db, 1), Ok(()));

    let (schema_callback, schema_errors) = get_callback::<BackendError>();
    let schema =
        |store_name: &str, operation| request(store_name, AsyncOperation::Schema(operation));
    let put = get_callback();
    db.upgrade_version(txn(2), 2);
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Versionchange,
        vec![
            put_request("kept", put.0, IndexedDBKeyType::Number(2.0), vec![2], false),
            schema(
                "created",
                AsyncSchemaOperation::CreateObjectStore {
                    callback: schema_callback.clone(),
                    key_path: None,
                    auto_increment: true,
                },
            ),
            schema(
                "kept",
                AsyncSchemaOperation::CreateIndex {
                    callback: schema_callback.clone(),
                    index_name: "by_b".to_owned(),
                    key_path: KeyPath::String("b".to_owned()),
                    unique: false,
                    multi_entry: false,
                },
            ),
            schema(
                "kept",
                AsyncSchemaOperation::RenameIndex {
                    callback: schema_callback.clone(),
                    index_name: "by_a".to_owned(),
                    new_name: "by_c".to_owned(),
                },
            ),
            schema(
                "deleted",
                AsyncSchemaOperation::DeleteObjectStore {
                    callback: schema_callback.clone(),
                },
            ),
        ],
    );
    let generated = get_callback();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Versionchange,
        vec![request(
            "created",
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                callback: generated.0,
                key: None,
                value: vec![3],
                should_overwrite: false,
            }),
        )],
    );
    assert!(recv(&put.1).is_ok());
    assert_eq!(
        recv(&generated.1),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(1.0)))
    );
    assert!(schema_errors.try_recv().is_err());
    let index_names = |db: &SqlEngine<X>| -> Vec<String> {
        queries::indexes(&mut *db.connection(), "kept")
            .unwrap()
            .into_iter()
            .map(|index| index.name)
            .collect()
    };
    let before = (1, vec!["kept".to_owned(), "deleted".to_owned()]);
    assert_eq!(
        stored_version_and_store_names(&db),
        (2, vec!["kept".to_owned(), "created".to_owned()])
    );
    assert_eq!(index_names(&db), vec!["by_c", "by_b"]);
    assert_eq!(stored_keys(&db, "kept"), number_keys([1, 2]));
    assert_eq!(
        (db.version().unwrap(), db.object_store_names().unwrap()),
        before
    );

    abort(&db, 2);

    assert_eq!(stored_version_and_store_names(&db), before);
    assert_eq!(
        (db.version().unwrap(), db.object_store_names().unwrap()),
        before
    );
    assert_eq!(index_names(&db), vec!["by_a"]);
    assert_eq!(
        db.indexes("kept")
            .unwrap()
            .into_iter()
            .map(|index| index.name)
            .collect::<Vec<_>>(),
        vec!["by_a"]
    );
    assert_eq!(stored_keys(&db, "kept"), number_keys([1]));
    assert_eq!(stored_keys(&db, "deleted"), number_keys([1]));
}

/// A transaction's batches all run in its one SQL transaction: another connection sees none of
/// its writes before the commit and all of them after it, and an abort leaves none.
fn test_transaction_spanning_batches_commits_atomically<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, false).unwrap();
    let mut other = db.connection().connector()().unwrap();

    let put_batches = |transaction: u64, keys: std::ops::RangeInclusive<i32>, other: &mut X| {
        for key in keys {
            let put = get_callback();
            run_batch(
                &db,
                transaction,
                IndexedDBTxnMode::Readwrite,
                vec![put_request(
                    "test_store",
                    put.0,
                    IndexedDBKeyType::Number(key.into()),
                    vec![key as u8],
                    false,
                )],
            );
            assert!(recv(&put.1).is_ok());
            assert_eq!(keys_of(other, "test_store"), vec![]);
        }
    };

    put_batches(1, 1..=10, &mut other);
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(keys_of(&mut other, "test_store"), number_keys(1..=10));

    let mut other_after_commit = db.connection().connector()().unwrap();
    for key in 11..=20 {
        let put = get_callback();
        run_batch(
            &db,
            2,
            IndexedDBTxnMode::Readwrite,
            vec![put_request(
                "test_store",
                put.0,
                IndexedDBKeyType::Number(key.into()),
                vec![key as u8],
                false,
            )],
        );
        assert!(recv(&put.1).is_ok());
    }
    abort(&db, 2);
    assert_eq!(
        keys_of(&mut other_after_commit, "test_store"),
        number_keys(1..=10)
    );
    assert_eq!(stored_keys(&db, "test_store"), number_keys(1..=10));
}

/// A callback that reports `id` on `order` when it is called.
fn tagged<T>(order: &mpsc::Sender<u32>, id: u32) -> GenericCallback<T>
where
    T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
{
    let order = order.clone();
    GenericCallback::new(move |_| {
        let _ = order.send(id);
    })
    .unwrap()
}

/// Requests run in the order they were handed over: those of one transaction, and those of
/// readonly transactions that run alongside a writer.
fn test_requests_run_in_order<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    for store_name in ["written", "read"] {
        create_store(&db, store_name, None, false).unwrap();
    }
    let (order_sender, order) = mpsc::channel();
    let put = |id: u32, key: f64, value: u8| {
        put_request(
            "written",
            tagged(&order_sender, id),
            IndexedDBKeyType::Number(key),
            vec![value],
            true,
        )
    };
    let read = |id: u32| {
        request(
            "read",
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                callback: tagged(&order_sender, id),
                key_range: IndexedDBKeyRange::default(),
            }),
        )
    };
    let batches = [
        (
            1,
            IndexedDBTxnMode::Readwrite,
            vec![put(0, 1.0, 1), put(1, 2.0, 2)],
        ),
        (2, IndexedDBTxnMode::Readonly, vec![read(2)]),
        (1, IndexedDBTxnMode::Readwrite, vec![put(3, 3.0, 3)]),
        (3, IndexedDBTxnMode::Readonly, vec![read(4), read(5)]),
        (1, IndexedDBTxnMode::Readwrite, vec![put(6, 1.0, 9)]),
    ];
    let (done_sender, done) = mpsc::channel();
    let batch_count = batches.len();
    for (transaction, mode, requests) in batches {
        let done_sender = done_sender.clone();
        db.process_transaction(
            txn(transaction),
            KvsTransaction {
                mode,
                requests: VecDeque::from(requests),
            },
            Box::new(move || {
                let _ = done_sender.send(());
            }),
        );
    }
    for _ in 0..batch_count {
        done.recv_timeout(TIMEOUT)
            .expect("The batch should complete");
    }
    let replies: Vec<u32> = (0..7)
        .map(|_| {
            order
                .recv_timeout(TIMEOUT)
                .expect("The request should reply")
        })
        .collect();
    assert_eq!(replies, (0..7).collect::<Vec<_>>());
    assert_eq!(commit(&db, 1), Ok(()));

    let value = get_callback();
    run_batch(
        &db,
        4,
        IndexedDBTxnMode::Readonly,
        vec![request(
            "written",
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                callback: value.0,
                key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
            }),
        )],
    );
    assert_eq!(recv(&value.1), Ok(Some(stored_value(1.0, vec![9]))));
}

/// Each database has its own actor: one whose connection is busy, with a write transaction
/// open, doesn't hold up another database's transactions.
fn test_two_databases_progress_independently<X: SqlExecutor>() {
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let [busy, other] = dirs.each_ref().map(|dir| {
        let db = open_engine::<X>(dir);
        create_store(&db, "test_store", None, false).unwrap();
        db
    });
    let put_one = |db: &SqlEngine<X>| {
        let put = get_callback();
        run_batch(
            db,
            1,
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
    };

    put_one(&busy);
    let (release, released) = mpsc::channel::<()>();
    busy.spawn_on_connection(move |_| {
        let _ = released.recv_timeout(TIMEOUT);
    });
    let get = get_callback();
    busy.process_transaction(
        txn(2),
        KvsTransaction {
            mode: IndexedDBTxnMode::Readonly,
            requests: VecDeque::from([get_request(get.0, 1.0)]),
        },
        Box::new(|| {}),
    );

    put_one(&other);
    assert_eq!(commit(&other, 1), Ok(()));
    assert!(
        get.1.try_recv().is_err(),
        "A request waits for the jobs handed over to its database before it"
    );

    release.send(()).unwrap();
    assert_eq!(recv(&get.1), Ok(Some(stored_value(1.0, vec![1]))));
    assert_eq!(commit(&busy, 1), Ok(()));
    assert_eq!(stored_keys(&busy, "test_store"), number_keys([1]));
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
pub(crate) struct Faults {
    /// Fail every `begin`.
    begin: bool,
    /// Fail every `commit`, after rolling the transaction back.
    pub(crate) commit: bool,
    /// Fail the statement or query whose SQL contains this text, after letting the given number
    /// of them through, and end the open transaction the way SQLite does after SQLITE_FULL.
    statement: Option<(&'static str, usize)>,
    /// Leave the database in place on delete, as RDB does when it can't remove the files.
    keep_on_delete: bool,
}

/// The faults of the [`Faulty`] executors opened on each directory.
static FAULTS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<Faults>>>>> =
    LazyLock::new(Default::default);

pub(crate) fn faults_for(directory: &Path) -> Arc<Mutex<Faults>> {
    FAULTS
        .lock()
        .unwrap()
        .entry(directory.to_owned())
        .or_default()
        .clone()
}

/// An executor that wraps another and fails where the [`Faults`] of its directory say.
pub(crate) struct Faulty<X> {
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

    fn delete(self) -> Result<(), SqlError> {
        if self.faults.lock().unwrap().keep_on_delete {
            return Ok(());
        }
        self.inner.delete()
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
    create_store(&db, "test_store", None, false).unwrap();
    let put = get_callback();
    run_batch(
        &db,
        0,
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
    assert_eq!(commit(&db, 0), Ok(()));
    db
}

fn get_request(
    callback: GenericCallback<BackendResult<Option<IndexedDBValue>>>,
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

fn put_requests(
    puts: &[Callback<BackendResult<PutItemResult>>],
    first_key: i32,
) -> Vec<KvsOperation> {
    puts.iter()
        .zip(first_key..)
        .map(|(put, key)| {
            put_request(
                "test_store",
                put.0.clone(),
                IndexedDBKeyType::Number(key.into()),
                vec![1],
                false,
            )
        })
        .collect()
}

/// If the SQL transaction can't begin, the transaction fails: each of its requests fails once,
/// also those that come after the fault is gone, and so does its commit.
fn test_begin_failure_fails_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    faults_for(dir.path()).lock().unwrap().begin = true;

    let put = get_callback();
    let get = get_callback();
    run_batch(
        &db,
        1,
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
    let later = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&later, 3));
    assert!(recv(&later[0].1).is_err());
    assert!(commit(&db, 1).is_err());
    assert_eq!(stored_keys(&db, "test_store"), number_keys([1]));
}

/// A statement that ends the SQL transaction, as SQLite does after SQLITE_FULL, fails the
/// transaction: its writes so far are rolled back, and its later requests and its commit fail. A
/// readonly transaction has nothing to roll back, so only its failing request fails.
fn test_statement_failure_that_ends_the_transaction_fails_it<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);

    faults_for(dir.path()).lock().unwrap().statement = Some(("INSERT INTO object_data", 1));
    let puts: Vec<_> = (0..3).map(|_| get_callback()).collect();
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&puts, 2));
    assert!(recv(&puts[0].1).is_ok());
    assert!(recv(&puts[1].1).is_err());
    assert!(recv(&puts[2].1).is_err());
    assert!(commit(&db, 1).is_err());
    assert_eq!(stored_keys(&db, "test_store"), number_keys([1]));

    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_data", 1));
    let gets: Vec<_> = (0..3).map(|_| get_callback()).collect();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        gets.iter()
            .map(|get| get_request(get.0.clone(), 1.0))
            .collect(),
    );
    assert_eq!(recv(&gets[0].1), Ok(Some(stored_value(1.0, vec![1]))));
    assert!(recv(&gets[1].1).is_err());
    assert_eq!(recv(&gets[2].1), Ok(Some(stored_value(1.0, vec![1]))));
    assert_eq!(commit(&db, 2), Ok(()));
}

/// A readonly request runs inside the open SQL transaction of a writer with a disjoint scope. If
/// it fails in a way that ends that SQL transaction, the writer's changes are gone, so the writer
/// fails, not only the readonly request.
fn test_readonly_failure_that_ends_the_sql_transaction_fails_the_writer<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    create_store(&db, "other", None, false).unwrap();

    let puts = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&puts, 2));
    assert!(recv(&puts[0].1).is_ok());

    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_data", 0));
    let get = get_callback();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![request(
            "other",
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                callback: get.0,
                key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
            }),
        )],
    );
    assert!(recv(&get.1).is_err());
    assert_eq!(commit(&db, 2), Ok(()));

    let later = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&later, 3));
    assert!(recv(&later[0].1).is_err());
    assert!(commit(&db, 1).is_err());
    assert_eq!(stored_keys(&db, "test_store"), number_keys([1]));
}

/// If the commit fails, the transaction's writes are rolled back and the commit reports the
/// error; the requests had replied as they ran. A readonly transaction has nothing to commit.
fn test_commit_failure_rolls_back<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    faults_for(dir.path()).lock().unwrap().commit = true;

    let puts: Vec<_> = (0..2).map(|_| get_callback()).collect();
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&puts, 2));
    for put in &puts {
        assert!(recv(&put.1).is_ok());
    }
    assert!(matches!(commit(&db, 1), Err(BackendError::DbErr(_))));

    let get = get_callback();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![get_request(get.0, 1.0)],
    );
    assert_eq!(recv(&get.1), Ok(Some(stored_value(1.0, vec![1]))));
    assert_eq!(commit(&db, 2), Ok(()));

    faults_for(dir.path()).lock().unwrap().commit = false;
    assert_eq!(stored_keys(&db, "test_store"), number_keys([1]));
}

/// Batches reply in the order the manager hands them over, also those of overlapping readonly
/// transactions, which ran on parallel connections before and could overtake each other.
fn test_batches_reply_in_dispatch_order<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, false).unwrap();

    let (order_sender, order) = mpsc::channel();
    let (done_sender, done) = mpsc::channel();
    for id in 0..8 {
        let done_sender = done_sender.clone();
        db.process_transaction(
            txn(id.into()),
            KvsTransaction {
                mode: IndexedDBTxnMode::Readonly,
                requests: VecDeque::from([get_request(tagged(&order_sender, id), 1.0)]),
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
    let replies: Vec<u32> = (0..8)
        .map(|_| {
            order
                .recv_timeout(TIMEOUT)
                .expect("The request should reply")
        })
        .collect();
    assert_eq!(replies, (0..8).collect::<Vec<_>>());
}

/// Runs a put of key 2 in the readwrite transaction 1 of a [`Faulty`] engine, lets `between` end
/// the SQL transaction outside of the transaction's requests, and checks that the transaction
/// has failed: its next put fails rather than running in autocommit, so after its abort only
/// the key 1 of [`open_faulty_engine`] is left.
fn check_writer_fails_after<X: SqlExecutor>(between: impl FnOnce(&SqlEngine<Faulty<X>>)) {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    let puts = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&puts, 2));
    assert!(recv(&puts[0].1).is_ok());

    // The fault clears itself when it fires.
    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_store", 0));
    between(&db);

    let later = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&later, 3));
    assert!(recv(&later[0].1).is_err());
    assert!(commit(&db, 1).is_err());
    abort(&db, 1);
    assert_eq!(stored_keys(&db, "test_store"), number_keys([1]));
}

/// Any job on the connection, not only a request, can end the writer's SQL transaction.
fn test_job_that_ends_the_sql_transaction_fails_the_writer<X: SqlExecutor>() {
    check_writer_fails_after::<X>(|db| {
        db.spawn_on_connection(|executor| {
            let _ = queries::key_path(executor, "test_store");
        })
    });
}

/// The manager's metadata reads answer from the committed metadata, so they neither wait for
/// the work handed over before nor run statements inside the writer's SQL transaction.
fn test_metadata_reads_do_not_wait_for_the_connection<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let key_path = Some(KeyPath::String("id".to_owned()));
    create_store(&db, "test_store", key_path.clone(), true).unwrap();
    let (release, released) = mpsc::channel::<()>();
    let (timed_out_sender, timed_out) = mpsc::channel();
    db.spawn_on_connection(move |_| {
        let _ = timed_out_sender.send(released.recv_timeout(TIMEOUT).is_err());
    });

    assert_eq!(db.version().unwrap(), 0);
    assert_eq!(db.object_store_names().unwrap(), vec!["test_store"]);
    assert_eq!(db.key_path("test_store"), key_path);
    assert!(db.has_key_generator("test_store"));
    assert!(db.indexes("test_store").unwrap().is_empty());
    release.send(()).unwrap();
    assert!(
        !timed_out.recv().unwrap(),
        "A metadata read waited for the busy connection"
    );
}

/// An upgrade transaction without requests sets the version when it commits, and one that
/// aborts leaves it alone.
fn test_upgrade_without_requests_sets_the_version_at_commit<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    db.upgrade_version(txn(1), 2);
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(queries::version(&mut *db.connection()).unwrap(), 2);
    assert_eq!(db.version().unwrap(), 2);

    db.upgrade_version(txn(2), 3);
    abort(&db, 2);
    assert_eq!(db.version().unwrap(), 2);
    let mut connection = db.connection();
    assert_eq!(queries::version(&mut *connection).unwrap(), 2);
    assert!(!connection.in_transaction());
}

/// A write transaction stays open across minutes in which script sends nothing, past the 150 s
/// after which RDB closes unused pooled transaction connections, and then commits all its
/// writes at once. Takes more than three minutes, so it runs only with `--run-ignored only` and
/// a nextest slow-timeout above 200 s.
fn transaction_holds_across_idle_minutes<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "test_store", None, false).unwrap();
    let mut other = db.connection().connector()().unwrap();
    let start = Instant::now();
    for (key, idle_seconds) in [(1, 160), (2, 20), (3, 0)] {
        let put = get_callback();
        run_batch(
            &db,
            1,
            IndexedDBTxnMode::Readwrite,
            vec![put_request(
                "test_store",
                put.0,
                IndexedDBKeyType::Number(key.into()),
                vec![1],
                false,
            )],
        );
        assert!(recv(&put.1).is_ok());
        assert_eq!(keys_of(&mut other, "test_store"), vec![]);
        std::thread::sleep(Duration::from_secs(idle_seconds));
    }
    assert_eq!(commit(&db, 1), Ok(()));
    assert!(start.elapsed() > Duration::from_secs(180));
    assert_eq!(keys_of(&mut other, "test_store"), number_keys(1..=3));
}

#[cfg(feature = "sqlite-backend")]
#[test]
#[ignore]
fn sqlite_transaction_holds_across_idle_minutes() {
    transaction_holds_across_idle_minutes::<super::sqlite::SqliteExecutor>();
}

#[cfg(ohos_rdb)]
#[test]
#[ignore]
fn rdb_transaction_holds_across_idle_minutes() {
    transaction_holds_across_idle_minutes::<super::rdb::RdbExecutor>();
}

/// Deleting the store would close it under the other executors on it.
#[cfg(ohos_rdb)]
#[test]
fn rdb_delete_refuses_while_other_executors_exist() {
    use super::rdb::RdbExecutor;

    let dir = tempfile::tempdir().unwrap();
    let executor = RdbExecutor::open(dir.path()).unwrap();
    let other = executor.connector()().unwrap();
    assert!(matches!(
        executor.delete(),
        Err(SqlError::TransactionState(_))
    ));
    drop(other);
    RdbExecutor::open(dir.path()).unwrap().delete().unwrap();
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
