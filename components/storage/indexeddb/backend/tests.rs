/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Tests of the executors and of the engine, run on every executor compiled in.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::time::{Duration, Instant};

use profile_traits::generic_callback::GenericCallback;
use servo_base::threadpool::ThreadPool;
use servo_url::ImmutableOrigin;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, AsyncSchemaOperation,
    BackendError, BackendResult, ConnectionEvent, IndexPopulationEvent, IndexPopulationStep,
    IndexedDBCursorDirection, IndexedDBCursorStep, IndexedDBIndex, IndexedDBIndexKey,
    IndexedDBIndexKeys, IndexedDBKeyRange, IndexedDBKeyType, IndexedDBObjectStore, IndexedDBRecord,
    IndexedDBRecordIndexKeys, IndexedDBTxnId, IndexedDBTxnMode, IndexedDBValue, KeyPath,
    PutItemResult, RequestOutput, TransactionEvent, Utf16String,
};
use tempfile::TempDir;
use url::Host;
use uuid::Uuid;

use super::engine::SqlEngine;
use super::executor::{
    Connector, OnRow, SqlError, SqlExecutor, SqlParam, TransactionBehavior, query_row, query_rows,
    with_transaction,
};
use super::queries::{self, RecordSource};
use super::schema::{self, SCHEMA_VERSION};
use crate::indexeddb::engines::{
    KvsEngine, KvsOperation, KvsTransaction, TransactionResults, encoding,
};
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
    test_unpaired_surrogates_in_names_and_keys,
    test_key_path,
    test_delete_store,
    test_delete_store_removes_store_records,
    test_metadata_snapshot_and_renames,
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
    test_invalid_schema_operation_fails_the_transaction,
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
    test_index_records_follow_puts_and_overwrites,
    test_deleted_records_take_their_index_records_along,
    test_deleted_indexes_take_their_records_along,
    test_unique_index_conflict_changes_nothing,
    test_placeholders_become_the_record_key,
    test_abort_reverts_index_records,
    test_index_keys_for_other_indexes_fail_the_transaction,
    test_population_gives_the_index_records_that_puts_give,
    test_population_unique_conflict_fails_the_upgrade,
    test_population_of_a_unique_multi_entry_index,
    test_requests_after_create_index_wait_for_the_population,
    test_waiting_put_sees_the_populated_unique_index,
    test_indexes_created_one_after_another_populate_in_turn,
    test_abort_during_population_leaves_nothing,
    test_commit_of_a_transaction_that_failed_while_populating,
    test_population_of_an_empty_store_and_a_step_out_of_turn,
    test_population_batches_are_limited_in_size,
    test_population_finds_unique_conflicts_in_every_check,
    test_index_reads_go_in_key_then_primary_key_order,
    test_index_reads_see_the_writes_before_them,
    test_index_reads_after_create_index_see_the_whole_index,
    test_cursor_steps_find_what_the_spec_finds,
    test_cursor_steps_see_the_writes_between_them,
    test_get_all_retrieves_what_a_cursor_visits,
);

const TIMEOUT: Duration = Duration::from_secs(10);

/// The id of the first store created in a database, which the tests name "test_store".
const TEST_STORE: i64 = 1;

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
        name: "test_db".into(),
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

/// The id after the largest in the `id` column of `table`, as script gives a new store or index.
fn next_id<X: SqlExecutor>(executor: &mut X, table: &str) -> Result<i64, SqlError> {
    Ok(query_row(
        executor,
        &format!("SELECT MAX(id) FROM {table}"),
        &[],
        |row| row.optional_integer(0),
    )?
    .flatten()
    .unwrap_or(0) +
        1)
}

/// Creates the store `store_name` with the next id, and returns the id.
fn create_store<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store_name: &str,
    key_path: Option<KeyPath>,
    auto_increment: bool,
) -> Result<i64, SqlError> {
    in_own_transaction(db, |executor| {
        let id = next_id(executor, "object_store")?;
        queries::create_object_store(
            executor,
            id,
            &store_name.into(),
            key_path.as_ref(),
            auto_increment,
        )?;
        Ok(id)
    })
}

fn delete_store<X: SqlExecutor>(db: &SqlEngine<X>, store: i64) -> Result<(), SqlError> {
    in_own_transaction(db, |executor| queries::delete_object_store(executor, store))
}

/// Creates the index `index_name` of `store` with the next id, and returns the id.
fn create_index<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store: i64,
    index_name: String,
    key_path: KeyPath,
    unique: bool,
    multi_entry: bool,
) -> Result<i64, SqlError> {
    in_own_transaction(db, |executor| {
        let id = next_id(executor, "object_store_index")?;
        queries::create_index(
            executor,
            store,
            id,
            &index_name.into(),
            &key_path,
            unique,
            multi_entry,
        )?;
        Ok(id)
    })
}

/// The names of the stores in the engine's committed metadata.
fn store_names<X: SqlExecutor>(db: &SqlEngine<X>) -> Vec<String> {
    db.object_stores()
        .into_iter()
        .map(|store| store.name.to_string())
        .collect()
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
    assert!(store_names(&db).is_empty());
}

fn schema_version_match_keeps_the_database<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine_in::<X>(dir.path(), true);
    create_store(&db, "store", None, false).unwrap();
    set_version(&db, 3).unwrap();
    drop(db);

    let db = open_engine_in::<X>(dir.path(), false);
    assert!(!db.created_db_path());
    assert_eq!(store_names(&db), vec!["store".to_owned()]);
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
        assert!(store_names(&db).is_empty());
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
    assert!(store_names(&db).is_empty());
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
    let store = create_store(&db, "test_store", None, true).unwrap();
    assert_eq!(store, 1);
    // A second store of the same name, or one with an id that isn't new, is refused.
    assert!(matches!(
        create_store(&db, "test_store", None, false),
        Err(SqlError::InvalidMetadata(_))
    ));
    let reused_id = in_own_transaction(&db, |executor| {
        queries::create_object_store(executor, store, &"other".into(), None, false)
    });
    assert!(matches!(reused_id, Err(SqlError::InvalidMetadata(_))));
    // Ensure store was not overwritten
    assert!(db.key_generator_current_number(store).is_some());
    assert_eq!(store_names(&db), vec!["test_store".to_owned()]);
}

fn test_create_store_empty_name<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    create_store(&db, "", None, true).unwrap();
    assert_eq!(store_names(&db), vec![String::new()]);
}

fn test_injection<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    // Create a normal store
    let store_name1 = "test_store";
    create_store(&db, store_name1, None, true).unwrap();
    // Injection
    let store_name2 = "' OR 1=1 -- -";
    create_store(&db, store_name2, None, false).unwrap();
    assert_eq!(
        store_names(&db),
        vec![store_name1.to_owned(), store_name2.to_owned()]
    );
}

fn units(units: &[u16]) -> Utf16String {
    Utf16String::from_units(units.to_vec())
}

/// Database, store and index names keep their unpaired surrogates, also after reopening, and so
/// do string keys, which sort by code unit.
fn test_unpaired_surrogates_in_names_and_keys<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let description = IndexedDBDescription {
        name: units(&[0xDC00, 0xD800]),
        origin: test_origin(),
    };
    let open = |created| {
        SqlEngine::<X>::new(
            dir.path().to_path_buf(),
            created,
            &description,
            ThreadPool::global(),
        )
        .unwrap()
    };
    let db = open(true);
    let lead = units(&[0xD800]);
    let trail = units(&[0xDC00]);
    let replacement = Utf16String::from("\u{FFFD}");
    in_own_transaction(&db, |executor| {
        // Names that are equal once each unpaired surrogate is replaced are different names.
        for (name, id) in [&lead, &trail, &replacement].into_iter().zip(1..) {
            queries::create_object_store(executor, id, name, None, false)?;
        }
        queries::rename_object_store(executor, 3, &units(&[0x61, 0xDFFF]))?;
        let key_path = KeyPath::String("a".to_owned());
        queries::create_index(executor, 1, 1, &trail, &key_path, false, false)?;
        queries::create_index(executor, 1, 2, &replacement, &key_path, false, false)?;
        queries::rename_index(executor, 1, 2, &lead)
    })
    .unwrap();
    assert!(
        in_own_transaction(&db, |executor| {
            queries::create_object_store(executor, 4, &trail, None, false)
        })
        .is_err()
    );
    let names = |db: &SqlEngine<X>| {
        let stores = db.object_stores();
        let index_names: Vec<_> = stores[0]
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect();
        (
            stores
                .into_iter()
                .map(|store| store.name)
                .collect::<Vec<_>>(),
            index_names,
        )
    };
    let expected = (
        vec![lead.clone(), trail.clone(), units(&[0x61, 0xDFFF])],
        vec![trail.clone(), lead.clone()],
    );
    assert_eq!(names(&db), expected);

    let puts = [
        units(&[0xDC00]),
        units(&[0xD834, 0xDD1E]),
        replacement,
        units(&[0xD800]),
        units(&[0x61, 0xDFFF]),
        units(&[0xDC00, 0xD800]),
    ]
    .into_iter()
    .map(|key| {
        let key = IndexedDBKeyType::String(key);
        let index_keys = vec![index_keys(1, [key.clone()]), index_keys(2, [])];
        (Some(key), vec![], index_keys)
    })
    .collect();
    assert!(
        run_indexed_puts(&db, 1, 1, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert!(commit(&db, 1).is_ok());
    let sorted: Vec<_> = [
        &[0x61, 0xDFFF][..],
        &[0xD800],
        &[0xD834, 0xDD1E],
        &[0xDC00],
        &[0xDC00, 0xD800],
        &[0xFFFD],
    ]
    .into_iter()
    .map(|key| IndexedDBKeyType::String(units(key)))
    .collect();
    assert!(sorted.is_sorted_by(|a, b| a < b));
    assert_eq!(stored_keys(&db, 1), sorted);
    let by_trail: Vec<_> = sorted
        .iter()
        .map(|key| (key.clone(), key.clone()))
        .collect();
    assert_eq!(index_records_of(&db, 1, 1), by_trail);
    let range = IndexedDBKeyRange::new(
        Some(IndexedDBKeyType::String(lead.clone())),
        Some(IndexedDBKeyType::String(trail.clone())),
        true,
        false,
    );
    let mut connection = db.connection();
    let store = queries::object_store(&mut *connection, 1).unwrap().unwrap();
    assert_eq!(
        queries::get_all_keys(
            &mut *connection,
            RecordSource::ObjectStore(&store),
            &range,
            IndexedDBCursorDirection::Next,
            None
        )
        .unwrap(),
        vec![
            IndexedDBKeyType::String(units(&[0xD834, 0xDD1E])),
            IndexedDBKeyType::String(trail),
        ]
    );
    let stored_name = query_row(&mut *connection, "SELECT name FROM database", &[], |row| {
        row.blob(0)
    })
    .unwrap();
    assert_eq!(stored_name, Some(description.name.to_be_bytes()));
    drop(connection);
    drop(db);

    let db = open(false);
    assert_eq!(names(&db), expected);
    assert_eq!(stored_keys(&db, 1), sorted);
}

fn test_key_path<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let key_path = Some(KeyPath::String("test".to_string()));
    let store = create_store(&db, "test_store", key_path.clone(), true).unwrap();
    let stores = db.object_stores();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].id, store);
    assert_eq!(stores[0].key_path, key_path);
    assert!(stores[0].has_key_generator);
}

fn test_delete_store<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "test_store", None, false).expect("Failed to create store");
    // Delete the store
    delete_store(&db, store).expect("Failed to delete store");
    // A store that doesn't exist can't be deleted.
    assert!(delete_store(&db, store).is_err());
    assert!(store_names(&db).is_empty());
}

/// The metadata snapshot has every store and index with its id, in the order of creation; a
/// renamed store keeps its id, records and indexes, and a deleted index takes its records along.
fn test_metadata_snapshot_and_renames<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let books = create_store(&db, "books", Some(KeyPath::String("isbn".to_owned())), true).unwrap();
    let authors = create_store(&db, "authors", None, false).unwrap();
    let by_title = create_index(
        &db,
        books,
        "by_title".to_owned(),
        KeyPath::String("title".to_owned()),
        true,
        false,
    )
    .unwrap();
    let by_name = create_index(
        &db,
        authors,
        "by_name".to_owned(),
        KeyPath::Sequence(vec!["first".to_owned(), "last".to_owned()]),
        false,
        false,
    )
    .unwrap();
    let by_author = create_index(
        &db,
        books,
        "by_author".to_owned(),
        KeyPath::String("author".to_owned()),
        false,
        true,
    )
    .unwrap();
    assert_eq!((books, authors), (1, 2));
    assert_eq!((by_title, by_name, by_author), (1, 2, 3));
    let index = |id, name: &str, key_path: KeyPath, unique, multi_entry| IndexedDBIndex {
        id,
        name: name.into(),
        key_path,
        unique,
        multi_entry,
    };
    assert_eq!(
        db.object_stores(),
        vec![
            IndexedDBObjectStore {
                id: books,
                name: "books".into(),
                key_path: Some(KeyPath::String("isbn".to_owned())),
                has_key_generator: true,
                indexes: vec![
                    index(
                        by_title,
                        "by_title",
                        KeyPath::String("title".to_owned()),
                        true,
                        false
                    ),
                    index(
                        by_author,
                        "by_author",
                        KeyPath::String("author".to_owned()),
                        false,
                        true
                    ),
                ],
            },
            IndexedDBObjectStore {
                id: authors,
                name: "authors".into(),
                key_path: None,
                has_key_generator: false,
                indexes: vec![index(
                    by_name,
                    "by_name",
                    KeyPath::Sequence(vec!["first".to_owned(), "last".to_owned()]),
                    false,
                    false
                )],
            },
        ]
    );

    in_own_transaction(&db, |executor| {
        let store = queries::object_store(executor, books)?.unwrap();
        let index_keys = vec![
            index_keys(by_title, []),
            index_keys(by_author, [IndexedDBKeyType::Number(1.0)]),
        ];
        queries::put_record(executor, &store, None, &[7], false, index_keys)?;
        Ok(())
    })
    .unwrap();

    // Swap the names of the two stores and of two indexes, as script does it.
    in_own_transaction(&db, |executor| {
        queries::rename_object_store(executor, books, &"tmp".into())?;
        queries::rename_object_store(executor, authors, &"books".into())?;
        queries::rename_object_store(executor, books, &"authors".into())?;
        queries::rename_index(executor, books, by_title, &"tmp".into())?;
        queries::rename_index(executor, books, by_author, &"by_title".into())?;
        queries::rename_index(executor, books, by_title, &"by_author".into())
    })
    .unwrap();
    let stores = db.object_stores();
    assert_eq!(
        (
            stores[0].id,
            stores[0].name.to_string(),
            stores[1].name.to_string()
        ),
        (books, "authors".to_owned(), "books".to_owned())
    );
    assert_eq!(
        stores[0]
            .indexes
            .iter()
            .map(|index| (index.id, index.name.to_string()))
            .collect::<Vec<_>>(),
        vec![
            (by_title, "by_author".to_owned()),
            (by_author, "by_title".to_owned())
        ]
    );
    assert_eq!(stored_keys(&db, books), number_keys([1]));
    assert_eq!(db.key_generator_current_number(books), Some(2));

    // A name in use, a missing store or index, and an index of another store are refused.
    for result in [
        in_own_transaction(&db, |executor| {
            queries::rename_object_store(executor, books, &"books".into())
        }),
        in_own_transaction(&db, |executor| {
            queries::rename_object_store(executor, 9, &"other".into())
        }),
        in_own_transaction(&db, |executor| {
            queries::rename_index(executor, books, by_title, &"by_title".into())
        }),
        in_own_transaction(&db, |executor| {
            queries::rename_index(executor, authors, by_title, &"other".into())
        }),
        in_own_transaction(&db, |executor| {
            queries::delete_index(executor, authors, by_title)
        }),
        in_own_transaction(&db, |executor| {
            queries::create_index(
                executor,
                books,
                by_author,
                &"new".into(),
                &KeyPath::String("new".to_owned()),
                false,
                false,
            )
        }),
        in_own_transaction(&db, |executor| {
            queries::create_index(
                executor,
                9,
                9,
                &"new".into(),
                &KeyPath::String("new".to_owned()),
                false,
                false,
            )
        }),
    ] {
        assert!(result.is_err(), "{result:?}");
    }

    in_own_transaction(&db, |executor| {
        queries::delete_index(executor, books, by_author)
    })
    .unwrap();
    let index_rows = query_row(
        &mut *db.connection(),
        "SELECT COUNT(*) FROM index_data WHERE index_id = ?",
        &[SqlParam::Integer(by_author)],
        |row| row.integer(0),
    )
    .unwrap();
    assert_eq!(index_rows, Some(0));
    assert_eq!(
        db.object_stores()[0]
            .indexes
            .iter()
            .map(|index| index.id)
            .collect::<Vec<_>>(),
        vec![by_title]
    );
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

    let store = create_store(&db, "test_store", None, false).expect("Failed to create store");
    let store_id = {
        let mut connection = db.connection();
        let object_store = queries::object_store(&mut *connection, store)
            .expect("Failed to fetch store metadata")
            .expect("The store should exist");
        queries::put_record(
            &mut *connection,
            &object_store,
            Some(IndexedDBKeyType::Number(1.0)),
            &[1, 2, 3],
            true,
            vec![],
        )
        .expect("Failed to insert item");
        assert_eq!(record_count(&mut *connection, object_store.id), 1);
        object_store.id
    };

    delete_store(&db, store).expect("Failed to delete store");

    assert_eq!(record_count(&mut *db.connection(), store_id), 0);
}

/// The result of a request, as the kind of operation reports it.
trait TestReply: Sized + Send + 'static {
    fn from_result(result: BackendResult<RequestOutput>) -> Self;
}

macro_rules! test_reply {
    ($type:ty, $variant:ident) => {
        impl TestReply for BackendResult<$type> {
            fn from_result(result: BackendResult<RequestOutput>) -> Self {
                result.map(|output| match output {
                    RequestOutput::$variant(value) => value,
                    other => panic!("unexpected request output {other:?}"),
                })
            }
        }
    };
}
test_reply!(Option<IndexedDBKeyType>, Key);
test_reply!(Option<IndexedDBValue>, Value);
test_reply!(Vec<IndexedDBKeyType>, Keys);
test_reply!(Vec<IndexedDBValue>, Values);
test_reply!(u64, Count);
test_reply!(Vec<IndexedDBRecord>, Records);
test_reply!(PutItemResult, Put);

/// The records of [`AsyncReadOnlyOperation::GetAllRecords`].
#[derive(Debug, PartialEq)]
struct AllRecords(Vec<IndexedDBRecord>);

impl TestReply for BackendResult<AllRecords> {
    fn from_result(result: BackendResult<RequestOutput>) -> Self {
        result.map(|output| match output {
            RequestOutput::AllRecords(records) => AllRecords(records),
            other => panic!("unexpected request output {other:?}"),
        })
    }
}

impl TestReply for BackendResult<()> {
    fn from_result(result: BackendResult<RequestOutput>) -> Self {
        result.map(|output| assert_eq!(output, RequestOutput::Done))
    }
}

type ReplyHandler = Box<dyn FnOnce(BackendResult<RequestOutput>) + Send>;

/// Where the result of each test request goes, by request id. Request ids are unique in the
/// test process.
static REPLY_HANDLERS: LazyLock<Mutex<HashMap<u64, ReplyHandler>>> =
    LazyLock::new(Default::default);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// A new request id whose result `handler` gets.
fn register_reply(handler: ReplyHandler) -> u64 {
    let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    REPLY_HANDLERS.lock().unwrap().insert(id, handler);
    id
}

/// The id of a test request whose result reaches the test as a `T`.
struct ReplyId<T>(u64, PhantomData<T>);

impl<T> Clone for ReplyId<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ReplyId<T> {}

type PutReply = BackendResult<PutItemResult>;
type GetReply = BackendResult<Option<IndexedDBValue>>;

/// A request id and the receiver of the request's result.
type Callback<T> = (ReplyId<T>, mpsc::Receiver<T>);

fn get_callback<T: TestReply>() -> Callback<T> {
    let (sender, receiver) = mpsc::channel();
    let id = register_reply(Box::new(move |result| {
        assert!(sender.send(T::from_result(result)).is_ok());
    }));
    (ReplyId(id, PhantomData), receiver)
}

/// Where the results of `transaction` go in a test: each request result to the handler of its
/// id, and each schema operation failure to `schema_failures`.
fn routed_results(
    transaction: u64,
    schema_failures: mpsc::Sender<BackendError>,
) -> TransactionResults {
    routed_results_with_populations(transaction, schema_failures, None)
}

/// [`routed_results`], with the index population events to `populations`.
fn routed_results_with_populations(
    transaction: u64,
    schema_failures: mpsc::Sender<BackendError>,
    populations: Option<mpsc::Sender<(i64, IndexPopulationEvent)>>,
) -> TransactionResults {
    let channel = GenericCallback::new(move |event| match event.unwrap() {
        ConnectionEvent::Transaction {
            event: TransactionEvent::IndexPopulation { index_id, event },
            ..
        } if populations.is_some() => {
            let _ = populations.as_ref().unwrap().send((index_id, event));
        },
        ConnectionEvent::Transaction {
            event: TransactionEvent::Request { request_id, result },
            ..
        } => {
            let handler = REPLY_HANDLERS
                .lock()
                .unwrap()
                .remove(&request_id)
                .expect("A request replies once");
            handler(result);
        },
        ConnectionEvent::Transaction {
            event: TransactionEvent::SchemaChangeFailed(error),
            ..
        } => {
            let _ = schema_failures.send(error);
        },
        other => panic!("unexpected result of a batch: {other:?}"),
    })
    .unwrap();
    TransactionResults::new(txn(transaction), Some(channel))
}

/// Where the results of `transaction` go in a test whose schema operations don't fail.
fn results(transaction: u64) -> TransactionResults {
    let (schema_failures, _) = mpsc::channel();
    routed_results(transaction, schema_failures)
}

fn request<T>(store: i64, request_id: ReplyId<T>, operation: AsyncOperation) -> KvsOperation {
    KvsOperation {
        store,
        request_id: Some(request_id.0),
        operation,
    }
}

fn schema(store: i64, operation: AsyncSchemaOperation) -> KvsOperation {
    KvsOperation {
        store,
        request_id: None,
        operation: AsyncOperation::Schema(operation),
    }
}

/// A put into a store without indexes.
fn put_request(
    store: i64,
    request_id: ReplyId<PutReply>,
    key: IndexedDBKeyType,
    value: Vec<u8>,
    should_overwrite: bool,
) -> KvsOperation {
    indexed_put_request(
        store,
        request_id,
        Some(key),
        value,
        should_overwrite,
        vec![],
    )
}

/// A put under `key`, or under a generated key if it is `None`, with the record's keys in the
/// store's indexes.
fn indexed_put_request(
    store: i64,
    request_id: ReplyId<PutReply>,
    key: Option<IndexedDBKeyType>,
    value: Vec<u8>,
    should_overwrite: bool,
    index_keys: Vec<IndexedDBIndexKeys>,
) -> KvsOperation {
    request(
        store,
        request_id,
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
            key,
            value,
            should_overwrite,
            index_keys,
        }),
    )
}

/// The keys `keys` of a record in the index `index`.
fn index_keys(index: i64, keys: impl IntoIterator<Item = IndexedDBKeyType>) -> IndexedDBIndexKeys {
    IndexedDBIndexKeys {
        index_id: index,
        keys: keys.into_iter().map(IndexedDBIndexKey::Key).collect(),
    }
}

/// No keys in each of the indexes `indexes`.
fn no_index_keys(indexes: &[i64]) -> Vec<IndexedDBIndexKeys> {
    indexes.iter().map(|index| index_keys(*index, [])).collect()
}

/// Runs `puts` (an explicit key or `None` for a generated one, and the value) in order as one
/// batch of the readwrite `transaction` on `store`, which has no indexes, and returns their
/// replies.
fn run_puts<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    store: i64,
    puts: Vec<(Option<IndexedDBKeyType>, Vec<u8>)>,
    should_overwrite: bool,
) -> Vec<BackendResult<PutItemResult>> {
    let puts = puts
        .into_iter()
        .map(|(key, value)| (key, value, vec![]))
        .collect();
    run_indexed_puts(db, transaction, store, puts, should_overwrite)
}

/// Like [`run_puts`], with the keys of each record in the store's indexes.
fn run_indexed_puts<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    store: i64,
    puts: Vec<(Option<IndexedDBKeyType>, Vec<u8>, Vec<IndexedDBIndexKeys>)>,
    should_overwrite: bool,
) -> Vec<BackendResult<PutItemResult>> {
    let (requests, receivers): (Vec<_>, Vec<_>) = puts
        .into_iter()
        .map(|(key, value, index_keys)| {
            let (id, receiver) = get_callback();
            let put = indexed_put_request(store, id, key, value, should_overwrite, index_keys);
            (put, receiver)
        })
        .unzip();
    run_batch(db, transaction, IndexedDBTxnMode::Readwrite, requests);
    receivers.iter().map(recv).collect()
}

/// The reply to a put that stored its record under the number `key`.
fn key_reply(key: f64) -> BackendResult<PutItemResult> {
    Ok(PutItemResult::Key(IndexedDBKeyType::Number(key)))
}

/// Runs a batch of `transaction`, waits until it has run, and returns the failures of its
/// schema operations, in order.
fn run_batch<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    mode: IndexedDBTxnMode,
    requests: Vec<KvsOperation>,
) -> Vec<BackendError> {
    let (done_sender, done_receiver) = mpsc::channel();
    let (schema_failures, failures) = mpsc::channel();
    db.process_transaction(
        txn(transaction),
        KvsTransaction {
            mode,
            requests: VecDeque::from(requests),
        },
        routed_results(transaction, schema_failures),
        Box::new(move || {
            let _ = done_sender.send(());
        }),
    );
    done_receiver
        .recv_timeout(TIMEOUT)
        .expect("The batch should complete");
    failures.try_iter().collect()
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

/// The keys of the store `store`, read on `executor`.
fn keys_of<X: SqlExecutor>(executor: &mut X, store: i64) -> Vec<IndexedDBKeyType> {
    let store = queries::object_store(executor, store).unwrap().unwrap();
    queries::get_all_keys(
        executor,
        RecordSource::ObjectStore(&store),
        &IndexedDBKeyRange::default(),
        IndexedDBCursorDirection::Next,
        None,
    )
    .unwrap()
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
    let store_id = create_store(&db, "test_store", None, false).expect("Failed to create store");
    let put = get_callback();
    let put2 = get_callback();
    let put3 = get_callback();
    let put_dup = get_callback();
    let put_overwrite = get_callback();
    let get_item_some = get_callback::<GetReply>();
    let get_item_none = get_callback::<GetReply>();
    let get_all_items = get_callback::<BackendResult<Vec<IndexedDBValue>>>();
    let count = get_callback::<BackendResult<u64>>();
    let remove = get_callback::<BackendResult<()>>();
    let clear = get_callback::<BackendResult<()>>();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        vec![
            put_request(
                store_id,
                put.0,
                IndexedDBKeyType::Number(1.0),
                vec![1, 2, 3],
                false,
            ),
            put_request(
                store_id,
                put2.0,
                IndexedDBKeyType::String("2.0".into()),
                vec![4, 5, 6],
                false,
            ),
            put_request(
                store_id,
                put3.0,
                IndexedDBKeyType::Array(vec![
                    IndexedDBKeyType::String("3".into()),
                    IndexedDBKeyType::Number(0.0),
                ]),
                vec![7, 8, 9],
                false,
            ),
            // Try to put a duplicate key without overwrite
            put_request(
                store_id,
                put_dup.0,
                IndexedDBKeyType::Number(1.0),
                vec![10, 11, 12],
                false,
            ),
            put_request(
                store_id,
                put_overwrite.0,
                IndexedDBKeyType::Number(1.0),
                vec![13, 14, 15],
                true,
            ),
            request(
                store_id,
                get_item_some.0,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                    index: None,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_id,
                get_item_none.0,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                    index: None,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(5.0)),
                }),
            ),
            request(
                store_id,
                get_all_items.0,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetAllItems {
                    index: None,
                    key_range: IndexedDBKeyRange::lower_bound(IndexedDBKeyType::Number(0.0), false),
                    count: None,
                    direction: IndexedDBCursorDirection::Next,
                }),
            ),
            request(
                store_id,
                count.0,
                AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                    index: None,
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_id,
                remove.0,
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem {
                    key_range: IndexedDBKeyRange::only(IndexedDBKeyType::Number(1.0)),
                }),
            ),
            request(
                store_id,
                clear.0,
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear),
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
        primary_key: IndexedDBKeyType::String("2.0".into()),
        value: vec![4, 5, 6],
    }));
    assert!(all_items.contains(&IndexedDBValue {
        primary_key: IndexedDBKeyType::Array(vec![
            IndexedDBKeyType::String("3".into()),
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
        let store_id =
            create_store(&db, "test_store", None, false).expect("Failed to create store");
        let mut connection = db.connection();
        let store = queries::object_store(&mut *connection, store_id)
            .expect("Failed to get object store")
            .expect("The store should exist");

        for key in 1..=10 {
            queries::put_record(
                &mut *connection,
                &store,
                Some(IndexedDBKeyType::Number(key as f64)),
                &[key as u8],
                false,
                vec![],
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
            RecordSource::ObjectStore(&store),
            &IndexedDBKeyRange::default(),
            IndexedDBCursorDirection::Next,
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

fn stored_keys<X: SqlExecutor>(db: &SqlEngine<X>, store: i64) -> Vec<IndexedDBKeyType> {
    keys_of(&mut *db.connection(), store)
}

/// The version and the object store names in the database, also inside its open SQL
/// transaction, unlike the engine's metadata reads, which answer as of the last commit.
pub(crate) fn stored_version_and_store_names<X: SqlExecutor>(
    db: &SqlEngine<X>,
) -> (u64, Vec<String>) {
    let mut connection = db.connection();
    (
        queries::version(&mut *connection).unwrap(),
        queries::object_stores(&mut *connection)
            .unwrap()
            .into_iter()
            .map(|store| store.name.to_string())
            .collect(),
    )
}

/// A write that fails in the database aborts the transaction: everything it wrote is rolled back,
/// and its later requests and its commit fail.
fn test_failed_write_fails_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_id = create_store(&db, "test_store", None, false).unwrap();
    let index_ids: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|index_name| {
            create_index(
                &db,
                store_id,
                index_name.to_owned(),
                KeyPath::String(index_name.to_owned()),
                false,
                false,
            )
            .unwrap()
        })
        .collect();

    let put = get_callback();
    let put_after = get_callback();
    let schema_failures = run_batch(
        &db,
        1,
        IndexedDBTxnMode::Versionchange,
        vec![
            indexed_put_request(
                store_id,
                put.0,
                Some(IndexedDBKeyType::Number(1.0)),
                vec![1],
                false,
                no_index_keys(&index_ids),
            ),
            // Index names are unique per store, so this fails in the database.
            schema(
                store_id,
                AsyncSchemaOperation::RenameIndex {
                    index_id: index_ids[0],
                    new_name: "b".into(),
                },
            ),
            indexed_put_request(
                store_id,
                put_after.0,
                Some(IndexedDBKeyType::Number(2.0)),
                vec![2],
                false,
                no_index_keys(&index_ids),
            ),
        ],
    );
    assert_eq!(
        recv(&put.1),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(1.0)))
    );
    assert!(matches!(schema_failures[..], [BackendError::DbErr(_)]));
    assert!(recv(&put_after.1).is_err());

    let put_later = get_callback();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Versionchange,
        vec![indexed_put_request(
            store_id,
            put_later.0,
            Some(IndexedDBKeyType::Number(3.0)),
            vec![3],
            false,
            no_index_keys(&index_ids),
        )],
    );
    assert!(recv(&put_later.1).is_err());
    assert!(commit(&db, 1).is_err());

    assert_eq!(stored_keys(&db, store_id), vec![]);
    let index_names: Vec<String> = db.object_stores()[0]
        .indexes
        .iter()
        .map(|index| index.name.to_string())
        .collect();
    assert_eq!(index_names, vec!["a".to_owned(), "b".to_owned()]);
}

/// A request for a store that does not exist fails without effects, so the transaction goes on.
fn test_missing_store_fails_only_its_request<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store_id = create_store(&db, "test_store", None, false).unwrap();

    let missing = get_callback();
    let put = get_callback();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        vec![
            put_request(
                store_id + 1,
                missing.0,
                IndexedDBKeyType::Number(1.0),
                vec![1],
                false,
            ),
            put_request(
                store_id,
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
    assert_eq!(stored_keys(&db, store_id), number_keys([2]));
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
        TEST_STORE,
        vec![
            (None, vec![1]),
            (None, vec![2]),
            (number(1000.0), vec![3]),
            (None, vec![4]),
            (Some(IndexedDBKeyType::String("9999".into())), vec![5]),
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
    assert_eq!(db.key_generator_current_number(TEST_STORE), Some(1006));
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
        TEST_STORE,
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
        stored_keys(&db, TEST_STORE),
        vec![
            IndexedDBKeyType::Number(2.0),
            IndexedDBKeyType::Number(max - 1.0),
            IndexedDBKeyType::Number(max),
        ]
    );
    assert_eq!(
        db.key_generator_current_number(TEST_STORE),
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
        let store = create_store(&db, &format!("store {key}"), None, true).unwrap();
        let replies = run_puts(
            &db,
            transaction,
            store,
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
    let replies = run_puts(&db, 1, TEST_STORE, vec![(None, vec![1])], false);
    assert_eq!(replies[0], key_reply(1.0));

    let replies = run_puts(
        &db,
        1,
        TEST_STORE,
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
    assert_eq!(db.key_generator_current_number(TEST_STORE), Some(3));
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
        TEST_STORE,
        vec![
            (None, vec![1]),
            (Some(IndexedDBKeyType::Number(10.0)), vec![2]),
            (None, vec![3]),
        ],
        false,
    );
    assert_eq!(replies[2], key_reply(11.0));
    abort(&db, 1);
    assert_eq!(db.key_generator_current_number(TEST_STORE), Some(1));

    let replies = run_puts(&db, 2, TEST_STORE, vec![(None, vec![4])], true);
    assert_eq!(replies[0], key_reply(1.0));
    assert_eq!(commit(&db, 2), Ok(()));
    assert_eq!(db.key_generator_current_number(TEST_STORE), Some(2));
}

/// Aborting a transaction rolls back everything it did in all its batches, and for an upgrade
/// transaction also its schema changes and the version.
fn test_abort_rolls_back_data_schema_and_version<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    set_version(&db, 1).unwrap();
    let kept = create_store(&db, "kept", None, false).unwrap();
    let deleted = create_store(&db, "deleted", None, false).unwrap();
    let by_a = create_index(
        &db,
        kept,
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
        [kept, deleted]
            .into_iter()
            .zip(&seeds)
            .map(|(store, seed)| {
                let index_keys = if store == kept {
                    no_index_keys(&[by_a])
                } else {
                    vec![]
                };
                let key = Some(IndexedDBKeyType::Number(1.0));
                indexed_put_request(store, seed.0, key, vec![1], false, index_keys)
            })
            .collect(),
    );
    assert_eq!(commit(&db, 1), Ok(()));

    let created = 3;
    let put = get_callback();
    db.upgrade_version(txn(2), 2);
    let schema_failures = run_batch(
        &db,
        2,
        IndexedDBTxnMode::Versionchange,
        vec![
            indexed_put_request(
                kept,
                put.0,
                Some(IndexedDBKeyType::Number(2.0)),
                vec![2],
                false,
                no_index_keys(&[by_a]),
            ),
            schema(
                created,
                AsyncSchemaOperation::CreateObjectStore {
                    name: "created".into(),
                    key_path: None,
                    auto_increment: true,
                },
            ),
            schema(
                kept,
                AsyncSchemaOperation::CreateIndex {
                    index_id: 2,
                    name: "by_b".into(),
                    key_path: KeyPath::String("b".to_owned()),
                    unique: false,
                    multi_entry: false,
                    populate: false,
                },
            ),
            schema(
                kept,
                AsyncSchemaOperation::RenameIndex {
                    index_id: by_a,
                    new_name: "by_c".into(),
                },
            ),
            schema(deleted, AsyncSchemaOperation::DeleteObjectStore),
            schema(
                kept,
                AsyncSchemaOperation::RenameObjectStore {
                    new_name: "renamed".into(),
                },
            ),
        ],
    );
    let generated = get_callback::<PutReply>();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Versionchange,
        vec![request(
            created,
            generated.0,
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                key: None,
                value: vec![3],
                should_overwrite: false,
                index_keys: vec![],
            }),
        )],
    );
    assert!(recv(&put.1).is_ok());
    assert_eq!(
        recv(&generated.1),
        Ok(PutItemResult::Key(IndexedDBKeyType::Number(1.0)))
    );
    assert_eq!(schema_failures, vec![]);
    let index_names = |db: &SqlEngine<X>| -> Vec<String> {
        queries::object_stores(&mut *db.connection())
            .unwrap()
            .into_iter()
            .find(|store| store.id == kept)
            .unwrap()
            .indexes
            .into_iter()
            .map(|index| index.name.to_string())
            .collect()
    };
    let before = (1, vec!["kept".to_owned(), "deleted".to_owned()]);
    assert_eq!(
        stored_version_and_store_names(&db),
        (2, vec!["renamed".to_owned(), "created".to_owned()])
    );
    assert_eq!(index_names(&db), vec!["by_c", "by_b"]);
    assert_eq!(stored_keys(&db, kept), number_keys([1, 2]));
    assert_eq!((db.version().unwrap(), store_names(&db)), before);

    abort(&db, 2);

    assert_eq!(stored_version_and_store_names(&db), before);
    assert_eq!((db.version().unwrap(), store_names(&db)), before);
    assert_eq!(index_names(&db), vec!["by_a"]);
    assert_eq!(
        db.object_stores()[0]
            .indexes
            .iter()
            .map(|index| index.name.to_string())
            .collect::<Vec<_>>(),
        vec!["by_a"]
    );
    assert_eq!(stored_keys(&db, kept), number_keys([1]));
    assert_eq!(stored_keys(&db, deleted), number_keys([1]));
}

/// A schema operation with an id that isn't new, or for a store or index that doesn't exist,
/// fails its transaction, which rolls back.
fn test_invalid_schema_operation_fails_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "test_store", None, false).unwrap();
    let invalid_operations = || {
        vec![
            (
                store,
                AsyncSchemaOperation::CreateObjectStore {
                    name: "other".into(),
                    key_path: None,
                    auto_increment: false,
                },
            ),
            (
                store + 1,
                AsyncSchemaOperation::CreateIndex {
                    index_id: 0,
                    name: "index".into(),
                    key_path: KeyPath::String("a".to_owned()),
                    unique: false,
                    multi_entry: false,
                    populate: false,
                },
            ),
            (
                store + 5,
                AsyncSchemaOperation::CreateIndex {
                    index_id: 1,
                    name: "index".into(),
                    key_path: KeyPath::String("a".to_owned()),
                    unique: false,
                    multi_entry: false,
                    populate: false,
                },
            ),
            (store, AsyncSchemaOperation::DeleteIndex { index_id: 1 }),
            (
                store,
                AsyncSchemaOperation::RenameIndex {
                    index_id: 1,
                    new_name: "index".into(),
                },
            ),
            (
                store,
                AsyncSchemaOperation::RenameObjectStore {
                    new_name: "created".into(),
                },
            ),
            (
                store + 5,
                AsyncSchemaOperation::RenameObjectStore {
                    new_name: "other".into(),
                },
            ),
            (store + 5, AsyncSchemaOperation::DeleteObjectStore),
        ]
    };
    for (serial, (target, operation)) in (1..).zip(invalid_operations()) {
        db.upgrade_version(txn(serial), serial + 1);
        let put = get_callback();
        let schema_failures = run_batch(
            &db,
            serial,
            IndexedDBTxnMode::Versionchange,
            vec![
                schema(
                    store + 1,
                    AsyncSchemaOperation::CreateObjectStore {
                        name: "created".into(),
                        key_path: None,
                        auto_increment: false,
                    },
                ),
                schema(target, operation),
                put_request(store, put.0, IndexedDBKeyType::Number(1.0), vec![1], false),
            ],
        );
        // Only the invalid operation fails: the store is created before it.
        assert!(matches!(schema_failures[..], [BackendError::DbErr(_)]));
        assert!(recv(&put.1).is_err());
        assert!(commit(&db, serial).is_err());
        abort(&db, serial);
        assert_eq!(
            stored_version_and_store_names(&db),
            (0, vec!["test_store".to_owned()])
        );
    }
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
                    TEST_STORE,
                    put.0,
                    IndexedDBKeyType::Number(key.into()),
                    vec![key as u8],
                    false,
                )],
            );
            assert!(recv(&put.1).is_ok());
            assert_eq!(keys_of(other, TEST_STORE), vec![]);
        }
    };

    put_batches(1, 1..=10, &mut other);
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(keys_of(&mut other, TEST_STORE), number_keys(1..=10));

    let mut other_after_commit = db.connection().connector()().unwrap();
    for key in 11..=20 {
        let put = get_callback();
        run_batch(
            &db,
            2,
            IndexedDBTxnMode::Readwrite,
            vec![put_request(
                TEST_STORE,
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
        keys_of(&mut other_after_commit, TEST_STORE),
        number_keys(1..=10)
    );
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys(1..=10));
}

/// A request id whose result reports `id` on `order`.
fn tagged<T>(order: &mpsc::Sender<u32>, id: u32) -> ReplyId<T> {
    let order = order.clone();
    ReplyId(
        register_reply(Box::new(move |_| {
            let _ = order.send(id);
        })),
        PhantomData,
    )
}

/// Requests run in the order they were handed over: those of one transaction, and those of
/// readonly transactions that run alongside a writer.
fn test_requests_run_in_order<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let written = create_store(&db, "written", None, false).unwrap();
    let read = create_store(&db, "read", None, false).unwrap();
    let (order_sender, order) = mpsc::channel();
    let put = |id: u32, key: f64, value: u8| {
        put_request(
            written,
            tagged(&order_sender, id),
            IndexedDBKeyType::Number(key),
            vec![value],
            true,
        )
    };
    let read = |id: u32| {
        request(
            read,
            tagged::<BackendResult<u64>>(&order_sender, id),
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                index: None,
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
            results(transaction),
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

    let value = get_callback::<GetReply>();
    run_batch(
        &db,
        4,
        IndexedDBTxnMode::Readonly,
        vec![request(
            written,
            value.0,
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                index: None,
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
                TEST_STORE,
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
        results(2),
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
    assert_eq!(stored_keys(&busy, TEST_STORE), number_keys([1]));
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
            TEST_STORE,
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

fn get_request(request_id: ReplyId<GetReply>, key: f64) -> KvsOperation {
    request(
        TEST_STORE,
        request_id,
        AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
            index: None,
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
                TEST_STORE,
                put.0,
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
                TEST_STORE,
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
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys([1]));
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
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys([1]));

    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_data", 1));
    let gets: Vec<_> = (0..3).map(|_| get_callback()).collect();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        gets.iter().map(|get| get_request(get.0, 1.0)).collect(),
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
    let other = create_store(&db, "other", None, false).unwrap();

    let puts = [get_callback()];
    run_batch(&db, 1, IndexedDBTxnMode::Readwrite, put_requests(&puts, 2));
    assert!(recv(&puts[0].1).is_ok());

    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_data", 0));
    let get = get_callback::<GetReply>();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![request(
            other,
            get.0,
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                index: None,
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
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys([1]));
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
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys([1]));
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
            results(id.into()),
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
    assert_eq!(stored_keys(&db, TEST_STORE), number_keys([1]));
}

/// Any job on the connection, not only a request, can end the writer's SQL transaction.
fn test_job_that_ends_the_sql_transaction_fails_the_writer<X: SqlExecutor>() {
    check_writer_fails_after::<X>(|db| {
        db.spawn_on_connection(|executor| {
            let _ = queries::object_store(executor, TEST_STORE);
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
    let stores = db.object_stores();
    assert_eq!(store_names(&db), vec!["test_store"]);
    assert_eq!(stores[0].key_path, key_path);
    assert!(stores[0].has_key_generator);
    assert!(stores[0].indexes.is_empty());
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
                TEST_STORE,
                put.0,
                IndexedDBKeyType::Number(key.into()),
                vec![1],
                false,
            )],
        );
        assert!(recv(&put.1).is_ok());
        assert_eq!(keys_of(&mut other, TEST_STORE), vec![]);
        std::thread::sleep(Duration::from_secs(idle_seconds));
    }
    assert_eq!(commit(&db, 1), Ok(()));
    assert!(start.elapsed() > Duration::from_secs(180));
    assert_eq!(keys_of(&mut other, TEST_STORE), number_keys(1..=3));
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

/// A string key.
fn string(string: &str) -> IndexedDBKeyType {
    IndexedDBKeyType::String(string.into())
}

fn number(number: f64) -> IndexedDBKeyType {
    IndexedDBKeyType::Number(number)
}

/// The records of the index `index` of `store`, as (key, primary key), in the index's order.
fn index_records_of<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store: i64,
    index: i64,
) -> Vec<(IndexedDBKeyType, IndexedDBKeyType)> {
    let mut connection = db.connection();
    let index = queries::object_store(&mut *connection, store)
        .unwrap()
        .unwrap()
        .index(index)
        .unwrap();
    let sql = format!(
        "SELECT value, object_data_key FROM {} WHERE index_id = ? \
         ORDER BY value, object_data_key",
        index.table()
    );
    query_rows(
        &mut *connection,
        &sql,
        &[SqlParam::Integer(index.id)],
        |row| {
            Ok((
                encoding::deserialize(&row.blob(0)?).unwrap(),
                encoding::deserialize(&row.blob(1)?).unwrap(),
            ))
        },
    )
    .unwrap()
}

/// The number of records of the index `index` in both index tables.
fn index_record_count<X: SqlExecutor>(db: &SqlEngine<X>, index: i64) -> i64 {
    query_row(
        &mut *db.connection(),
        "SELECT (SELECT COUNT(*) FROM index_data WHERE index_id = ?) + \
         (SELECT COUNT(*) FROM unique_index_data WHERE index_id = ?)",
        &[SqlParam::Integer(index), SqlParam::Integer(index)],
        |row| row.integer(0),
    )
    .unwrap()
    .unwrap()
}

/// The number of index records whose record isn't in the store. The host's SQLite rejects them
/// with its foreign keys; RDB doesn't check them in transactions.
fn dangling_index_records<X: SqlExecutor>(db: &SqlEngine<X>) -> i64 {
    query_row(
        &mut *db.connection(),
        "SELECT \
         (SELECT COUNT(*) FROM index_data AS i WHERE NOT EXISTS (SELECT 1 FROM object_data AS o \
         WHERE o.object_store_id = i.object_store_id AND o.key = i.object_data_key)) + \
         (SELECT COUNT(*) FROM unique_index_data AS i WHERE NOT EXISTS (SELECT 1 FROM object_data \
         AS o WHERE o.object_store_id = i.object_store_id AND o.key = i.object_data_key))",
        &[],
        |row| row.integer(0),
    )
    .unwrap()
    .unwrap()
}

/// `(key, primary key)` index records with string keys and number primary keys.
fn string_records(records: &[(&str, f64)]) -> Vec<(IndexedDBKeyType, IndexedDBKeyType)> {
    records
        .iter()
        .map(|(key, primary_key)| (string(key), number(*primary_key)))
        .collect()
}

fn remove_request(
    store: i64,
    request_id: ReplyId<BackendResult<()>>,
    key_range: IndexedDBKeyRange,
) -> KvsOperation {
    request(
        store,
        request_id,
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem { key_range }),
    )
}

/// A put stores a record in each index for each of the record's keys there, a put that replaces a
/// record replaces its index records, and an index without keys for the record gets none.
fn test_index_records_follow_puts_and_overwrites<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "people", None, false).unwrap();
    let index = |name: &str, unique, multi_entry| {
        let key_path = KeyPath::String(name.to_owned());
        create_index(&db, store, name.to_owned(), key_path, unique, multi_entry).unwrap()
    };
    let (by_name, by_email, by_tag) = (
        index("name", false, false),
        index("email", true, false),
        index("tag", false, true),
    );
    let keys = |name: Option<&str>, email: &str, tags: &[&str]| {
        vec![
            index_keys(by_name, name.map(string)),
            index_keys(by_email, [string(email)]),
            index_keys(by_tag, tags.iter().map(|tag| string(tag))),
        ]
    };
    let puts = vec![
        (
            Some(number(1.0)),
            vec![1],
            keys(Some("ann"), "a@x", &["red", "blue"]),
        ),
        (
            Some(number(2.0)),
            vec![2],
            keys(Some("ann"), "b@x", &["blue"]),
        ),
        (Some(number(3.0)), vec![3], keys(None, "c@x", &[])),
    ];
    assert_eq!(
        run_indexed_puts(&db, 1, store, puts, true),
        vec![key_reply(1.0), key_reply(2.0), key_reply(3.0)]
    );
    let emails = string_records(&[("a@x", 1.0), ("b@x", 2.0), ("c@x", 3.0)]);
    assert_eq!(
        index_records_of(&db, store, by_name),
        string_records(&[("ann", 1.0), ("ann", 2.0)])
    );
    assert_eq!(index_records_of(&db, store, by_email), emails);
    assert_eq!(
        index_records_of(&db, store, by_tag),
        string_records(&[("blue", 1.0), ("blue", 2.0), ("red", 1.0)])
    );

    // Record 1 gets another name, keeps its own email, and loses its tags.
    let puts = vec![(Some(number(1.0)), vec![4], keys(Some("bea"), "a@x", &[]))];
    assert_eq!(
        run_indexed_puts(&db, 1, store, puts, true),
        vec![key_reply(1.0)]
    );
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(
        index_records_of(&db, store, by_name),
        string_records(&[("ann", 2.0), ("bea", 1.0)])
    );
    assert_eq!(index_records_of(&db, store, by_email), emails);
    assert_eq!(
        index_records_of(&db, store, by_tag),
        string_records(&[("blue", 2.0)])
    );
    // A unique index's records are only in `unique_index_data`.
    assert_eq!(index_record_count(&db, by_email), 3);
    assert_eq!(dangling_index_records(&db), 0);
}

/// Deleting a record, a range of records or all of them deletes their index records, and only
/// theirs.
fn test_deleted_records_take_their_index_records_along<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let other = create_store(&db, "other", None, false).unwrap();
    let index = |store, name: &str, unique| {
        let key_path = KeyPath::String(name.to_owned());
        create_index(&db, store, name.to_owned(), key_path, unique, false).unwrap()
    };
    let (by_parity, by_tenfold, by_other) = (
        index(store, "parity", false),
        index(store, "tenfold", true),
        index(other, "parity", false),
    );
    let puts = (1..=6)
        .map(|key| {
            let key = f64::from(key);
            let index_keys = vec![
                index_keys(by_parity, [number(key % 2.0)]),
                index_keys(by_tenfold, [number(key * 10.0)]),
            ];
            (Some(number(key)), vec![], index_keys)
        })
        .collect();
    assert!(
        run_indexed_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    let puts = vec![(
        Some(number(2.0)),
        vec![],
        vec![index_keys(by_other, [number(0.0)])],
    )];
    assert_eq!(
        run_indexed_puts(&db, 1, other, puts, false),
        vec![key_reply(2.0)]
    );

    let removals: Vec<_> = (0..2).map(|_| get_callback()).collect();
    let lower_open = IndexedDBKeyRange::new(Some(number(3.0)), Some(number(5.0)), true, false);
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        vec![
            remove_request(store, removals[0].0, IndexedDBKeyRange::only(number(2.0))),
            remove_request(store, removals[1].0, lower_open),
        ],
    );
    assert!(removals.iter().all(|removal| recv(&removal.1).is_ok()));
    assert_eq!(stored_keys(&db, store), number_keys([1, 3, 6]));
    let records = |pairs: &[(f64, f64)]| -> Vec<_> {
        pairs
            .iter()
            .map(|(key, primary_key)| (number(*key), number(*primary_key)))
            .collect()
    };
    assert_eq!(
        index_records_of(&db, store, by_parity),
        records(&[(0.0, 6.0), (1.0, 1.0), (1.0, 3.0)])
    );
    assert_eq!(
        index_records_of(&db, store, by_tenfold),
        records(&[(10.0, 1.0), (30.0, 3.0), (60.0, 6.0)])
    );

    let clear = get_callback::<BackendResult<()>>();
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        vec![request(
            store,
            clear.0,
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear),
        )],
    );
    assert_eq!(recv(&clear.1), Ok(()));
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(index_record_count(&db, by_parity), 0);
    assert_eq!(index_record_count(&db, by_tenfold), 0);
    assert_eq!(
        index_records_of(&db, other, by_other),
        records(&[(0.0, 2.0)])
    );
    assert_eq!(dangling_index_records(&db), 0);
}

/// Deleting an index deletes its records, and deleting a store those of all its indexes, so an
/// index that gets the id of a deleted one in a later transaction starts empty.
fn test_deleted_indexes_take_their_records_along<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    set_version(&db, 1).unwrap();
    let kept = create_store(&db, "kept", None, false).unwrap();
    let deleted = create_store(&db, "deleted", None, false).unwrap();
    let key_path = KeyPath::String("a".to_owned());
    let index = |store, name: &str, unique| {
        create_index(&db, store, name.to_owned(), key_path.clone(), unique, false).unwrap()
    };
    let (stays, goes, goes_with_store) = (
        index(kept, "stays", false),
        index(kept, "goes", true),
        index(deleted, "goes_with_store", false),
    );
    let puts = vec![(
        Some(number(1.0)),
        vec![],
        vec![
            index_keys(stays, [string("a")]),
            index_keys(goes, [string("a")]),
        ],
    )];
    assert!(run_indexed_puts(&db, 1, kept, puts, false)[0].is_ok());
    let puts = vec![(
        Some(number(1.0)),
        vec![],
        vec![index_keys(goes_with_store, [string("a")])],
    )];
    assert!(run_indexed_puts(&db, 1, deleted, puts, false)[0].is_ok());
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(
        [stays, goes, goes_with_store].map(|index| index_record_count(&db, index)),
        [1, 1, 1]
    );

    db.upgrade_version(txn(2), 2);
    let failures = run_batch(
        &db,
        2,
        IndexedDBTxnMode::Versionchange,
        vec![
            schema(kept, AsyncSchemaOperation::DeleteIndex { index_id: goes }),
            schema(deleted, AsyncSchemaOperation::DeleteObjectStore),
        ],
    );
    assert_eq!(failures, vec![]);
    assert_eq!(commit(&db, 2), Ok(()));
    assert_eq!(
        [stays, goes, goes_with_store].map(|index| index_record_count(&db, index)),
        [1, 0, 0]
    );

    // The largest index id is `stays`'s now, so the ids of the deleted indexes come back.
    db.upgrade_version(txn(3), 3);
    let failures = run_batch(
        &db,
        3,
        IndexedDBTxnMode::Versionchange,
        vec![schema(
            kept,
            AsyncSchemaOperation::CreateIndex {
                index_id: goes,
                name: "comes".into(),
                key_path: key_path.clone(),
                unique: false,
                multi_entry: false,
                populate: false,
            },
        )],
    );
    assert_eq!(failures, vec![]);
    assert_eq!(commit(&db, 3), Ok(()));
    assert_eq!(index_records_of(&db, kept, goes), vec![]);
    assert_eq!(index_record_count(&db, goes), 0);
    assert_eq!(dangling_index_records(&db), 0);
}

/// A put with a key that a unique index has for another record fails without any effect: not on
/// the record it would replace, on the other indexes or on the key generator, and the transaction
/// goes on. A replaced record doesn't conflict with its own keys, and each key of a multiEntry
/// unique index must be unique.
fn test_unique_index_conflict_changes_nothing<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "users", None, true).unwrap();
    let index = |name: &str, unique, multi_entry| {
        let key_path = KeyPath::String(name.to_owned());
        create_index(&db, store, name.to_owned(), key_path, unique, multi_entry).unwrap()
    };
    let (by_email, by_name, by_tag) = (
        index("email", true, false),
        index("name", false, false),
        index("tag", true, true),
    );
    let put = |key: Option<f64>, value: u8, email: &str, name: &str, tags: &[&str]| {
        let index_keys = vec![
            index_keys(by_email, [string(email)]),
            index_keys(by_name, [string(name)]),
            index_keys(by_tag, tags.iter().map(|tag| string(tag))),
        ];
        (key.map(number), vec![value], index_keys)
    };
    let conflict = |index| Ok(PutItemResult::UniqueIndexKeyExists(index));
    let replies = run_indexed_puts(
        &db,
        1,
        store,
        vec![
            put(None, 1, "a", "ann", &["t1", "t2"]),
            put(None, 2, "b", "bob", &["t3"]),
            put(None, 3, "a", "cat", &[]),
            put(Some(10.0), 4, "x", "cat", &["t5", "t2"]),
            put(Some(1.0), 5, "b", "cat", &[]),
            put(Some(1.0), 6, "a", "ann", &["t2", "t1"]),
            put(None, 7, "c", "dan", &["t4", "t4"]),
        ],
        true,
    );
    assert_eq!(
        replies,
        vec![
            key_reply(1.0),
            key_reply(2.0),
            conflict(by_email),
            conflict(by_tag),
            conflict(by_email),
            key_reply(1.0),
            key_reply(3.0),
        ]
    );
    assert_eq!(commit(&db, 1), Ok(()));
    assert_eq!(db.key_generator_current_number(store), Some(4));
    assert_eq!(stored_keys(&db, store), number_keys([1, 2, 3]));
    let get = get_callback::<GetReply>();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![request(
            store,
            get.0,
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem {
                index: None,
                key_range: IndexedDBKeyRange::only(number(1.0)),
            }),
        )],
    );
    assert_eq!(recv(&get.1), Ok(Some(stored_value(1.0, vec![6]))));
    assert_eq!(
        index_records_of(&db, store, by_email),
        string_records(&[("a", 1.0), ("b", 2.0), ("c", 3.0)])
    );
    assert_eq!(
        index_records_of(&db, store, by_name),
        string_records(&[("ann", 1.0), ("bob", 2.0), ("dan", 3.0)])
    );
    assert_eq!(
        index_records_of(&db, store, by_tag),
        string_records(&[("t1", 1.0), ("t2", 1.0), ("t3", 2.0), ("t4", 3.0)])
    );
    assert_eq!(dangling_index_records(&db), 0);
}

/// The parts of index keys that come from a generated key become the record's key, as a whole
/// key and inside an array key.
fn test_placeholders_become_the_record_key<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let id = KeyPath::String("id".to_owned());
    let store = create_store(&db, "items", Some(id.clone()), true).unwrap();
    let pair = KeyPath::Sequence(vec!["x".to_owned(), "id".to_owned()]);
    let by_id = create_index(&db, store, "by_id".to_owned(), id.clone(), true, false).unwrap();
    let by_ids = create_index(&db, store, "by_ids".to_owned(), id, false, true).unwrap();
    let by_pair = create_index(&db, store, "by_pair".to_owned(), pair, true, false).unwrap();
    let placeholders = |x: f64| {
        vec![
            IndexedDBIndexKeys {
                index_id: by_id,
                keys: vec![IndexedDBIndexKey::PrimaryKey],
            },
            IndexedDBIndexKeys {
                index_id: by_ids,
                keys: vec![IndexedDBIndexKey::PrimaryKey],
            },
            IndexedDBIndexKeys {
                index_id: by_pair,
                keys: vec![IndexedDBIndexKey::Array(vec![
                    IndexedDBIndexKey::Key(number(x)),
                    IndexedDBIndexKey::PrimaryKey,
                ])],
            },
        ]
    };
    let explicit = vec![
        index_keys(by_id, [number(5.0)]),
        index_keys(by_ids, [number(5.0)]),
        index_keys(
            by_pair,
            [IndexedDBKeyType::Array(vec![number(0.0), number(5.0)])],
        ),
    ];
    let replies = run_indexed_puts(
        &db,
        1,
        store,
        vec![
            (None, vec![1], placeholders(0.0)),
            (None, vec![2], placeholders(0.0)),
            (Some(number(5.0)), vec![3], explicit),
            (None, vec![4], placeholders(1.0)),
        ],
        false,
    );
    assert_eq!(
        replies,
        vec![
            key_reply(1.0),
            key_reply(2.0),
            key_reply(5.0),
            key_reply(6.0)
        ]
    );
    assert_eq!(commit(&db, 1), Ok(()));
    let same: Vec<_> = [1.0, 2.0, 5.0, 6.0]
        .into_iter()
        .map(|key| (number(key), number(key)))
        .collect();
    assert_eq!(index_records_of(&db, store, by_id), same);
    assert_eq!(index_records_of(&db, store, by_ids), same);
    let pairs: Vec<_> = [(0.0, 1.0), (0.0, 2.0), (0.0, 5.0), (1.0, 6.0)]
        .into_iter()
        .map(|(x, key)| {
            (
                IndexedDBKeyType::Array(vec![number(x), number(key)]),
                number(key),
            )
        })
        .collect();
    assert_eq!(index_records_of(&db, store, by_pair), pairs);
}

/// Aborting a transaction reverts what it did to index records, in both index tables.
fn test_abort_reverts_index_records<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let key_path = KeyPath::String("a".to_owned());
    let plain = create_index(
        &db,
        store,
        "plain".to_owned(),
        key_path.clone(),
        false,
        false,
    )
    .unwrap();
    let unique = create_index(&db, store, "unique".to_owned(), key_path, true, false).unwrap();
    let put = |key: f64, index_key: &str| {
        let index_keys = vec![
            index_keys(plain, [string(index_key)]),
            index_keys(unique, [string(index_key)]),
        ];
        (Some(number(key)), vec![], index_keys)
    };
    assert!(run_indexed_puts(&db, 1, store, vec![put(1.0, "a")], true)[0].is_ok());
    assert_eq!(commit(&db, 1), Ok(()));

    let replies = run_indexed_puts(&db, 2, store, vec![put(1.0, "b"), put(2.0, "c")], true);
    assert!(replies.iter().all(Result::is_ok));
    let removal = get_callback();
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readwrite,
        vec![remove_request(
            store,
            removal.0,
            IndexedDBKeyRange::only(number(1.0)),
        )],
    );
    assert_eq!(recv(&removal.1), Ok(()));
    assert_eq!(
        index_records_of(&db, store, unique),
        string_records(&[("c", 2.0)])
    );
    abort(&db, 2);

    let before = string_records(&[("a", 1.0)]);
    assert_eq!(index_records_of(&db, store, plain), before);
    assert_eq!(index_records_of(&db, store, unique), before);
    // The aborted put's unique key is free again.
    assert_eq!(
        run_indexed_puts(&db, 3, store, vec![put(2.0, "b")], true),
        vec![key_reply(2.0)]
    );
    assert_eq!(commit(&db, 3), Ok(()));
}

/// Index keys for other indexes than exactly the store's mean that script's metadata isn't the
/// database's, so the put fails its transaction, which rolls back.
fn test_index_keys_for_other_indexes_fail_the_transaction<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let other = create_store(&db, "other", None, false).unwrap();
    let key_path = KeyPath::String("a".to_owned());
    let index = create_index(&db, store, "a".to_owned(), key_path.clone(), false, false).unwrap();
    let other_index = create_index(&db, other, "a".to_owned(), key_path, false, false).unwrap();
    let mismatches = [
        vec![],
        no_index_keys(&[index, index]),
        no_index_keys(&[index, other_index]),
        no_index_keys(&[other_index]),
    ];
    for (transaction, index_keys) in (1..).zip(mismatches) {
        let replies = run_indexed_puts(
            &db,
            transaction,
            store,
            vec![
                (Some(number(1.0)), vec![], no_index_keys(&[index])),
                (Some(number(2.0)), vec![], index_keys),
                (Some(number(3.0)), vec![], no_index_keys(&[index])),
            ],
            false,
        );
        assert_eq!(replies[0], key_reply(1.0));
        assert!(matches!(replies[1], Err(BackendError::DbErr(_))));
        assert!(replies[2].is_err());
        assert!(commit(&db, transaction).is_err());
        assert_eq!(stored_keys(&db, store), vec![]);
    }
}

/// The index population events of a batch.
type PopulationEvents = Vec<(i64, IndexPopulationEvent)>;

/// Runs a batch of the upgrade `transaction`, waits until it has run, and returns the index
/// population events it gave.
fn run_upgrade_batch<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    requests: Vec<KvsOperation>,
) -> PopulationEvents {
    let (done_sender, done_receiver) = mpsc::channel();
    let (schema_failures, _) = mpsc::channel();
    let (populations, events) = mpsc::channel();
    db.process_transaction(
        txn(transaction),
        KvsTransaction {
            mode: IndexedDBTxnMode::Versionchange,
            requests: VecDeque::from(requests),
        },
        routed_results_with_populations(transaction, schema_failures, Some(populations)),
        Box::new(move || {
            let _ = done_sender.send(());
        }),
    );
    done_receiver
        .recv_timeout(TIMEOUT)
        .expect("The batch should complete");
    events.try_iter().collect()
}

/// Creates the index `index` of `store` on the key path `key_path`, populated from the store's
/// records.
fn create_populated_index(
    store: i64,
    index: i64,
    key_path: &str,
    unique: bool,
    multi_entry: bool,
) -> KvsOperation {
    schema(
        store,
        AsyncSchemaOperation::CreateIndex {
            index_id: index,
            name: key_path.into(),
            key_path: KeyPath::String(key_path.to_owned()),
            unique,
            multi_entry,
            populate: true,
        },
    )
}

fn population_step(store: i64, index: i64, step: IndexPopulationStep) -> KvsOperation {
    schema(
        store,
        AsyncSchemaOperation::PopulateIndex {
            index_id: index,
            step,
        },
    )
}

/// The steps that script takes after the batch of records `records` of the population of
/// `index`: it reads the next records unless these are the last, and sends the keys that
/// `keys` gives each record.
fn population_steps(
    store: i64,
    index: i64,
    records: &[IndexedDBValue],
    last: bool,
    keys: &impl Fn(&IndexedDBValue) -> Vec<IndexedDBKeyType>,
) -> Vec<KvsOperation> {
    let mut steps = Vec::new();
    if !last {
        let after = records.last().unwrap().primary_key.clone();
        steps.push(population_step(
            store,
            index,
            IndexPopulationStep::ReadRecords { after },
        ));
    }
    let records = records
        .iter()
        .map(|record| IndexedDBRecordIndexKeys {
            primary_key: record.primary_key.clone(),
            keys: keys(record)
                .into_iter()
                .map(IndexedDBIndexKey::Key)
                .collect(),
        })
        .collect();
    steps.push(population_step(
        store,
        index,
        IndexPopulationStep::SetIndexKeys {
            records,
            done: last,
        },
    ));
    steps
}

/// Populates `index` of `store` in the upgrade `transaction` as script does, from `events`, the
/// events of the batch that created it, with `keys` computing the index keys of a record.
/// Returns how the population ended, the population events of the other indexes on the way, and
/// the number of record batches.
fn populate<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    store: i64,
    index: i64,
    mut events: PopulationEvents,
    keys: impl Fn(&IndexedDBValue) -> Vec<IndexedDBKeyType>,
) -> (BackendResult<()>, PopulationEvents, usize) {
    let mut others = Vec::new();
    let mut batches = 0;
    loop {
        let mut next = None;
        let mut done = None;
        for (index_id, event) in events {
            match event {
                _ if index_id != index || done.is_some() => others.push((index_id, event)),
                IndexPopulationEvent::Done(result) => done = Some(result),
                IndexPopulationEvent::Records { records, last } => {
                    assert!(next.is_none(), "One batch of records at a time");
                    next = Some((records, last));
                },
            }
        }
        if let Some(result) = done {
            return (result, others, batches);
        }
        let (records, last) = next.expect("The population should go on");
        batches += 1;
        let steps = population_steps(store, index, &records, last, &keys);
        events = run_upgrade_batch(db, transaction, steps);
    }
}

/// The number that a test value is: its bytes, big-endian.
fn value_number(record: &IndexedDBValue) -> u32 {
    u32::from_be_bytes(record.value[..4].try_into().unwrap())
}

/// The index records that populating an index gives a store's records are those that puts give
/// them in an index that exists already, with several batches of records, a multiEntry index, a
/// unique index and records without keys.
fn test_population_gives_the_index_records_that_puts_give<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    // The keys of the record `n` in the index `kind`: by parity, none for multiples of ten; by
    // digit, multiEntry; by name, unique.
    let keys = |kind: usize, n: u32| -> Vec<IndexedDBKeyType> {
        match kind {
            0 if n.is_multiple_of(10) => vec![],
            0 => vec![number(f64::from(n % 2))],
            1 => {
                let mut digits: Vec<u8> = n.to_string().bytes().map(|digit| digit - b'0').collect();
                digits.sort();
                digits.dedup();
                digits
                    .into_iter()
                    .map(|digit| number(digit.into()))
                    .collect()
            },
            _ => vec![string(&format!("n{n}"))],
        }
    };
    let options = [
        ("parity", false, false),
        ("digit", false, true),
        ("name", true, false),
    ];

    let indexed = create_store(&db, "indexed", None, false).unwrap();
    let indexes: Vec<i64> = options
        .iter()
        .map(|(name, unique, multi_entry)| {
            let key_path = KeyPath::String((*name).to_owned());
            create_index(
                &db,
                indexed,
                (*name).to_owned(),
                key_path,
                *unique,
                *multi_entry,
            )
            .unwrap()
        })
        .collect();
    let populated = create_store(&db, "populated", None, false).unwrap();
    let count = 2 * queries::POPULATION_BATCH_RECORDS as u32 + 234;
    let puts: Vec<_> = (1..=count)
        .map(|n| {
            let index_keys = (0..)
                .zip(&indexes)
                .map(|(kind, index)| index_keys(*index, keys(kind, n)))
                .collect();
            (
                Some(number(f64::from(n))),
                n.to_be_bytes().to_vec(),
                index_keys,
            )
        })
        .collect();
    let plain_puts = puts
        .iter()
        .map(|(key, value, _)| (key.clone(), value.clone()))
        .collect();
    assert!(
        run_indexed_puts(&db, 1, indexed, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert!(
        run_puts(&db, 1, populated, plain_puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let mut new_indexes = Vec::new();
    for (kind, (name, unique, multi_entry)) in options.iter().enumerate() {
        let index = indexes[2] + 1 + kind as i64;
        let events = run_upgrade_batch(
            &db,
            2,
            vec![create_populated_index(
                populated,
                index,
                name,
                *unique,
                *multi_entry,
            )],
        );
        let (result, others, batches) = populate(&db, 2, populated, index, events, |record| {
            keys(kind, value_number(record))
        });
        assert_eq!(result, Ok(()));
        assert!(others.is_empty());
        assert_eq!(batches, 3);
        new_indexes.push(index);
    }
    assert_eq!(commit(&db, 2), Ok(()));
    for (index, new_index) in indexes.iter().zip(&new_indexes) {
        let records = index_records_of(&db, indexed, *index);
        assert!(!records.is_empty());
        assert_eq!(index_records_of(&db, populated, *new_index), records);
    }
    assert_eq!(dangling_index_records(&db), 0);
}

/// A unique index that gets a key twice while it is populated, here from records in different
/// batches, fails the upgrade transaction with Constraint: the requests that waited for the
/// population and later steps fail, the commit fails, and aborting it leaves the database as it
/// was.
fn test_population_unique_conflict_fails_the_upgrade<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let count = queries::POPULATION_BATCH_RECORDS as u32 + 150;
    let puts = (1..=count)
        .map(|n| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));
    let unique_key = |n: u32| string(&format!("k{}", if n == count - 1 { 3 } else { n }));

    db.upgrade_version(txn(2), 2);
    let (before, after) = (get_callback::<PutReply>(), get_callback::<PutReply>());
    let index = 1;
    let events = run_upgrade_batch(
        &db,
        2,
        vec![
            put_request(
                store,
                before.0,
                number(0.0),
                0u32.to_be_bytes().to_vec(),
                false,
            ),
            create_populated_index(store, index, "k", true, false),
            indexed_put_request(
                store,
                after.0,
                Some(number(-1.0)),
                vec![],
                false,
                vec![index_keys(index, [string("new")])],
            ),
        ],
    );
    assert_eq!(recv(&before.1), key_reply(0.0));
    assert!(
        after.1.try_recv().is_err(),
        "The put waits for the population"
    );
    let (result, _, batches) = populate(&db, 2, store, index, events, |record| {
        vec![unique_key(value_number(record))]
    });
    assert_eq!(result, Err(BackendError::Constraint));
    assert_eq!(batches, 2);
    assert_eq!(recv(&after.1), Err(BackendError::Constraint));
    let events = run_upgrade_batch(
        &db,
        2,
        vec![population_step(
            store,
            index,
            IndexPopulationStep::ReadRecords { after: number(1.0) },
        )],
    );
    assert!(matches!(
        &events[..],
        [(1, IndexPopulationEvent::Done(Err(BackendError::Constraint)))]
    ));
    assert_eq!(commit(&db, 2), Err(BackendError::Constraint));
    abort(&db, 2);

    assert!(db.object_stores()[0].indexes.is_empty());
    assert_eq!(index_record_count(&db, index), 0);
    assert_eq!(stored_keys(&db, store).len(), count as usize);
    assert_eq!(stored_keys(&db, store)[0], number(1.0));
}

/// A unique multiEntry index gets each of a record's keys once and fails if two records share
/// one.
fn test_population_of_a_unique_multi_entry_index<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let puts = (1..=3)
        .map(|n: u32| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));
    let tags = |third: &'static str| {
        move |record: &IndexedDBValue| match value_number(record) {
            1 => vec![string("a"), string("a"), string("b")],
            2 => vec![string("c")],
            _ => vec![string(third), string("d")],
        }
    };

    db.upgrade_version(txn(2), 2);
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, 1, "t", true, true)],
    );
    assert_eq!(populate(&db, 2, store, 1, events, tags("e")).0, Ok(()));
    assert_eq!(
        index_records_of(&db, store, 1),
        string_records(&[("a", 1.0), ("b", 1.0), ("c", 2.0), ("d", 3.0), ("e", 3.0)])
    );
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, 2, "u", true, true)],
    );
    assert_eq!(
        populate(&db, 2, store, 2, events, tags("b")).0,
        Err(BackendError::Constraint)
    );
    abort(&db, 2);
}

/// The requests and schema operations that come after the creation of an index that is
/// populated run once the population is done, in order, and see the whole index: a put whose
/// unique key an existing record has fails, and one after the index's deletion doesn't.
fn test_requests_after_create_index_wait_for_the_population<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let puts = (1..=3)
        .map(|n: u32| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let index = 1;
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, index, "k", true, false)],
    );
    let (order_sender, order) = mpsc::channel();
    let put = |id, key: f64, index_key: Option<&str>| {
        let index_keys = index_key.map(|key| vec![index_keys(index, [string(key)])]);
        indexed_put_request(
            store,
            tagged(&order_sender, id),
            Some(number(key)),
            vec![],
            false,
            index_keys.unwrap_or_default(),
        )
    };
    assert!(
        run_upgrade_batch(
            &db,
            2,
            vec![
                put(4, 4.0, Some("k1")),
                put(5, 5.0, Some("k5")),
                schema(store, AsyncSchemaOperation::DeleteIndex { index_id: index }),
                put(6, 6.0, None),
            ],
        )
        .is_empty()
    );
    assert!(
        order.try_recv().is_err(),
        "The requests wait for the population"
    );
    let (result, _, _) = populate(&db, 2, store, index, events, |record| {
        vec![string(&format!("k{}", value_number(record)))]
    });
    assert_eq!(result, Ok(()));
    assert_eq!(order.try_iter().collect::<Vec<_>>(), vec![4, 5, 6]);
    assert_eq!(commit(&db, 2), Ok(()));
    assert_eq!(stored_keys(&db, store), number_keys([1, 2, 3, 5, 6]));
    assert!(db.object_stores()[0].indexes.is_empty());
    assert_eq!(index_record_count(&db, index), 0);
}

/// The results of the requests that waited, and the unique conflict of one of them with an
/// existing record.
fn test_waiting_put_sees_the_populated_unique_index<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let puts = vec![(Some(number(1.0)), 1u32.to_be_bytes().to_vec())];
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let (conflict, other) = (get_callback::<PutReply>(), get_callback::<PutReply>());
    let index = 1;
    let events = run_upgrade_batch(
        &db,
        2,
        vec![
            create_populated_index(store, index, "k", true, false),
            indexed_put_request(
                store,
                conflict.0,
                Some(number(2.0)),
                vec![],
                false,
                vec![index_keys(index, [string("k1")])],
            ),
            indexed_put_request(
                store,
                other.0,
                Some(number(3.0)),
                vec![],
                false,
                vec![index_keys(index, [string("k3")])],
            ),
        ],
    );
    let (result, _, _) = populate(&db, 2, store, index, events, |record| {
        vec![string(&format!("k{}", value_number(record)))]
    });
    assert_eq!(result, Ok(()));
    assert_eq!(
        recv(&conflict.1),
        Ok(PutItemResult::UniqueIndexKeyExists(index))
    );
    assert_eq!(recv(&other.1), key_reply(3.0));
    assert_eq!(commit(&db, 2), Ok(()));
    assert_eq!(
        index_records_of(&db, store, index),
        string_records(&[("k1", 1.0), ("k3", 3.0)])
    );
}

/// Indexes created one after another are populated one after another, each after the requests
/// that came before its creation, so the second sees a record put between the two.
fn test_indexes_created_one_after_another_populate_in_turn<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let puts = (1..=3)
        .map(|n: u32| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let (first, second) = (1, 2);
    let put = get_callback::<PutReply>();
    let events = run_upgrade_batch(
        &db,
        2,
        vec![
            create_populated_index(store, first, "a", false, false),
            indexed_put_request(
                store,
                put.0,
                Some(number(4.0)),
                4u32.to_be_bytes().to_vec(),
                false,
                vec![index_keys(first, [number(4.0)])],
            ),
            create_populated_index(store, second, "b", false, false),
        ],
    );
    assert!(events.iter().all(|(index, _)| *index == first));
    let same = |record: &IndexedDBValue| vec![number(f64::from(value_number(record)))];
    let (result, others, _) = populate(&db, 2, store, first, events, same);
    assert_eq!(result, Ok(()));
    assert_eq!(recv(&put.1), key_reply(4.0));
    let [
        (
            _,
            IndexPopulationEvent::Records {
                records,
                last: true,
            },
        ),
    ] = &others[..]
    else {
        panic!("The second population should start: {others:?}");
    };
    assert_eq!(records.len(), 4);
    let (result, _, _) = populate(&db, 2, store, second, others, same);
    assert_eq!(result, Ok(()));
    assert_eq!(commit(&db, 2), Ok(()));
    let records: Vec<_> = (1..=4)
        .map(|n| (number(f64::from(n)), number(f64::from(n))))
        .collect();
    assert_eq!(index_records_of(&db, store, first), records);
    assert_eq!(index_records_of(&db, store, second), records);
}

/// Aborting the upgrade transaction during a population rolls it back and drops the requests
/// that waited, and the database's next writer runs.
fn test_abort_during_population_leaves_nothing<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let count = queries::POPULATION_BATCH_RECORDS as u32 + 10;
    let puts = (1..=count)
        .map(|n| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let index = 1;
    let waiting = get_callback::<PutReply>();
    let events = run_upgrade_batch(
        &db,
        2,
        vec![
            create_populated_index(store, index, "k", false, false),
            put_request(store, waiting.0, number(0.0), vec![], false),
        ],
    );
    let [
        (
            _,
            IndexPopulationEvent::Records {
                records,
                last: false,
            },
        ),
    ] = &events[..]
    else {
        panic!("The population should start: {events:?}");
    };
    let steps = population_steps(store, index, records, false, &|_| vec![number(1.0)]);
    assert_eq!(run_upgrade_batch(&db, 2, steps).len(), 1);
    assert!(index_record_count(&db, index) > 0);
    abort(&db, 2);
    assert!(waiting.1.recv_timeout(Duration::from_millis(100)).is_err());

    assert_eq!(index_record_count(&db, index), 0);
    assert!(db.object_stores()[0].indexes.is_empty());
    let replies = run_puts(&db, 3, store, vec![(Some(number(0.0)), vec![])], false);
    assert_eq!(replies, vec![key_reply(0.0)]);
    assert_eq!(commit(&db, 3), Ok(()));
    assert_eq!(stored_keys(&db, store).len(), count as usize + 1);
}

/// The commit of an upgrade transaction that failed while it populated an index fails and drops
/// the population, so that the next upgrade can populate an index without an abort in between.
fn test_commit_of_a_transaction_that_failed_while_populating<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_faulty_engine::<X>(&dir);
    db.upgrade_version(txn(2), 2);
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(TEST_STORE, 1, "k", false, false)],
    );
    assert!(
        matches!(&events[..], [(1, IndexPopulationEvent::Records { .. })]),
        "The population should start: {events:?}"
    );
    // A job outside the transaction's requests ends its SQL transaction; the fault clears
    // itself when it fires.
    faults_for(dir.path()).lock().unwrap().statement = Some(("FROM object_store", 0));
    db.spawn_on_connection(|executor| {
        let _ = queries::object_store(executor, TEST_STORE);
    });
    assert!(commit(&db, 2).is_err());

    db.upgrade_version(txn(3), 2);
    let events = run_upgrade_batch(
        &db,
        3,
        vec![create_populated_index(TEST_STORE, 1, "k", false, false)],
    );
    let (result, _, batches) = populate(&db, 3, TEST_STORE, 1, events, |_| vec![number(1.0)]);
    assert_eq!((result, batches), (Ok(()), 1));
    assert_eq!(commit(&db, 3), Ok(()));
    assert_eq!(index_record_count(&db, 1), 1);
}

/// An index of an empty store is populated in one step, and a step for an index that isn't
/// being populated fails the transaction.
fn test_population_of_an_empty_store_and_a_step_out_of_turn<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();

    db.upgrade_version(txn(1), 1);
    let events = run_upgrade_batch(
        &db,
        1,
        vec![create_populated_index(store, 1, "a", false, false)],
    );
    let [
        (
            1,
            IndexPopulationEvent::Records {
                records,
                last: true,
            },
        ),
    ] = &events[..]
    else {
        panic!("The population should start with no records: {events:?}");
    };
    assert!(records.is_empty());
    let (result, _, batches) = populate(&db, 1, store, 1, events, |_| vec![]);
    assert_eq!((result, batches), (Ok(()), 1));

    let put = get_callback::<PutReply>();
    let events = run_upgrade_batch(
        &db,
        1,
        vec![
            population_step(
                store,
                1,
                IndexPopulationStep::SetIndexKeys {
                    records: vec![],
                    done: true,
                },
            ),
            put_request(store, put.0, number(1.0), vec![], false),
        ],
    );
    assert!(matches!(
        &events[..],
        [(1, IndexPopulationEvent::Done(Err(BackendError::DbErr(_))))]
    ));
    assert!(recv(&put.1).is_err());
    assert!(commit(&db, 1).is_err());
}

/// A unique index's keys in a batch of records are checked against the records of earlier
/// batches in several statements, and a conflict in any of them fails the population.
fn test_population_finds_unique_conflicts_in_every_check<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let count = queries::POPULATION_BATCH_RECORDS as u32 + 300;
    let puts = (1..=count)
        .map(|n| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));
    // Two keys per record; the last record's first key is the first record's, which the second
    // batch's 600 keys only reach in their second check.
    let keys = |last_conflicts: bool| {
        move |record: &IndexedDBValue| {
            let n = f64::from(value_number(record));
            match n as u32 {
                n if n == count && last_conflicts => vec![number(2.0), number(f64::from(2 * n))],
                _ => vec![number(2.0 * n), number(2.0 * n + 1.0)],
            }
        }
    };

    db.upgrade_version(txn(2), 2);
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, 1, "a", true, true)],
    );
    let (result, _, batches) = populate(&db, 2, store, 1, events, keys(false));
    assert_eq!((result, batches), (Ok(()), 2));
    assert_eq!(index_record_count(&db, 1), 2 * i64::from(count));
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, 2, "b", true, true)],
    );
    let (result, _, _) = populate(&db, 2, store, 2, events, keys(true));
    assert_eq!(result, Err(BackendError::Constraint));
    abort(&db, 2);
}

/// A batch of records for a population stops early once their values reach the size limit.
fn test_population_batches_are_limited_in_size<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let puts = (1..=10)
        .map(|n| (Some(number(f64::from(n))), vec![n as u8; 300 * 1024]))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    let mut connection = db.connection();
    let object_store = queries::object_store(&mut *connection, store)
        .unwrap()
        .unwrap();
    let mut after = None;
    let mut batches = Vec::new();
    loop {
        let (records, last) =
            queries::population_records(&mut *connection, &object_store, after.as_ref()).unwrap();
        batches.push((records.len(), last));
        if last {
            break;
        }
        after = Some(records.last().unwrap().primary_key.clone());
    }
    assert_eq!(batches, vec![(4, false), (4, false), (2, true)]);
}

fn read_request<T>(
    store: i64,
    request_id: ReplyId<T>,
    operation: AsyncReadOnlyOperation,
) -> KvsOperation {
    request(store, request_id, AsyncOperation::ReadOnly(operation))
}

/// Runs `operation`, which reads `store` or one of its indexes, as the only request of a batch
/// of the readonly `transaction`, and returns its result.
fn read<X: SqlExecutor, T: TestReply>(
    db: &SqlEngine<X>,
    transaction: u64,
    store: i64,
    operation: AsyncReadOnlyOperation,
) -> T {
    let (id, receiver) = get_callback::<T>();
    let read = read_request(store, id, operation);
    run_batch(db, transaction, IndexedDBTxnMode::Readonly, vec![read]);
    recv(&receiver)
}

fn date(time: f64) -> IndexedDBKeyType {
    IndexedDBKeyType::Date(time)
}

/// The values of `values`, each with its number key, as the test stores them.
fn number_values(values: &[(f64, u8)]) -> Vec<IndexedDBValue> {
    values
        .iter()
        .map(|(key, value)| stored_value(*key, vec![*value]))
        .collect()
}

/// Reads of an index see its records in (index key, primary key) order, whatever the order of
/// the puts and the types of the keys, and stop at a count; a unique index's and a multiEntry
/// index's reads too, and a read of an index the store doesn't have fails alone.
fn test_index_reads_go_in_key_then_primary_key_order<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let index = |name: &str, unique, multi_entry| {
        let key_path = KeyPath::String(name.to_owned());
        create_index(&db, store, name.to_owned(), key_path, unique, multi_entry).unwrap()
    };
    let (by_kind, by_id, by_tag) = (
        index("kind", false, false),
        index("id", true, false),
        index("tag", false, true),
    );
    // Index keys of every type, in key order: numbers, dates, strings (by code unit), binary and
    // arrays; each kind has two records, put with the larger primary key first.
    let kinds = [
        number(f64::NEG_INFINITY),
        number(-1.5),
        number(0.0),
        number(f64::INFINITY),
        date(-1.0),
        date(0.0),
        string(""),
        string("A"),
        IndexedDBKeyType::String(units(&[0xD800])),
        IndexedDBKeyType::String(units(&[0xFFFF])),
        IndexedDBKeyType::Binary(vec![]),
        IndexedDBKeyType::Binary(vec![0, 255]),
        IndexedDBKeyType::Array(vec![]),
        IndexedDBKeyType::Array(vec![number(1.0), IndexedDBKeyType::Array(vec![])]),
    ];
    let mut puts = Vec::new();
    for (n, kind) in kinds.iter().enumerate().rev() {
        for primary_key in [2 * n + 1, 2 * n] {
            let tags = [
                string("all"),
                string(if n % 2 == 0 { "even" } else { "odd" }),
            ];
            puts.push((
                Some(number(primary_key as f64)),
                vec![primary_key as u8],
                vec![
                    index_keys(by_kind, [kind.clone()]),
                    index_keys(by_id, [number(-(primary_key as f64) - 1.0)]),
                    index_keys(by_tag, tags),
                ],
            ));
        }
    }
    let record_count = 2 * kinds.len();
    let replies = run_indexed_puts(&db, 1, store, puts, false);
    assert!(replies.iter().all(Result::is_ok), "{replies:?}");
    assert_eq!(commit(&db, 1), Ok(()));

    let all = IndexedDBKeyRange::default();
    let get_all_keys = |index, key_range, count| -> Vec<IndexedDBKeyType> {
        read::<X, BackendResult<Vec<IndexedDBKeyType>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetAllKeys {
                index: Some(index),
                key_range,
                count,
                direction: IndexedDBCursorDirection::Next,
            },
        )
        .unwrap()
    };
    let count = |index, key_range| -> u64 {
        read::<X, BackendResult<u64>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::Count { index, key_range },
        )
        .unwrap()
    };
    let primary_keys = |keys: std::ops::Range<usize>| -> Vec<IndexedDBKeyType> {
        keys.map(|key| number(key as f64)).collect()
    };

    // The primary keys in (index key, primary key) order, also with a count of 0 (every record).
    assert_eq!(
        get_all_keys(by_kind, all.clone(), None),
        primary_keys(0..record_count)
    );
    assert_eq!(
        get_all_keys(by_kind, all.clone(), Some(0)),
        primary_keys(0..record_count)
    );
    assert_eq!(
        get_all_keys(by_kind, all.clone(), Some(3)),
        primary_keys(0..3)
    );
    // A range across key types: from the first date up to the strings, without "A".
    let range = IndexedDBKeyRange::new(Some(date(-1.0)), Some(string("A")), false, true);
    assert_eq!(
        get_all_keys(by_kind, range.clone(), None),
        primary_keys(8..14)
    );
    assert_eq!(count(Some(by_kind), range.clone()), 6);
    assert_eq!(
        get_all_keys(
            by_kind,
            IndexedDBKeyRange::only(IndexedDBKeyType::Array(vec![])),
            None
        ),
        primary_keys(24..26)
    );
    // The values, with their primary keys, of the first records in the range.
    assert_eq!(
        read::<X, BackendResult<Vec<IndexedDBValue>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetAllItems {
                index: Some(by_kind),
                key_range: range.clone(),
                count: Some(3),
                direction: IndexedDBCursorDirection::Next,
            },
        ),
        Ok(number_values(&[(8.0, 8), (9.0, 9), (10.0, 10)]))
    );
    // get and getKey take the first record: the lowest primary key of the lowest index key.
    assert_eq!(
        read::<X, GetReply>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetItem {
                index: Some(by_kind),
                key_range: range.clone(),
            },
        ),
        Ok(Some(stored_value(8.0, vec![8])))
    );
    assert_eq!(
        read::<X, BackendResult<Option<IndexedDBKeyType>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetKey {
                index: Some(by_kind),
                key_range: IndexedDBKeyRange::only(string("A")),
            },
        ),
        Ok(Some(number(14.0)))
    );
    assert_eq!(
        read::<X, BackendResult<Option<IndexedDBKeyType>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetKey {
                index: Some(by_kind),
                key_range: IndexedDBKeyRange::only(string("none")),
            },
        ),
        Ok(None)
    );

    // The unique index's keys are the negated primary keys, minus one.
    let descending: Vec<_> = primary_keys(0..record_count).into_iter().rev().collect();
    assert_eq!(get_all_keys(by_id, all.clone(), None), descending);
    assert_eq!(
        get_all_keys(
            by_id,
            IndexedDBKeyRange::new(Some(number(-4.0)), Some(number(0.0)), true, false),
            None
        ),
        primary_keys(0..3).into_iter().rev().collect::<Vec<_>>()
    );
    assert_eq!(count(Some(by_id), all.clone()), record_count as u64);

    // A record has one record per subkey in a multiEntry index.
    assert_eq!(count(Some(by_tag), all.clone()), 2 * record_count as u64);
    assert_eq!(
        count(Some(by_tag), IndexedDBKeyRange::only(string("all"))),
        record_count as u64
    );
    let even: Vec<_> = (0..kinds.len())
        .step_by(2)
        .flat_map(|n| [2 * n, 2 * n + 1])
        .map(|key| number(key as f64))
        .collect();
    assert_eq!(
        get_all_keys(by_tag, IndexedDBKeyRange::only(string("even")), None),
        even
    );
    assert_eq!(count(None, all.clone()), record_count as u64);

    // A read of an index that the store doesn't have fails, and the reads after it run.
    let (missing, missing_reply) = get_callback::<BackendResult<u64>>();
    let (after, after_reply) = get_callback::<BackendResult<u64>>();
    let count_request = |id, index| {
        request(
            store,
            id,
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                index,
                key_range: IndexedDBKeyRange::default(),
            }),
        )
    };
    run_batch(
        &db,
        2,
        IndexedDBTxnMode::Readonly,
        vec![
            count_request(missing, Some(by_tag + 1)),
            count_request(after, Some(by_kind)),
        ],
    );
    assert!(matches!(recv(&missing_reply), Err(BackendError::DbErr(_))));
    assert_eq!(recv(&after_reply), Ok(record_count as u64));
}

/// Index reads in a readwrite transaction see the puts, overwrites, deletes and clears before
/// them, and an aborted transaction's writes are gone from the index afterwards.
fn test_index_reads_see_the_writes_before_them<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let index = create_index(
        &db,
        store,
        "name".to_owned(),
        KeyPath::String("name".to_owned()),
        false,
        false,
    )
    .unwrap();
    let (order_sender, order) = mpsc::channel();
    let put = |id, key: f64, name: &str| {
        indexed_put_request(
            store,
            tagged(&order_sender, id),
            Some(number(key)),
            vec![key as u8],
            true,
            vec![index_keys(index, [string(name)])],
        )
    };
    let (names, names_reply) = get_callback::<BackendResult<Vec<IndexedDBKeyType>>>();
    let (moved, moved_reply) = get_callback::<BackendResult<Vec<IndexedDBValue>>>();
    let (deleted, deleted_reply) = get_callback::<BackendResult<u64>>();
    let (cleared, cleared_reply) = get_callback::<BackendResult<u64>>();
    let (remove_id, _remove) = get_callback::<BackendResult<()>>();
    let (clear_id, _clear) = get_callback::<BackendResult<()>>();
    let ann = || IndexedDBKeyRange::only(string("ann"));
    run_batch(
        &db,
        1,
        IndexedDBTxnMode::Readwrite,
        vec![
            put(1, 1.0, "ann"),
            put(2, 2.0, "bob"),
            put(3, 3.0, "ann"),
            read_request(
                store,
                names,
                AsyncReadOnlyOperation::GetAllKeys {
                    index: Some(index),
                    key_range: ann(),
                    count: None,
                    direction: IndexedDBCursorDirection::Next,
                },
            ),
            put(4, 1.0, "cid"),
            read_request(
                store,
                moved,
                AsyncReadOnlyOperation::GetAllItems {
                    index: Some(index),
                    key_range: IndexedDBKeyRange::default(),
                    count: None,
                    direction: IndexedDBCursorDirection::Next,
                },
            ),
            remove_request(store, remove_id, IndexedDBKeyRange::only(number(3.0))),
            read_request(
                store,
                deleted,
                AsyncReadOnlyOperation::Count {
                    index: Some(index),
                    key_range: ann(),
                },
            ),
            request(
                store,
                clear_id,
                AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear),
            ),
            read_request(
                store,
                cleared,
                AsyncReadOnlyOperation::Count {
                    index: Some(index),
                    key_range: IndexedDBKeyRange::default(),
                },
            ),
        ],
    );
    assert_eq!(order.try_iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    assert_eq!(recv(&names_reply), Ok(number_keys([1, 3])));
    assert_eq!(
        recv(&moved_reply),
        Ok(number_values(&[(3.0, 3), (2.0, 2), (1.0, 1)]))
    );
    assert_eq!(recv(&deleted_reply), Ok(0));
    assert_eq!(recv(&cleared_reply), Ok(0));
    assert_eq!(commit(&db, 1), Ok(()));

    assert!(
        run_batch(
            &db,
            2,
            IndexedDBTxnMode::Readwrite,
            vec![put(5, 5.0, "ann")]
        )
        .is_empty()
    );
    abort(&db, 2);
    let count: BackendResult<u64> = read_count(&db, 3, store, index);
    assert_eq!(count, Ok(0));
}

fn read_count<X: SqlExecutor>(
    db: &SqlEngine<X>,
    transaction: u64,
    store: i64,
    index: i64,
) -> BackendResult<u64> {
    read(
        db,
        transaction,
        store,
        AsyncReadOnlyOperation::Count {
            index: Some(index),
            key_range: IndexedDBKeyRange::default(),
        },
    )
}

/// Index reads made after a createIndex that populates the index wait until every batch of the
/// population is stored, and then see the whole index.
fn test_index_reads_after_create_index_see_the_whole_index<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let records = queries::POPULATION_BATCH_RECORDS as u32 * 2 + 1;
    let puts = (0..records)
        .map(|n| (Some(number(f64::from(n))), n.to_be_bytes().to_vec()))
        .collect();
    assert!(
        run_puts(&db, 1, store, puts, false)
            .iter()
            .all(Result::is_ok)
    );
    assert_eq!(commit(&db, 1), Ok(()));

    db.upgrade_version(txn(2), 2);
    let index = 1;
    let events = run_upgrade_batch(
        &db,
        2,
        vec![create_populated_index(store, index, "parity", false, false)],
    );
    let (keys, keys_reply) = get_callback::<BackendResult<Vec<IndexedDBKeyType>>>();
    let (count, count_reply) = get_callback::<BackendResult<u64>>();
    let (first, first_reply) = get_callback::<GetReply>();
    let odd = || IndexedDBKeyRange::only(number(1.0));
    assert!(
        run_upgrade_batch(
            &db,
            2,
            vec![
                read_request(
                    store,
                    keys,
                    AsyncReadOnlyOperation::GetAllKeys {
                        index: Some(index),
                        key_range: odd(),
                        count: None,
                        direction: IndexedDBCursorDirection::Next,
                    },
                ),
                read_request(
                    store,
                    count,
                    AsyncReadOnlyOperation::Count {
                        index: Some(index),
                        key_range: IndexedDBKeyRange::default(),
                    },
                ),
                read_request(
                    store,
                    first,
                    AsyncReadOnlyOperation::GetItem {
                        index: Some(index),
                        key_range: odd(),
                    },
                ),
            ],
        )
        .is_empty()
    );
    assert!(
        keys_reply.try_recv().is_err(),
        "The reads wait for the population"
    );
    let (result, _, batches) = populate(&db, 2, store, index, events, |record| {
        vec![number(f64::from(value_number(record) % 2))]
    });
    assert_eq!(result, Ok(()));
    assert_eq!(batches, 3);
    let odd_keys: Vec<_> = (1..records)
        .step_by(2)
        .map(|n| number(f64::from(n)))
        .collect();
    assert_eq!(recv(&keys_reply), Ok(odd_keys));
    assert_eq!(recv(&count_reply), Ok(u64::from(records)));
    assert_eq!(
        recv(&first_reply),
        Ok(Some(stored_value(1.0, 1u32.to_be_bytes().to_vec())))
    );
    assert_eq!(commit(&db, 2), Ok(()));
}

/// A record of a cursor's source in the cursor tests: (key, value), where an index's record's
/// value is the primary key and a store's record's value is its key.
type SourceRecord = (IndexedDBKeyType, IndexedDBKeyType);

/// <https://w3c.github.io/IndexedDB/#iterate-a-cursor> step 9 as the spec words it, over
/// `records`, the records of the source in (key, value) order: the record that the steps find.
fn spec_found_record(
    records: &[SourceRecord],
    is_index: bool,
    direction: IndexedDBCursorDirection,
    range: &IndexedDBKeyRange,
    step: &IndexedDBCursorStep,
) -> Option<SourceRecord> {
    let IndexedDBCursorStep {
        key,
        primary_key,
        count,
        ..
    } = step;
    let mut position = step.position.clone();
    let mut object_store_position = step.object_store_position.clone();
    let mut found = None;
    for _ in 0..*count {
        let forward = |record: &&SourceRecord| {
            let (record_key, record_value) = *record;
            key.as_ref().is_none_or(|key| record_key >= key) &&
                primary_key.as_ref().is_none_or(|primary_key| {
                    let key = key.as_ref().unwrap();
                    (record_key == key && record_value >= primary_key) || record_key > key
                }) &&
                (is_index ||
                    position
                        .as_ref()
                        .is_none_or(|position| record_key > position)) &&
                (!is_index ||
                    position.as_ref().is_none_or(|position| {
                        (record_key == position &&
                            object_store_position
                                .as_ref()
                                .is_some_and(|osp| record_value > osp)) ||
                            record_key > position
                    })) &&
                range.contains(record_key)
        };
        let forward_unique = |record: &&SourceRecord| {
            let record_key = &record.0;
            key.as_ref().is_none_or(|key| record_key >= key) &&
                position
                    .as_ref()
                    .is_none_or(|position| record_key > position) &&
                range.contains(record_key)
        };
        let backward = |record: &&SourceRecord| {
            let (record_key, record_value) = *record;
            key.as_ref().is_none_or(|key| record_key <= key) &&
                primary_key.as_ref().is_none_or(|primary_key| {
                    let key = key.as_ref().unwrap();
                    (record_key == key && record_value <= primary_key) || record_key < key
                }) &&
                (is_index ||
                    position
                        .as_ref()
                        .is_none_or(|position| record_key < position)) &&
                (!is_index ||
                    position.as_ref().is_none_or(|position| {
                        (record_key == position &&
                            object_store_position
                                .as_ref()
                                .is_some_and(|osp| record_value < osp)) ||
                            record_key < position
                    })) &&
                range.contains(record_key)
        };
        let backward_unique = |record: &&SourceRecord| {
            let record_key = &record.0;
            key.as_ref().is_none_or(|key| record_key <= key) &&
                position
                    .as_ref()
                    .is_none_or(|position| record_key < position) &&
                range.contains(record_key)
        };
        found = match direction {
            IndexedDBCursorDirection::Next => records.iter().find(forward),
            IndexedDBCursorDirection::NextUnique => records.iter().find(forward_unique),
            IndexedDBCursorDirection::Prev => records.iter().rev().find(backward),
            IndexedDBCursorDirection::PrevUnique => records
                .iter()
                .rev()
                .find(backward_unique)
                .map(|temp| records.iter().find(|record| record.0 == temp.0).unwrap()),
        };
        let (found_key, found_value) = found?;
        position = Some(found_key.clone());
        if is_index {
            object_store_position = Some(found_value.clone());
        }
    }
    found.cloned()
}

/// The records that a step that asks for `records` records gives by the spec: the one the step
/// finds, then those of the steps without a key after it.
fn spec_cursor_records(
    records: &[SourceRecord],
    is_index: bool,
    direction: IndexedDBCursorDirection,
    range: &IndexedDBKeyRange,
    step: &IndexedDBCursorStep,
    count: u32,
) -> Vec<SourceRecord> {
    let mut found = Vec::new();
    let mut step = step.clone();
    while found.len() < count as usize {
        let Some(record) = spec_found_record(records, is_index, direction, range, &step) else {
            break;
        };
        step = cursor_step_at(&record, is_index);
        found.push(record);
    }
    found
}

/// A step without a key from the record `at`.
fn cursor_step_at(at: &SourceRecord, is_index: bool) -> IndexedDBCursorStep {
    IndexedDBCursorStep {
        position: Some(at.0.clone()),
        object_store_position: is_index.then(|| at.1.clone()),
        key: None,
        primary_key: None,
        count: 1,
    }
}

const CURSOR_DIRECTIONS: [IndexedDBCursorDirection; 4] = [
    IndexedDBCursorDirection::Next,
    IndexedDBCursorDirection::NextUnique,
    IndexedDBCursorDirection::Prev,
    IndexedDBCursorDirection::PrevUnique,
];

/// A cursor's source in the cursor tests, the store's row, and its records in order.
fn cursor_source<X: SqlExecutor>(
    db: &SqlEngine<X>,
    store: i64,
    index: Option<i64>,
) -> (queries::ObjectStore, Vec<SourceRecord>) {
    let mut connection = db.connection();
    let object_store = queries::object_store(&mut *connection, store)
        .unwrap()
        .unwrap();
    drop(connection);
    let records = match index {
        Some(index) => index_records_of(db, store, index),
        None => keys_of(&mut *db.connection(), store)
            .into_iter()
            .map(|key| (key.clone(), key))
            .collect(),
    };
    (object_store, records)
}

/// Runs a cursor step on `index` of `store`, or on the store, as [`queries::cursor_records`]
/// does, and returns the records as (key, value) with their values, which `value_of` must give
/// for the record's primary key.
#[allow(clippy::too_many_arguments)]
fn backend_cursor_records<X: SqlExecutor>(
    connection: &mut X,
    store: &queries::ObjectStore,
    index: Option<i64>,
    direction: IndexedDBCursorDirection,
    range: &IndexedDBKeyRange,
    step: &IndexedDBCursorStep,
    records: u32,
    key_only: bool,
    value_of: &dyn Fn(&IndexedDBKeyType) -> Vec<u8>,
) -> Vec<SourceRecord> {
    let source = match index {
        Some(index) => RecordSource::Index(store.index(index).unwrap()),
        None => RecordSource::ObjectStore(store),
    };
    let found = queries::cursor_records(
        connection, source, range, direction, step, records, key_only,
    )
    .unwrap_or_else(|error| {
        panic!("{error:?}: {index:?} {direction:?} {range:?} {step:?} {records} {key_only}")
    });
    found
        .into_iter()
        .map(|record| {
            let expected = (!key_only).then(|| value_of(&record.primary_key));
            assert_eq!(record.value, expected, "the value of {record:?}");
            if index.is_none() {
                assert_eq!(record.key, record.primary_key);
            }
            (record.key, record.primary_key)
        })
        .collect()
}

/// Creates a store with a non-unique, a unique and a multiEntry index ("kind", "id", "tag"), and
/// 12 records with number and string keys and repeated index keys, put in an order that is
/// neither the key order nor any index's. Returns the store, the indexes and the primary keys;
/// the record with the n-th primary key has the value `[n]`.
fn store_with_three_indexes<X: SqlExecutor>(
    db: &SqlEngine<X>,
) -> (i64, [i64; 3], Vec<IndexedDBKeyType>) {
    let store = create_store(db, "store", None, false).unwrap();
    let index = |name: &str, unique, multi_entry| {
        let key_path = KeyPath::String(name.to_owned());
        create_index(db, store, name.to_owned(), key_path, unique, multi_entry).unwrap()
    };
    let (by_kind, by_id, by_tag) = (
        index("kind", false, false),
        index("id", true, false),
        index("tag", false, true),
    );
    let primary_keys: Vec<_> = (0..10)
        .map(|n| number(f64::from(n)))
        .chain([string("x"), string("y")])
        .collect();
    let kinds = [
        number(1.0),
        number(2.0),
        string("a"),
        number(2.0),
        string("b"),
    ];
    // Put in an order that is neither the key order nor any index's.
    let order = [7, 2, 11, 0, 9, 4, 1, 10, 5, 8, 3, 6];
    let puts = order
        .iter()
        .map(|&n| {
            let tags = match n % 3 {
                0 => vec![],
                1 => vec![string("t"), number(f64::from(n % 4))],
                _ => vec![string("t"), string("u")],
            };
            (
                Some(primary_keys[n as usize].clone()),
                vec![n as u8],
                vec![
                    index_keys(by_kind, [kinds[n as usize % kinds.len()].clone()]),
                    index_keys(by_id, [number(100.0 - f64::from(n))]),
                    index_keys(by_tag, tags),
                ],
            )
        })
        .collect();
    let replies = run_indexed_puts(db, 1, store, puts, false);
    assert!(replies.iter().all(Result::is_ok), "{replies:?}");
    assert_eq!(commit(db, 1), Ok(()));
    (store, [by_kind, by_id, by_tag], primary_keys)
}

/// The value of the record with the key `primary_key` in [`store_with_three_indexes`].
fn value_of_primary_key(
    primary_keys: &[IndexedDBKeyType],
    primary_key: &IndexedDBKeyType,
) -> Vec<u8> {
    let n = primary_keys
        .iter()
        .position(|key| key == primary_key)
        .unwrap();
    vec![n as u8]
}

/// Cursor steps find the records that the spec's steps to iterate a cursor find: on a store and
/// on a non-unique, a unique and a multiEntry index, in every direction, over ranges, from every
/// position, with continue(), advance(), continue(key) and continuePrimaryKey(), and asking for
/// several records per step.
fn test_cursor_steps_find_what_the_spec_finds<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let (store, [by_kind, by_id, by_tag], primary_keys) = store_with_three_indexes(&db);
    let value_of =
        |primary_key: &IndexedDBKeyType| value_of_primary_key(&primary_keys, primary_key);

    // The request goes to the index it names, and one that the store doesn't have fails.
    let first = |index| AsyncReadOnlyOperation::Iterate {
        index,
        key_range: IndexedDBKeyRange::default(),
        direction: IndexedDBCursorDirection::Next,
        step: IndexedDBCursorStep {
            position: None,
            object_store_position: None,
            key: None,
            primary_key: None,
            count: 1,
        },
        records: 1,
        key_only: false,
    };
    assert_eq!(
        read::<X, BackendResult<Vec<IndexedDBRecord>>>(&db, 2, store, first(Some(by_id))),
        Ok(vec![IndexedDBRecord {
            key: number(89.0),
            primary_key: string("y"),
            value: Some(vec![11]),
        }])
    );
    assert!(matches!(
        read::<X, BackendResult<Vec<IndexedDBRecord>>>(&db, 2, store, first(Some(by_tag + 1))),
        Err(BackendError::DbErr(_))
    ));

    let absent_primary_key = string("w");
    let mut steps = 0;
    for index in [None, Some(by_kind), Some(by_id), Some(by_tag)] {
        let is_index = index.is_some();
        let (object_store, records) = cursor_source(&db, store, index);
        let mut keys: Vec<IndexedDBKeyType> = Vec::new();
        for (key, _) in &records {
            if keys.last() != Some(key) {
                keys.push(key.clone());
            }
        }
        // Keys that no record has, between and around the records' keys.
        let absent_keys = [number(-1.0), number(1.5), string("aa"), string("zz")];
        let (second, before_last) = (&keys[1], &keys[keys.len() - 2]);
        let ranges = [
            IndexedDBKeyRange::default(),
            IndexedDBKeyRange::new(
                Some(second.clone()),
                Some(before_last.clone()),
                false,
                false,
            ),
            IndexedDBKeyRange::new(Some(second.clone()), Some(before_last.clone()), true, true),
            IndexedDBKeyRange::only(second.clone()),
        ];
        let mut connection = db.connection();
        for direction in CURSOR_DIRECTIONS {
            let forward = matches!(
                direction,
                IndexedDBCursorDirection::Next | IndexedDBCursorDirection::NextUnique
            );
            let plain = matches!(
                direction,
                IndexedDBCursorDirection::Next | IndexedDBCursorDirection::Prev
            );
            for range in &ranges {
                let mut check = |step: &IndexedDBCursorStep, count: u32, key_only: bool| {
                    steps += 1;
                    assert_eq!(
                        backend_cursor_records(
                            &mut *connection,
                            &object_store,
                            index,
                            direction,
                            range,
                            step,
                            count,
                            key_only,
                            &value_of,
                        ),
                        spec_cursor_records(&records, is_index, direction, range, step, count),
                        "{index:?} {direction:?} {range:?} {step:?} records {count}"
                    );
                };
                let first = IndexedDBCursorStep {
                    position: None,
                    object_store_position: None,
                    key: None,
                    primary_key: None,
                    count: 1,
                };
                check(&first, 1, false);
                check(&first, 1, true);
                check(&first, 100, false);
                let mut at = spec_found_record(&records, is_index, direction, range, &first);
                while let Some(record) = at {
                    let step = cursor_step_at(&record, is_index);
                    // continue()
                    check(&step, 1, false);
                    // Several records per step, as a prefetch asks for.
                    check(&step, 3, false);
                    // advance()
                    for count in [2, 3, 50] {
                        check(
                            &IndexedDBCursorStep {
                                count,
                                ..step.clone()
                            },
                            1,
                            false,
                        );
                    }
                    // continue(key), with keys beyond the position, which continue() requires.
                    let beyond = |key: &&IndexedDBKeyType| {
                        if forward {
                            *key > &record.0
                        } else {
                            *key < &record.0
                        }
                    };
                    let mut continue_keys: Vec<_> = keys.iter().filter(beyond).collect();
                    if !forward {
                        continue_keys.reverse();
                    }
                    continue_keys.truncate(2);
                    continue_keys.extend(absent_keys.iter().filter(beyond).take(1));
                    for key in continue_keys {
                        let step = IndexedDBCursorStep {
                            key: Some(key.clone()),
                            ..step.clone()
                        };
                        check(&step, 1, false);
                        check(&step, 2, false);
                    }
                    // continuePrimaryKey(), on indexes in the plain directions, with (key, primary
                    // key) beyond the position, which it requires, and a primary key that no
                    // record has.
                    if is_index && plain {
                        let beyond = |(key, primary_key): &&SourceRecord| {
                            if forward {
                                key > &record.0 || (key == &record.0 && primary_key > &record.1)
                            } else {
                                key < &record.0 || (key == &record.0 && primary_key < &record.1)
                            }
                        };
                        let mut targets: Vec<_> = records.iter().filter(beyond).collect();
                        if !forward {
                            targets.reverse();
                        }
                        let absent = (record.0.clone(), absent_primary_key.clone());
                        let targets = targets
                            .into_iter()
                            .take(2)
                            .chain(Some(&absent).filter(beyond))
                            .cloned()
                            .collect::<Vec<_>>();
                        for (key, primary_key) in targets {
                            check(
                                &IndexedDBCursorStep {
                                    key: Some(key),
                                    primary_key: Some(primary_key),
                                    ..step.clone()
                                },
                                1,
                                false,
                            );
                        }
                    }
                    at = spec_found_record(&records, is_index, direction, range, &step);
                }
            }
        }
    }
    assert!(steps > 1000, "{steps} steps");
}

/// The records of `records`, the records of a source in (key, value) order, that retrieve
/// multiple items gets: those in `range`, of the unique directions the first of each key, in
/// reverse order for the reverse directions, the first `count` of them (all for 0).
fn expected_get_all(
    records: &[SourceRecord],
    direction: IndexedDBCursorDirection,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
) -> Vec<SourceRecord> {
    let unique = matches!(
        direction,
        IndexedDBCursorDirection::NextUnique | IndexedDBCursorDirection::PrevUnique
    );
    let mut found: Vec<SourceRecord> = Vec::new();
    for record in records.iter().filter(|record| range.contains(&record.0)) {
        if !(unique && found.last().is_some_and(|last| last.0 == record.0)) {
            found.push(record.clone());
        }
    }
    if matches!(
        direction,
        IndexedDBCursorDirection::Prev | IndexedDBCursorDirection::PrevUnique
    ) {
        found.reverse();
    }
    if let Some(count) = count.filter(|count| *count != 0) {
        found.truncate(count as usize);
    }
    found
}

/// getAllKeys(), getAll() and getAllRecords() of a store and of a non-unique, a unique and a
/// multiEntry index retrieve the records in a range in every direction, up to a count, as a
/// cursor in that direction visits them: the unique directions the first record of each key.
fn test_get_all_retrieves_what_a_cursor_visits<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let (store, [by_kind, by_id, by_tag], primary_keys) = store_with_three_indexes(&db);
    let value_of =
        |primary_key: &IndexedDBKeyType| value_of_primary_key(&primary_keys, primary_key);
    let first_step = IndexedDBCursorStep {
        position: None,
        object_store_position: None,
        key: None,
        primary_key: None,
        count: 1,
    };
    let mut reads = 0;
    for index in [None, Some(by_kind), Some(by_id), Some(by_tag)] {
        let (object_store, records) = cursor_source(&db, store, index);
        let source = match index {
            Some(index) => RecordSource::Index(object_store.index(index).unwrap()),
            None => RecordSource::ObjectStore(&object_store),
        };
        let mut keys: Vec<IndexedDBKeyType> = Vec::new();
        for (key, _) in &records {
            if keys.last() != Some(key) {
                keys.push(key.clone());
            }
        }
        let (second, before_last) = (&keys[1], &keys[keys.len() - 2]);
        let ranges = [
            IndexedDBKeyRange::default(),
            IndexedDBKeyRange::new(
                Some(second.clone()),
                Some(before_last.clone()),
                false,
                false,
            ),
            IndexedDBKeyRange::new(Some(second.clone()), Some(before_last.clone()), true, true),
            IndexedDBKeyRange::new(Some(second.clone()), None, false, false),
            IndexedDBKeyRange::new(None, Some(before_last.clone()), false, true),
            IndexedDBKeyRange::only(second.clone()),
            IndexedDBKeyRange::only(string("absent")),
        ];
        let mut connection = db.connection();
        for direction in CURSOR_DIRECTIONS {
            for range in &ranges {
                for count in [None, Some(0), Some(1), Some(2), Some(5), Some(u32::MAX)] {
                    let context = format!("{index:?} {direction:?} {range:?} {count:?}");
                    let expected = expected_get_all(&records, direction, range, count);
                    let visited = spec_cursor_records(
                        &records,
                        index.is_some(),
                        direction,
                        range,
                        &first_step,
                        count.filter(|count| *count != 0).unwrap_or(u32::MAX),
                    );
                    assert_eq!(expected, visited, "{context}");
                    reads += 1;
                    assert_eq!(
                        queries::get_all_keys(&mut *connection, source, range, direction, count)
                            .unwrap(),
                        expected
                            .iter()
                            .map(|(_, primary_key)| primary_key.clone())
                            .collect::<Vec<_>>(),
                        "keys {context}"
                    );
                    assert_eq!(
                        queries::get_all_values(&mut *connection, source, range, direction, count)
                            .unwrap(),
                        expected
                            .iter()
                            .map(|(_, primary_key)| IndexedDBValue {
                                primary_key: primary_key.clone(),
                                value: value_of(primary_key),
                            })
                            .collect::<Vec<_>>(),
                        "values {context}"
                    );
                    assert_eq!(
                        queries::get_all_records(&mut *connection, source, range, direction, count)
                            .unwrap(),
                        expected
                            .iter()
                            .map(|(key, primary_key)| IndexedDBRecord {
                                key: key.clone(),
                                primary_key: primary_key.clone(),
                                value: Some(value_of(primary_key)),
                            })
                            .collect::<Vec<_>>(),
                        "records {context}"
                    );
                }
            }
        }
    }
    assert!(reads > 500, "{reads} reads");

    // Through the engine: the request reads the index it names, and one of an index that the
    // store doesn't have fails.
    let get_all_records = |index, direction, count| AsyncReadOnlyOperation::GetAllRecords {
        index,
        key_range: IndexedDBKeyRange::default(),
        count,
        direction,
    };
    let record = |key: IndexedDBKeyType, primary_key: IndexedDBKeyType| IndexedDBRecord {
        value: Some(value_of(&primary_key)),
        key,
        primary_key,
    };
    assert_eq!(
        read::<X, BackendResult<AllRecords>>(
            &db,
            2,
            store,
            get_all_records(Some(by_kind), IndexedDBCursorDirection::PrevUnique, Some(2)),
        ),
        Ok(AllRecords(vec![
            record(string("b"), number(4.0)),
            record(string("a"), number(2.0)),
        ]))
    );
    assert_eq!(
        read::<X, BackendResult<AllRecords>>(
            &db,
            2,
            store,
            get_all_records(None, IndexedDBCursorDirection::Prev, Some(1)),
        ),
        Ok(AllRecords(vec![record(string("y"), string("y"))]))
    );
    assert!(matches!(
        read::<X, BackendResult<AllRecords>>(
            &db,
            2,
            store,
            get_all_records(Some(by_tag + 1), IndexedDBCursorDirection::Next, None),
        ),
        Err(BackendError::DbErr(_))
    ));
    assert_eq!(
        read::<X, BackendResult<Vec<IndexedDBKeyType>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetAllKeys {
                index: Some(by_id),
                key_range: IndexedDBKeyRange::default(),
                count: Some(2),
                direction: IndexedDBCursorDirection::NextUnique,
            },
        ),
        Ok(vec![string("y"), string("x")])
    );
    assert_eq!(
        read::<X, BackendResult<Vec<IndexedDBValue>>>(
            &db,
            2,
            store,
            AsyncReadOnlyOperation::GetAllItems {
                index: Some(by_tag),
                key_range: IndexedDBKeyRange::only(string("t")),
                count: Some(2),
                direction: IndexedDBCursorDirection::Prev,
            },
        ),
        Ok(vec![
            IndexedDBValue {
                primary_key: string("y"),
                value: value_of(&string("y")),
            },
            IndexedDBValue {
                primary_key: string("x"),
                value: value_of(&string("x")),
            },
        ])
    );
}

/// A tiny pseudo-random generator for the tests, xorshift64.
struct TestRandom(u64);

impl TestRandom {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

/// Each cursor step reads the records as they are when it runs: records put, replaced and
/// deleted between the steps, ahead of and behind the cursor and at its position, are found or
/// skipped as the spec's steps find them on the records of the time.
fn test_cursor_steps_see_the_writes_between_them<X: SqlExecutor>() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_engine::<X>(&dir);
    let store = create_store(&db, "store", None, false).unwrap();
    let by_kind = create_index(
        &db,
        store,
        "kind".to_owned(),
        KeyPath::String("kind".to_owned()),
        false,
        false,
    )
    .unwrap();
    let by_id = create_index(
        &db,
        store,
        "id".to_owned(),
        KeyPath::String("id".to_owned()),
        true,
        false,
    )
    .unwrap();
    let mut connection = db.connection();
    let object_store = queries::object_store(&mut *connection, store)
        .unwrap()
        .unwrap();
    // The value of a record is its kind and id, which give its index keys.
    let put = |connection: &mut X, primary_key: f64, kind: u8, id: u8| {
        queries::put_record(
            connection,
            &object_store,
            Some(number(primary_key)),
            &[kind, id],
            true,
            vec![
                index_keys(by_kind, [number(f64::from(kind))]),
                index_keys(by_id, [number(f64::from(id))]),
            ],
        )
        .unwrap()
    };
    let delete = |connection: &mut X, primary_key: f64| {
        queries::delete_records(
            connection,
            &object_store,
            &IndexedDBKeyRange::only(number(primary_key)),
        )
        .unwrap()
    };
    let stored_values = |connection: &mut X| {
        queries::get_all_values(
            connection,
            RecordSource::ObjectStore(&object_store),
            &IndexedDBKeyRange::default(),
            IndexedDBCursorDirection::Next,
            None,
        )
        .unwrap()
    };
    let records_of = |connection: &mut X, index: Option<i64>| -> Vec<SourceRecord> {
        let source = match index {
            Some(index) => RecordSource::Index(object_store.index(index).unwrap()),
            None => RecordSource::ObjectStore(&object_store),
        };
        let all = IndexedDBCursorStep {
            position: None,
            object_store_position: None,
            key: None,
            primary_key: None,
            count: 1,
        };
        queries::cursor_records(
            connection,
            source,
            &IndexedDBKeyRange::default(),
            IndexedDBCursorDirection::Next,
            &all,
            u32::MAX,
            true,
        )
        .unwrap()
        .into_iter()
        .map(|record| (record.key, record.primary_key))
        .collect()
    };
    let step_and_check = |connection: &mut X,
                          index: Option<i64>,
                          direction: IndexedDBCursorDirection,
                          step: &IndexedDBCursorStep|
     -> Option<SourceRecord> {
        let records = records_of(connection, index);
        let values = stored_values(connection);
        let expected = spec_found_record(
            &records,
            index.is_some(),
            direction,
            &IndexedDBKeyRange::default(),
            step,
        );
        let found = backend_cursor_records(
            connection,
            &object_store,
            index,
            direction,
            &IndexedDBKeyRange::default(),
            step,
            1,
            false,
            &|primary_key| {
                let value = values
                    .iter()
                    .find(|value| &value.primary_key == primary_key);
                value.unwrap().value.clone()
            },
        );
        assert_eq!(
            found.first(),
            expected.as_ref(),
            "{index:?} {direction:?} {step:?}"
        );
        expected
    };
    let reset = |connection: &mut X| {
        queries::clear(connection, &object_store).unwrap();
        for n in 0..8u8 {
            assert!(matches!(
                put(connection, f64::from(n), n % 3, 10 + n),
                PutItemResult::Key(_)
            ));
        }
    };

    // A unique index whose record at the cursor's position goes, and another record gets its
    // key with a larger primary key: "next" visits that record, "nextunique" doesn't.
    for (direction, expected) in [
        (
            IndexedDBCursorDirection::Next,
            Some((number(13.0), number(9.0))),
        ),
        (
            IndexedDBCursorDirection::NextUnique,
            Some((number(14.0), number(4.0))),
        ),
    ] {
        reset(&mut *connection);
        let at = (number(13.0), number(3.0));
        delete(&mut *connection, 3.0);
        assert!(matches!(
            put(&mut *connection, 9.0, 0, 13),
            PutItemResult::Key(_)
        ));
        assert_eq!(
            step_and_check(
                &mut *connection,
                Some(by_id),
                direction,
                &cursor_step_at(&at, true)
            ),
            expected
        );
    }

    // A reverse cursor over the non-unique index sees a record that moved behind it, and not one
    // that moved ahead (idbindex_reverse_cursor).
    reset(&mut *connection);
    let at = spec_found_record(
        &records_of(&mut *connection, Some(by_kind)),
        true,
        IndexedDBCursorDirection::Prev,
        &IndexedDBKeyRange::default(),
        &IndexedDBCursorStep {
            position: None,
            object_store_position: None,
            key: None,
            primary_key: None,
            count: 1,
        },
    )
    .unwrap();
    assert_eq!(at, (number(2.0), number(5.0)));
    assert!(matches!(
        put(&mut *connection, 0.0, 2, 10),
        PutItemResult::Key(_)
    ));
    assert!(matches!(
        put(&mut *connection, 2.0, 2, 12),
        PutItemResult::Key(_)
    ));
    assert_eq!(
        step_and_check(
            &mut *connection,
            Some(by_kind),
            IndexedDBCursorDirection::Prev,
            &cursor_step_at(&at, true)
        ),
        Some((number(2.0), number(2.0)))
    );

    // Random writes between the steps of cursors over the store and both indexes.
    let mut random = TestRandom(0x2545_f491_4f6c_dd1d);
    for index in [None, Some(by_kind), Some(by_id)] {
        for direction in CURSOR_DIRECTIONS {
            for _ in 0..3 {
                reset(&mut *connection);
                let mut step = IndexedDBCursorStep {
                    position: None,
                    object_store_position: None,
                    key: None,
                    primary_key: None,
                    count: 1,
                };
                for _ in 0..40 {
                    let Some(record) = step_and_check(&mut *connection, index, direction, &step)
                    else {
                        break;
                    };
                    step = cursor_step_at(&record, index.is_some());
                    for _ in 0..random.below(3) {
                        let primary_key = random.below(12) as f64;
                        match random.below(3) {
                            0 => delete(&mut *connection, primary_key),
                            _ => {
                                let kind = random.below(4) as u8;
                                let id = 10 + random.below(14) as u8;
                                // A put whose id another record has changes nothing.
                                put(&mut *connection, primary_key, kind, id);
                            },
                        }
                    }
                }
            }
        }
    }
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
