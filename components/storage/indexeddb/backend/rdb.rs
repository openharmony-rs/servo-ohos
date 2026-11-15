/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::error::Error as StdError;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use ohos_rdb_sys::rdb_transaction::OH_RDB_TransType;
use ohos_rdb_sys::relational_store_error_code::OH_Rdb_ErrCode;

use super::executor::{
    Connector, OnRow, SqlError, SqlExecutor, SqlParam, SqlRow, TransactionBehavior,
};
use crate::ohos_rdb::{
    OhosRdbCursor, OhosRdbError, OhosRdbStore, OhosRdbTransaction, OhosRdbValues,
};
use crate::shared::{DB_PRAGMAS, has_enospc};

const DATABASE_FILE_NAME: &str = "indexeddb.rdb";

/// A [`SqlExecutor`] on an OpenHarmony RDB store.
///
/// RDB gives every transaction a dedicated connection from the store's pool, so the transaction
/// is a handle of its own; statements outside one go to the store, which picks a connection per
/// statement.
pub(crate) struct RdbExecutor {
    /// The directory the store was opened in, which RDB keeps its files under.
    directory: PathBuf,
    /// Declared before the store, so that it is dropped, and rolled back, first.
    transaction: Option<OhosRdbTransaction>,
    /// Shared with the executors of [`SqlExecutor::connector`]. The lock serializes the calls on
    /// the store handle; an open transaction doesn't hold it.
    store: Arc<Mutex<OhosRdbStore>>,
}

impl RdbExecutor {
    fn store(&self) -> Result<MutexGuard<'_, OhosRdbStore>, SqlError> {
        self.store
            .lock()
            .map_err(|_| SqlError::TransactionState("poisoned RDB store lock"))
    }

    /// Ends the open transaction, which SQLite may have rolled back already. RDB can't tell
    /// whether it did, and if it did, the transaction's connection runs every later statement in
    /// autocommit.
    fn end_transaction(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            // Fails if SQLite has rolled back already.
            let _ = transaction.rollback();
        }
    }

    /// Ends the open transaction after an execute error that SQLite may have rolled it back for.
    fn end_transaction_after<T>(&mut self, result: Result<T, SqlError>) -> Result<T, SqlError> {
        if result.as_ref().is_err_and(may_have_ended_transaction) {
            self.end_transaction();
        }
        result
    }
}

/// Whether SQLite may have rolled back the open transaction after `error` of an execute call:
/// SQLITE_FULL, SQLITE_IOERR, SQLITE_BUSY or SQLITE_NOMEM. Failing queries mostly come without
/// SQLite's code, see [`QueryError::Statement`].
fn may_have_ended_transaction(error: &SqlError) -> bool {
    matches!(error, SqlError::Rdb(OhosRdbError::Api { code, .. })
        if [
            OH_Rdb_ErrCode::RDB_E_SQLITE_FULL,
            OH_Rdb_ErrCode::RDB_E_SQLITE_IOERR,
            OH_Rdb_ErrCode::RDB_E_SQLITE_BUSY,
            OH_Rdb_ErrCode::RDB_E_SQLITE_NOMEM,
        ]
        .iter()
        .any(|ended| ended.0 == *code))
}

/// Whether an RDB error reports a full disk or an exceeded size limit.
pub(crate) fn is_disk_full_error(error: &OhosRdbError) -> bool {
    matches!(error, OhosRdbError::Api { code, .. }
        if *code == OH_Rdb_ErrCode::RDB_E_SQLITE_FULL.0 ||
            *code == OH_Rdb_ErrCode::RDB_E_SQLITE_IOERR.0) ||
        has_enospc(Some(error as &(dyn StdError + 'static)))
}

fn values(params: &[SqlParam<'_>]) -> Result<OhosRdbValues, SqlError> {
    let mut values = OhosRdbValues::new()?;
    for param in params {
        match *param {
            SqlParam::Null => values.push_null()?,
            SqlParam::Integer(value) => values.push_int(value)?,
            SqlParam::Text(value) => values.push_text(value)?,
            SqlParam::Blob(value) => values.push_blob(value)?,
        }
    }
    Ok(values)
}

/// How a query failed.
enum QueryError {
    /// The statement failed. RDB reports most such failures without SQLite's code, as
    /// RDB_E_STEP_RESULT_CLOSED or, after SQLITE_BUSY, as E_STEP_RESULT_QUERY_EXCEEDED, so any of
    /// them may have ended the open transaction.
    Statement(SqlError),
    /// Reading a row, or `on_row`, failed, which leaves the statement and the transaction alone.
    Row(SqlError),
}

fn read_rows(
    cursor: crate::ohos_rdb::Result<OhosRdbCursor<'_>>,
    on_row: &mut OnRow<'_>,
) -> Result<(), QueryError> {
    let statement_error = |error: OhosRdbError| QueryError::Statement(error.into());
    let mut cursor = cursor.map_err(statement_error)?;
    while cursor.next_row().map_err(statement_error)? {
        if on_row(&RdbRow(&cursor))
            .map_err(QueryError::Row)?
            .is_break()
        {
            break;
        }
    }
    Ok(())
}

struct RdbRow<'a, 'b>(&'a OhosRdbCursor<'b>);

impl SqlRow for RdbRow<'_, '_> {
    fn optional_integer(&self, column: usize) -> Result<Option<i64>, SqlError> {
        Ok(self.0.int64(column)?)
    }

    fn optional_text(&self, column: usize) -> Result<Option<String>, SqlError> {
        Ok(self.0.text(column)?)
    }

    fn optional_blob(&self, column: usize) -> Result<Option<Vec<u8>>, SqlError> {
        Ok(self.0.blob(column)?)
    }
}

impl SqlExecutor for RdbExecutor {
    fn open(directory: &Path) -> Result<Self, SqlError> {
        fs::create_dir_all(directory).map_err(OhosRdbError::from)?;
        let store = OhosRdbStore::open(directory, DATABASE_FILE_NAME)?;
        for pragma in DB_PRAGMAS {
            let _ = store.execute(pragma);
        }
        Ok(Self {
            directory: directory.to_owned(),
            store: Arc::new(Mutex::new(store)),
            transaction: None,
        })
    }

    fn connector(&self) -> Connector<Self> {
        let directory = self.directory.clone();
        let store = self.store.clone();
        Box::new(move || {
            Ok(Self {
                directory: directory.clone(),
                store: store.clone(),
                transaction: None,
            })
        })
    }

    fn delete(self) -> Result<(), SqlError> {
        let Self {
            directory,
            store,
            transaction,
        } = self;
        drop(transaction);
        // RDB would close the store under the other executors.
        let Some(store) = Arc::into_inner(store) else {
            return Err(SqlError::TransactionState(
                "delete while other executors on the database exist",
            ));
        };
        drop(store);
        OhosRdbStore::delete(&directory, DATABASE_FILE_NAME)?;
        Ok(())
    }

    fn execute(&mut self, sql: &str, params: &[SqlParam<'_>]) -> Result<u64, SqlError> {
        let values = values(params)?;
        // RDB returns the number of changed rows for UPDATE and DELETE, and the row id or -1 for
        // INSERT.
        let result = match &self.transaction {
            Some(transaction) => transaction.execute_with_result(sql, &values),
            None => self.store()?.execute_with_args(sql, &values),
        };
        let result = self.end_transaction_after(result.map_err(SqlError::from))?;
        Ok(result.filter(|value| *value > 0).unwrap_or(0) as u64)
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError> {
        let values = values(params)?;
        let result = match &self.transaction {
            Some(transaction) => read_rows(transaction.query_sql(sql, &values), on_row),
            None => {
                let store = self.store()?;
                read_rows(store.query_sql(sql, &values), on_row)
            },
        };
        match result {
            Ok(()) => Ok(()),
            Err(QueryError::Row(error)) => Err(error),
            Err(QueryError::Statement(error)) => {
                self.end_transaction();
                Err(error)
            },
        }
    }

    fn begin(&mut self, behavior: TransactionBehavior) -> Result<(), SqlError> {
        if self.transaction.is_some() {
            return Err(SqlError::TransactionState("begin inside a transaction"));
        }
        let transaction_type = match behavior {
            TransactionBehavior::Deferred => OH_RDB_TransType::RDB_TRANS_DEFERRED,
            TransactionBehavior::Immediate => OH_RDB_TransType::RDB_TRANS_IMMEDIATE,
        };
        let transaction = self.store()?.transaction_of_type(transaction_type)?;
        self.transaction = Some(transaction);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        // A failed commit drops the transaction's connection, which rolls it back.
        self.transaction
            .take()
            .ok_or(SqlError::TransactionState("commit without a transaction"))?
            .commit()?;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), SqlError> {
        if let Some(transaction) = self.transaction.take() {
            transaction.rollback()?;
        }
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    fn heap_size(&self) -> usize {
        0
    }
}
