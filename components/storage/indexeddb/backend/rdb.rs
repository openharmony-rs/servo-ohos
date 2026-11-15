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

fn read_rows(mut cursor: OhosRdbCursor<'_>, on_row: &mut OnRow<'_>) -> Result<(), SqlError> {
    while cursor.next_row()? {
        if on_row(&RdbRow(&cursor))?.is_break() {
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
            Some(transaction) => transaction.execute_with_result(sql, &values)?,
            None => self.store()?.execute_with_args(sql, &values)?,
        };
        Ok(result.filter(|value| *value > 0).unwrap_or(0) as u64)
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError> {
        let values = values(params)?;
        match &self.transaction {
            Some(transaction) => read_rows(transaction.query_sql(sql, &values)?, on_row),
            None => {
                let store = self.store()?;
                read_rows(store.query_sql(sql, &values)?, on_row)
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
