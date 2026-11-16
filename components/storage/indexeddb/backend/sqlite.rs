/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::{Path, PathBuf};
use std::{fs, io};

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Connection, ErrorCode, ToSql, ffi};

#[cfg(test)]
use super::executor::Connector;
use super::executor::{OnRow, SqlError, SqlExecutor, SqlParam, SqlRow, TransactionBehavior};
use crate::shared::DB_PRAGMAS;

const DATABASE_FILE_NAME: &str = "indexeddb.sqlite";

/// The suffixes of the files SQLite keeps next to a database file, and the empty one of the
/// database file itself, which is deleted last.
const DATABASE_FILE_SUFFIXES: [&str; 4] = ["-wal", "-shm", "-journal", ""];

/// Enough for every distinct statement of the IndexedDB layer, so none is prepared twice.
const STATEMENT_CACHE_CAPACITY: usize = 64;

/// The first three letters of the statements that RDB's execute calls run, which is how RDB tells
/// a statement's kind (`SqliteUtils::GetSqlStatementType` in relational_store): DDL, INSERT,
/// UPDATE, DELETE, REPLACE and PRAGMA. In a transaction RDB also runs transaction control, which
/// the executors only issue through `begin`, `commit` and `rollback`.
const RDB_EXECUTE_KINDS: [&str; 8] = ["ALT", "CRE", "DEL", "DRO", "INS", "PRA", "REP", "UPD"];

/// Rejects a statement that the RDB executor can't run, so that SQL that only works on SQLite
/// fails on the host too: `execute` runs only the kinds of [`RDB_EXECUTE_KINDS`], and `query`
/// only statements that don't write.
fn check_runs_on_rdb(
    sql: &str,
    statement: &rusqlite::Statement<'_>,
    query: bool,
) -> Result<(), SqlError> {
    let runs = if query {
        statement.readonly()
    } else {
        let kind = sql
            .trim_start_matches(|c: char| c.is_ascii_whitespace() || c.is_ascii_control())
            .get(..3)
            .unwrap_or_default()
            .to_ascii_uppercase();
        RDB_EXECUTE_KINDS.contains(&kind.as_str())
    };
    if runs {
        Ok(())
    } else {
        Err(SqlError::NotOnRdb(sql.to_owned()))
    }
}

/// A [`SqlExecutor`] on a bundled SQLite connection.
pub(crate) struct SqliteExecutor {
    path: PathBuf,
    connection: Connection,
}

impl SqliteExecutor {
    fn open_file(path: PathBuf) -> Result<Self, SqlError> {
        let connection = Connection::open(&path)?;
        connection.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        for pragma in DB_PRAGMAS {
            // TODO: Handle errors properly
            let _ = connection.execute(pragma, ());
        }
        Ok(Self { path, connection })
    }

    /// Ends the open transaction after an error that SQLite may have rolled it back for, as
    /// <https://sqlite.org/lang_transaction.html#response_to_errors_within_a_transaction>
    /// recommends.
    fn end_transaction_after<T>(&mut self, result: rusqlite::Result<T>) -> Result<T, SqlError> {
        if let Err(rusqlite::Error::SqliteFailure(error, _)) = &result &&
            matches!(
                error.code,
                ErrorCode::DiskFull |
                    ErrorCode::SystemIoFailure |
                    ErrorCode::DatabaseBusy |
                    ErrorCode::OutOfMemory
            ) &&
            self.in_transaction()
        {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        Ok(result?)
    }

    fn db_status(&self, op: i32) -> usize {
        let mut current = 0;
        let mut highwater = 0;
        // SAFETY: The handle belongs to the live connection, and the out-parameters are locals.
        let result = unsafe {
            ffi::sqlite3_db_status(
                self.connection.handle(),
                op,
                &mut current,
                &mut highwater,
                0,
            )
        };
        if result != 0 { 0 } else { current as usize }
    }
}

impl ToSql for SqlParam<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match *self {
            SqlParam::Null => ValueRef::Null,
            SqlParam::Integer(value) => ValueRef::Integer(value),
            SqlParam::Text(value) => ValueRef::Text(value.as_bytes()),
            SqlParam::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

struct SqliteRow<'a, 'b>(&'a rusqlite::Row<'b>);

impl SqlRow for SqliteRow<'_, '_> {
    fn optional_integer(&self, column: usize) -> Result<Option<i64>, SqlError> {
        Ok(self.0.get(column)?)
    }

    #[cfg(test)]
    fn optional_text(&self, column: usize) -> Result<Option<String>, SqlError> {
        Ok(self.0.get(column)?)
    }

    fn optional_blob(&self, column: usize) -> Result<Option<Vec<u8>>, SqlError> {
        Ok(self.0.get(column)?)
    }

    fn size(&self, column: usize) -> Result<usize, SqlError> {
        Ok(match self.0.get_ref(column)? {
            ValueRef::Blob(bytes) | ValueRef::Text(bytes) => bytes.len(),
            _ => 0,
        })
    }
}

impl SqlExecutor for SqliteExecutor {
    fn open(directory: &Path) -> Result<Self, SqlError> {
        Self::open_file(directory.join(DATABASE_FILE_NAME))
    }

    #[cfg(test)]
    fn connector(&self) -> Connector<Self> {
        let path = self.path.clone();
        Box::new(move || Self::open_file(path.clone()))
    }

    fn delete(self) -> Result<(), SqlError> {
        let Self { path, connection } = self;
        connection.close().map_err(|(_, error)| error)?;
        for suffix in DATABASE_FILE_SUFFIXES {
            let mut file = path.clone().into_os_string();
            file.push(suffix);
            match fs::remove_file(&file) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    return Err(SqlError::Io(error));
                },
                _ => {},
            }
        }
        Ok(())
    }

    fn execute(&mut self, sql: &str, params: &[SqlParam<'_>]) -> Result<u64, SqlError> {
        let result = match self.connection.prepare_cached(sql) {
            Ok(mut statement) => {
                check_runs_on_rdb(sql, &statement, false)?;
                statement.execute(rusqlite::params_from_iter(params))
            },
            Err(error) => Err(error),
        };
        match result {
            Ok(changed) => Ok(changed as u64),
            // A statement like `PRAGMA journal_mode = WAL` has run when it returns its row.
            Err(rusqlite::Error::ExecuteReturnedResults) => Ok(0),
            Err(error) => self.end_transaction_after(Err(error)),
        }
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError> {
        let mut on_row_error = None;
        let result = match self.connection.prepare_cached(sql) {
            Ok(mut statement) => {
                check_runs_on_rdb(sql, &statement, true)?;
                statement
                    .query(rusqlite::params_from_iter(params))
                    .and_then(|mut rows| {
                        while let Some(row) = rows.next()? {
                            match on_row(&SqliteRow(row)) {
                                Ok(flow) if flow.is_break() => break,
                                Ok(_) => {},
                                Err(error) => {
                                    on_row_error = Some(error);
                                    break;
                                },
                            }
                        }
                        Ok(())
                    })
            },
            Err(error) => Err(error),
        };
        self.end_transaction_after(result)?;
        on_row_error.map_or(Ok(()), Err)
    }

    fn begin(&mut self, behavior: TransactionBehavior) -> Result<(), SqlError> {
        if self.in_transaction() {
            return Err(SqlError::TransactionState("begin inside a transaction"));
        }
        self.connection.execute_batch(match behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
        })?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        if !self.in_transaction() {
            return Err(SqlError::TransactionState("commit without a transaction"));
        }
        let result = self.connection.execute_batch("COMMIT");
        // A failed COMMIT, e.g. with SQLITE_BUSY, leaves the transaction open.
        if result.is_err() && self.in_transaction() {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        Ok(result?)
    }

    fn rollback(&mut self) -> Result<(), SqlError> {
        // SQLite rolls back by itself after some errors, such as SQLITE_FULL.
        if self.in_transaction() {
            self.connection.execute_batch("ROLLBACK")?;
        }
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        !self.connection.is_autocommit()
    }

    fn heap_size(&self) -> usize {
        // 48 KB (3.3.1 at https://sqlite.org/malloc.html)
        const DEFAULT_LOOKASIDE_SIZE: usize = 48 * 1024;
        DEFAULT_LOOKASIDE_SIZE +
            self.db_status(ffi::SQLITE_DBSTATUS_CACHE_USED_SHARED) +
            self.db_status(ffi::SQLITE_DBSTATUS_SCHEMA_USED) +
            self.db_status(ffi::SQLITE_DBSTATUS_STMT_USED)
    }
}
