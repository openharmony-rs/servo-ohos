/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::fmt;
use std::ops::ControlFlow;
use std::path::Path;

use storage_traits::indexeddb::BackendError;

use crate::indexeddb::BackendErrorMapper;

/// How [`SqlExecutor::begin`] starts a transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionBehavior {
    /// `BEGIN DEFERRED`: locks are taken on first use. For transactions that only read; the
    /// engine runs reads without a transaction, so only tests use it.
    #[cfg_attr(not(test), expect(dead_code))]
    Deferred,
    /// `BEGIN IMMEDIATE`: the write lock is taken up front, so a writing transaction can't fail
    /// half-way because another connection wrote in the meantime.
    Immediate,
}

/// A statement parameter. Parameters bind to the statement's `?` placeholders in order.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SqlParam<'a> {
    Null,
    Integer(i64),
    Text(&'a str),
    Blob(&'a [u8]),
}

/// A result row of [`SqlExecutor::query`]. Columns are numbered from 0; `None` is SQL NULL.
pub(crate) trait SqlRow {
    fn optional_integer(&self, column: usize) -> Result<Option<i64>, SqlError>;
    #[cfg(test)]
    fn optional_text(&self, column: usize) -> Result<Option<String>, SqlError>;
    fn optional_blob(&self, column: usize) -> Result<Option<Vec<u8>>, SqlError>;
    /// The size in bytes of a BLOB or TEXT column, 0 for NULL, without reading its content.
    fn size(&self, column: usize) -> Result<usize, SqlError>;

    fn integer(&self, column: usize) -> Result<i64, SqlError> {
        self.optional_integer(column)?
            .ok_or(SqlError::UnexpectedNull(column))
    }

    #[cfg(test)]
    fn text(&self, column: usize) -> Result<String, SqlError> {
        self.optional_text(column)?
            .ok_or(SqlError::UnexpectedNull(column))
    }

    fn blob(&self, column: usize) -> Result<Vec<u8>, SqlError> {
        self.optional_blob(column)?
            .ok_or(SqlError::UnexpectedNull(column))
    }
}

/// Called by [`SqlExecutor::query`] with each result row, in order, until it returns
/// [`ControlFlow::Break`].
pub(crate) type OnRow<'a> = dyn FnMut(&dyn SqlRow) -> Result<ControlFlow<()>, SqlError> + 'a;

/// Makes executors on the database of an executor, from any thread.
#[cfg(test)]
pub(crate) type Connector<X> = Box<dyn Fn() -> Result<X, SqlError> + Send + Sync>;

/// One connection to the SQLite database of an IndexedDB database, which the IndexedDB-on-SQL
/// layer runs on. It is a connection in the SQL sense: [`Self::begin`] opens a transaction that
/// every later statement runs in until [`Self::commit`] or [`Self::rollback`], across any number
/// of calls; without one, each statement commits on its own.
///
/// Statements are plain SQLite SQL without `SAVEPOINT`/`RELEASE`, since RDB's transactions
/// reject `RELEASE`. Implementations cache prepared statements by their text where the
/// underlying library can, so there is no separate prepare step.
pub(crate) trait SqlExecutor: Send + Sized + 'static {
    /// Opens the database stored in `directory`, creating its file if there is none.
    fn open(directory: &Path) -> Result<Self, SqlError>;

    /// Makes further executors on the same database, for tests that look at it from another
    /// connection. The SQLite executor opens a connection for each; the RDB executor shares its
    /// store handle, whose connection pool runs the statements.
    #[cfg(test)]
    fn connector(&self) -> Connector<Self>;

    /// Closes this connection and deletes the database with all its files. No other connection
    /// to the database may be open.
    fn delete(self) -> Result<(), SqlError>;

    /// Whether a database is stored in `directory`, which [`Self::open`] would otherwise create.
    fn exists(directory: &Path) -> bool;

    /// Runs a statement that returns no rows. Returns the number of rows an `UPDATE` or `DELETE`
    /// changed; the value is unspecified for other statements.
    ///
    /// SQLite may roll back the whole transaction when a statement fails with SQLITE_FULL,
    /// SQLITE_IOERR, SQLITE_BUSY or SQLITE_NOMEM. After such an error, in a statement or a query,
    /// the open transaction is always rolled back and ended. The RDB executor, which doesn't
    /// learn SQLite's code for most failing queries, also ends it after any query whose
    /// statement failed, so [`Self::in_transaction`] stays true only while the transaction is
    /// open.
    fn execute(&mut self, sql: &str, params: &[SqlParam<'_>]) -> Result<u64, SqlError>;

    /// Runs a query and calls `on_row` with its rows. An error from `on_row` stops the query and
    /// is returned.
    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError>;

    /// Starts a transaction. Fails if one is already open.
    fn begin(&mut self, behavior: TransactionBehavior) -> Result<(), SqlError>;

    /// Commits the open transaction. The transaction has ended when this returns, also on error,
    /// in which case its changes were rolled back.
    fn commit(&mut self) -> Result<(), SqlError>;

    /// Rolls back the open transaction, if there is one.
    fn rollback(&mut self) -> Result<(), SqlError>;

    fn in_transaction(&self) -> bool;

    /// Memory held by the connection, for memory reports.
    fn heap_size(&self) -> usize;
}

/// Runs a query for at most one row and maps it with `map_row`.
pub(crate) fn query_row<X: SqlExecutor, T>(
    executor: &mut X,
    sql: &str,
    params: &[SqlParam<'_>],
    map_row: impl Fn(&dyn SqlRow) -> Result<T, SqlError>,
) -> Result<Option<T>, SqlError> {
    let mut result = None;
    executor.query(sql, params, &mut |row| {
        result = Some(map_row(row)?);
        Ok(ControlFlow::Break(()))
    })?;
    Ok(result)
}

/// Runs a query and maps every row with `map_row`.
pub(crate) fn query_rows<X: SqlExecutor, T>(
    executor: &mut X,
    sql: &str,
    params: &[SqlParam<'_>],
    map_row: impl Fn(&dyn SqlRow) -> Result<T, SqlError>,
) -> Result<Vec<T>, SqlError> {
    let mut rows = Vec::new();
    executor.query(sql, params, &mut |row| {
        rows.push(map_row(row)?);
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(rows)
}

/// Runs `f` in a transaction on `executor`, committing if it succeeds and rolling back if not.
pub(crate) fn with_transaction<X: SqlExecutor, T>(
    executor: &mut X,
    behavior: TransactionBehavior,
    f: impl FnOnce(&mut X) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    executor.begin(behavior)?;
    match f(executor) {
        Ok(value) => {
            executor.commit()?;
            Ok(value)
        },
        Err(error) => {
            if let Err(rollback_error) = executor.rollback() {
                log::warn!("Failed to roll back an IndexedDB transaction: {rollback_error:?}");
            }
            Err(error)
        },
    }
}

#[derive(Debug)]
pub(crate) enum SqlError {
    #[cfg(feature = "sqlite-backend")]
    Sqlite(rusqlite::Error),
    #[cfg(ohos_rdb)]
    Rdb(crate::ohos_rdb::OhosRdbError),
    /// A NULL in a column that must not hold one.
    UnexpectedNull(usize),
    /// A row that should exist doesn't, or more rows changed than expected.
    MissingRow(&'static str),
    /// A stored key path that can't be decoded.
    KeyPath(postcard::Error),
    /// A stored key that can't be decoded.
    InvalidKey,
    /// A stored name that can't be decoded.
    InvalidName,
    /// A misuse of the executor: [`SqlExecutor::begin`] while a transaction is open, a commit
    /// without one, or a delete while other executors on the database exist.
    TransactionState(&'static str),
    /// A database of another schema version that is still there after deleting it.
    ResetFailed,
    /// A schema operation that doesn't fit the object stores and indexes, such as a new store
    /// whose id isn't larger than every store id, or a missing index.
    InvalidMetadata(&'static str),
    /// A failure to delete a database file.
    #[cfg(feature = "sqlite-backend")]
    Io(std::io::Error),
    /// A statement that the RDB executor would reject, which the SQLite executor refuses too.
    #[cfg(feature = "sqlite-backend")]
    NotOnRdb(String),
    /// A corrupt database, injected by a test.
    #[cfg(test)]
    InjectedCorruption,
}

impl fmt::Display for SqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "sqlite-backend")]
            Self::Sqlite(error) => error.fmt(formatter),
            #[cfg(ohos_rdb)]
            Self::Rdb(error) => error.fmt(formatter),
            Self::UnexpectedNull(column) => write!(formatter, "unexpected NULL in column {column}"),
            Self::MissingRow(context) => write!(formatter, "missing row: {context}"),
            Self::KeyPath(error) => write!(formatter, "invalid stored key path: {error}"),
            Self::InvalidKey => write!(formatter, "invalid stored key"),
            Self::InvalidName => write!(formatter, "invalid stored name"),
            Self::TransactionState(context) => write!(formatter, "transaction state: {context}"),
            Self::ResetFailed => write!(formatter, "failed to delete a database to reset it"),
            Self::InvalidMetadata(context) => write!(formatter, "invalid metadata: {context}"),
            #[cfg(feature = "sqlite-backend")]
            Self::Io(error) => error.fmt(formatter),
            #[cfg(feature = "sqlite-backend")]
            Self::NotOnRdb(sql) => write!(formatter, "statement that RDB would reject: {sql}"),
            #[cfg(test)]
            Self::InjectedCorruption => write!(formatter, "injected corruption"),
        }
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(feature = "sqlite-backend")]
            Self::Sqlite(error) => Some(error),
            #[cfg(ohos_rdb)]
            Self::Rdb(error) => Some(error),
            Self::KeyPath(error) => Some(error),
            #[cfg(feature = "sqlite-backend")]
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(feature = "sqlite-backend")]
impl From<rusqlite::Error> for SqlError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[cfg(ohos_rdb)]
impl From<crate::ohos_rdb::OhosRdbError> for SqlError {
    fn from(error: crate::ohos_rdb::OhosRdbError) -> Self {
        Self::Rdb(error)
    }
}

impl SqlError {
    /// Whether the error reports a full disk or an exceeded size limit.
    fn is_disk_full(&self) -> bool {
        match self {
            #[cfg(feature = "sqlite-backend")]
            Self::Sqlite(error) => crate::shared::is_sqlite_disk_full_error(error),
            #[cfg(ohos_rdb)]
            Self::Rdb(error) => super::rdb::is_disk_full_error(error),
            _ => false,
        }
    }

    /// Whether the error says that the database file is damaged (SQLITE_CORRUPT or
    /// SQLITE_NOTADB), so that no statement on the database can be trusted any more.
    fn makes_database_unusable(&self) -> bool {
        match self {
            #[cfg(feature = "sqlite-backend")]
            Self::Sqlite(error) => matches!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase)
            ),
            #[cfg(ohos_rdb)]
            Self::Rdb(error) => super::rdb::is_corruption_error(error),
            #[cfg(test)]
            Self::InjectedCorruption => true,
            _ => false,
        }
    }

    /// Whether the error reports a constraint of the tables that a statement violated, such as
    /// a primary key or a unique index that would have had the same key twice.
    fn is_constraint_violation(&self) -> bool {
        match self {
            #[cfg(feature = "sqlite-backend")]
            Self::Sqlite(error) => {
                error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation)
            },
            #[cfg(ohos_rdb)]
            Self::Rdb(error) => super::rdb::is_constraint_error(error),
            _ => false,
        }
    }

    /// The error to report for this one: QuotaExceeded for a full disk, Unusable for a damaged
    /// database, Constraint for a violated constraint, DbErr otherwise.
    pub(crate) fn to_backend_error(&self) -> BackendError {
        if self.is_disk_full() {
            BackendError::QuotaExceeded
        } else if self.makes_database_unusable() {
            BackendError::Unusable(format!("{self:?}"))
        } else if self.is_constraint_violation() {
            BackendError::Constraint
        } else {
            BackendError::DbErr(format!("{self:?}"))
        }
    }
}

impl BackendErrorMapper for SqlError {
    fn into_backend_error(self) -> BackendError {
        self.to_backend_error()
    }
}
