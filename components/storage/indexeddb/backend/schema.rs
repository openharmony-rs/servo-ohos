/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::Path;

use log::info;

use super::executor::{
    SqlError, SqlExecutor, SqlParam, TransactionBehavior, query_row, with_transaction,
};
use crate::indexeddb::IndexedDBDescription;
use crate::shared::DB_INIT_PRAGMAS;

/// The version of the tables below and of how records are stored in them, kept in the database
/// file's `user_version`. Bump it with every change to either: a database stored with another
/// version is deleted and created anew when it is opened, as there are no migrations.
pub(crate) const SCHEMA_VERSION: i64 = 3;

// Adapted from:
// https://github.com/mozilla-firefox/firefox/blob/ee102e926521b3e460293b0aea6b54b1a03f6f74/dom/indexedDB/DBSchema.cpp#L78
// Names are BLOBs of UTF-16 code units (`Utf16String::to_be_bytes`), which, unlike text, hold
// any DOMString. A unique index keeps its records in `unique_index_data`, whose primary key allows
// one record per key, any other index in `index_data`; both are read in (key, primary key) order.
// Their indexes on the record's key find the index records of the records a write replaces or
// deletes.
const CREATE_TABLES: [&str; 8] = [
    r#"
CREATE TABLE IF NOT EXISTS database (
    name    blob             not null
        primary key,
    origin  varchar          not null,
    version bigint default 0 not null
) WITHOUT ROWID;"#,
    r#"
CREATE TABLE IF NOT EXISTS object_store (
    id             integer               not null
        primary key autoincrement,
    name           blob                  not null
        unique,
    key_path       varbinary_blob,
    auto_increment integer default FALSE not null
);"#,
    r#"
CREATE TABLE IF NOT EXISTS object_data (
    object_store_id integer not null
        references object_store,
    key             blob    not null,
    data            blob    not null,
    constraint "pk-object_data"
        primary key (object_store_id, key)
) WITHOUT ROWID;"#,
    r#"
CREATE TABLE IF NOT EXISTS object_store_index (
    id                integer        not null
    primary key autoincrement,
    object_store_id   integer        not null
    references object_store,
    name              blob           not null,
    key_path          varbinary_blob not null,
    unique_index      boolean        not null,
    multi_entry_index boolean        not null,
    unique(object_store_id, name)
);"#,
    r#"
CREATE TABLE IF NOT EXISTS index_data (
    index_id INTEGER NOT NULL,
    value BLOB NOT NULL,
    object_data_key BLOB NOT NULL,
    object_store_id INTEGER NOT NULL,
    value_locale BLOB,
    PRIMARY KEY (index_id, value, object_data_key)
    FOREIGN KEY (index_id) REFERENCES object_store_index(id),
    FOREIGN KEY (object_store_id, object_data_key)
    REFERENCES object_data(object_store_id, key)
) WITHOUT ROWID;"#,
    r#"
CREATE TABLE IF NOT EXISTS unique_index_data (
    index_id INTEGER NOT NULL,
    value BLOB NOT NULL,
    object_store_id INTEGER NOT NULL,
    object_data_key BLOB NOT NULL,
    value_locale BLOB,
    PRIMARY KEY (index_id, value),
    FOREIGN KEY (index_id) REFERENCES object_store_index(id),
    FOREIGN KEY (object_store_id, object_data_key)
    REFERENCES object_data(object_store_id, key)
) WITHOUT ROWID;"#,
    r#"
CREATE INDEX IF NOT EXISTS index_data_object_data_key_index
    ON index_data (object_store_id, object_data_key);"#,
    r#"
CREATE INDEX IF NOT EXISTS unique_index_data_object_data_key_index
    ON unique_index_data (object_store_id, object_data_key);"#,
];

/// Opens the database in `directory`, creating its tables if it is new. A database stored with
/// another [`SCHEMA_VERSION`] is deleted and created anew. Returns the connection and whether
/// the database was created.
pub(crate) fn open_database<X: SqlExecutor>(
    directory: &Path,
    description: &IndexedDBDescription,
) -> Result<(X, bool), SqlError> {
    let mut executor = X::open(directory)?;
    let stored_version = stored_schema_version(&mut executor)?;
    // A new database file has neither a version nor tables.
    if stored_version != SCHEMA_VERSION && (stored_version != 0 || is_initialized(&mut executor)?) {
        info!(
            "Resetting indexeddb database {:?} of schema version {stored_version}",
            description.name
        );
        executor.delete()?;
        executor = X::open(directory)?;
        // RDB's delete reports success even when it couldn't remove the files.
        if stored_schema_version(&mut executor)? != 0 || is_initialized(&mut executor)? {
            return Err(SqlError::ResetFailed);
        }
    }
    let created = init_database(&mut executor, description)?;
    Ok((executor, created))
}

/// The schema version a database was stored with, 0 if it has none.
pub(crate) fn stored_schema_version<X: SqlExecutor>(executor: &mut X) -> Result<i64, SqlError> {
    Ok(query_row(executor, "PRAGMA user_version", &[], |row| row.integer(0))?.unwrap_or(0))
}

fn is_initialized<X: SqlExecutor>(executor: &mut X) -> Result<bool, SqlError> {
    Ok(query_row(
        executor,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'database'",
        &[],
        |_| Ok(()),
    )?
    .is_some())
}

/// Creates the tables of a new database, stamped with [`SCHEMA_VERSION`]. Returns whether the
/// database was new.
fn init_database<X: SqlExecutor>(
    executor: &mut X,
    description: &IndexedDBDescription,
) -> Result<bool, SqlError> {
    if is_initialized(executor)? {
        return Ok(false);
    }

    info!("Initializing indexeddb database {:?}", description.name);
    for pragma in DB_INIT_PRAGMAS {
        // FIXME(arihant2math): this fails occasionally
        let _ = executor.execute(pragma, &[]);
    }
    let origin = description.origin.ascii_serialization();
    with_transaction(executor, TransactionBehavior::Immediate, |executor| {
        for statement in CREATE_TABLES {
            executor.execute(statement, &[])?;
        }
        // From https://w3c.github.io/IndexedDB/#database-version:
        // "When a database is first created, its version is 0 (zero)."
        executor.execute(
            "INSERT INTO database (name, origin, version) VALUES (?, ?, 0)",
            &[
                SqlParam::Blob(&description.name.to_be_bytes()),
                SqlParam::Text(&origin),
            ],
        )?;
        executor.execute(&format!("PRAGMA user_version = {SCHEMA_VERSION}"), &[])?;
        Ok(true)
    })
}
