/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use log::info;

use super::executor::{
    SqlError, SqlExecutor, SqlParam, TransactionBehavior, query_row, with_transaction,
};
use crate::indexeddb::IndexedDBDescription;
use crate::shared::DB_INIT_PRAGMAS;

// Adapted from:
// https://github.com/mozilla-firefox/firefox/blob/ee102e926521b3e460293b0aea6b54b1a03f6f74/dom/indexedDB/DBSchema.cpp#L78
const CREATE_TABLES: [&str; 6] = [
    r#"
CREATE TABLE IF NOT EXISTS database (
    name    varchar          not null
        primary key,
    origin  varchar          not null,
    version bigint default 0 not null
) WITHOUT ROWID;"#,
    r#"
CREATE TABLE IF NOT EXISTS object_store (
    id             integer               not null
        primary key autoincrement,
    name           varchar               not null
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
    name              varchar        not null,
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
];

/// Creates the tables of a new database.
pub(crate) fn init_database<X: SqlExecutor>(
    executor: &mut X,
    description: &IndexedDBDescription,
) -> Result<(), SqlError> {
    let exists = query_row(
        executor,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'database'",
        &[],
        |_| Ok(()),
    )?
    .is_some();
    if exists {
        return Ok(());
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
            &[SqlParam::Text(&description.name), SqlParam::Text(&origin)],
        )?;
        Ok(())
    })
}
