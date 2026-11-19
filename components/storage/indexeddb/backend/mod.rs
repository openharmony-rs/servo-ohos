/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The IndexedDB backend: one IndexedDB-on-SQL layer ([`SqlEngine`], `queries`, `schema`) over a
//! small SQL executor interface ([`executor::SqlExecutor`]), which is implemented for the bundled
//! SQLite and for OpenHarmony's RDB. With both compiled in, the
//! `storage_ohos_rdb_backend_enabled` pref chooses the executor per database.

mod engine;
pub(crate) mod executor;
mod queries;
#[cfg(ohos_rdb)]
pub(crate) mod rdb;
mod schema;
#[cfg(feature = "sqlite-backend")]
pub(crate) mod sqlite;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(all(feature = "sqlite-backend", ohos_rdb))]
pub(crate) mod twin;

pub(crate) use engine::SqlEngine;

#[cfg(all(feature = "sqlite-backend", not(ohos_rdb)))]
type ActiveExecutor = sqlite::SqliteExecutor;
#[cfg(all(not(feature = "sqlite-backend"), ohos_rdb))]
type ActiveExecutor = rdb::RdbExecutor;
#[cfg(all(feature = "sqlite-backend", ohos_rdb))]
type ActiveExecutor = twin::TwinExecutor;

/// The engine the IndexedDB manager runs on.
pub(crate) type ActiveKvsEngine = SqlEngine<ActiveExecutor>;
