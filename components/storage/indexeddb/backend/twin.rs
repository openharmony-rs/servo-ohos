/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Both executors in one binary, chosen per database open by the
//! `storage_ohos_rdb_backend_enabled` pref, so that SQLite and OHOS RDB can be compared without
//! reinstalling.

use std::path::Path;

use super::executor::{Connector, OnRow, SqlError, SqlExecutor, SqlParam, TransactionBehavior};
use super::rdb::RdbExecutor;
use super::sqlite::SqliteExecutor;
use crate::shared::use_ohos_rdb_backend;

pub(crate) enum TwinExecutor {
    Sqlite(SqliteExecutor),
    Rdb(RdbExecutor),
}

macro_rules! delegate {
    ($self:expr, |$executor:ident| $call:expr) => {
        match $self {
            TwinExecutor::Sqlite($executor) => $call,
            TwinExecutor::Rdb($executor) => $call,
        }
    };
}

impl SqlExecutor for TwinExecutor {
    fn open(directory: &Path) -> Result<Self, SqlError> {
        if use_ohos_rdb_backend() {
            RdbExecutor::open(directory).map(Self::Rdb)
        } else {
            SqliteExecutor::open(directory).map(Self::Sqlite)
        }
    }

    fn connector(&self) -> Connector<Self> {
        match self {
            Self::Sqlite(executor) => {
                let connector = executor.connector();
                Box::new(move || connector().map(Self::Sqlite))
            },
            Self::Rdb(executor) => {
                let connector = executor.connector();
                Box::new(move || connector().map(Self::Rdb))
            },
        }
    }

    fn execute(&mut self, sql: &str, params: &[SqlParam<'_>]) -> Result<u64, SqlError> {
        delegate!(self, |executor| executor.execute(sql, params))
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam<'_>],
        on_row: &mut OnRow<'_>,
    ) -> Result<(), SqlError> {
        delegate!(self, |executor| executor.query(sql, params, on_row))
    }

    fn begin(&mut self, behavior: TransactionBehavior) -> Result<(), SqlError> {
        delegate!(self, |executor| executor.begin(behavior))
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        delegate!(self, |executor| executor.commit())
    }

    fn rollback(&mut self) -> Result<(), SqlError> {
        delegate!(self, |executor| executor.rollback())
    }

    fn in_transaction(&self) -> bool {
        delegate!(self, |executor| executor.in_transaction())
    }

    fn heap_size(&self) -> usize {
        delegate!(self, |executor| executor.heap_size())
    }
}
