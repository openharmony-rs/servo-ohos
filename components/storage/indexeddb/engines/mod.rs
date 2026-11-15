/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::VecDeque;

use malloc_size_of::MallocSizeOf;
use malloc_size_of_derive::MallocSizeOf;
use storage_traits::indexeddb::{
    AsyncOperation, BackendResult, IndexedDBIndex, IndexedDBTxnMode, KeyPath,
};

pub(crate) use crate::indexeddb::backend::ActiveKvsEngine;

#[path = "sqlite/encoding.rs"]
pub(crate) mod encoding;

#[derive(MallocSizeOf)]
pub struct KvsOperation {
    pub store_name: String,
    pub operation: AsyncOperation,
}

#[derive(MallocSizeOf)]
pub struct KvsTransaction {
    // Mode could be used by a more optimal implementation of transactions
    // that has different allocated threadpools for reading and writing
    pub mode: IndexedDBTxnMode,
    pub requests: VecDeque<KvsOperation>,
}

pub trait KvsEngine: MallocSizeOf {
    type Error: std::error::Error;

    #[expect(dead_code)]
    fn close_store(&self, store_name: &str) -> Result<(), Self::Error>;

    /// Runs a batch of requests of `transaction` after all work handed over before, and calls
    /// `on_complete` once they have run.
    fn process_transaction(
        &self,
        transaction: u64,
        batch: KvsTransaction,
        on_complete: Box<dyn FnOnce() + Send + 'static>,
    );

    /// Makes the database's version `version` as part of the upgrade `transaction`, so that
    /// aborting the transaction reverts it.
    fn upgrade_version(&self, transaction: u64, version: u64);

    /// Commits `transaction` after all work handed over before, and calls `on_done` with the
    /// result once it is durable or has failed.
    fn commit(&self, transaction: u64, on_done: Box<dyn FnOnce(BackendResult<()>) + Send>);

    /// Undoes everything `transaction` changed, after all work handed over before, and calls
    /// `on_done` once it is undone.
    fn rollback(&self, transaction: u64, on_done: Box<dyn FnOnce() + Send>);

    fn key_generator_current_number(&self, store_name: &str) -> Option<i64>;
    fn key_path(&self, store_name: &str) -> Option<KeyPath>;
    fn object_store_names(&self) -> Result<Vec<String>, Self::Error>;
    fn indexes(&self, store_name: &str) -> Result<Vec<IndexedDBIndex>, Self::Error>;

    fn version(&self) -> Result<u64, Self::Error>;
}
