/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::VecDeque;

use log::debug;
use malloc_size_of::MallocSizeOf;
use malloc_size_of_derive::MallocSizeOf;
use profile_traits::generic_callback::GenericCallback;
use storage_traits::indexeddb::{
    AsyncOperation, BackendError, BackendResult, ConnectionEvent, IndexedDBObjectStore,
    IndexedDBObjectStoreId, IndexedDBTxnId, IndexedDBTxnMode, TransactionEvent,
};

pub(crate) use crate::indexeddb::backend::ActiveKvsEngine;

#[path = "sqlite/encoding.rs"]
pub(crate) mod encoding;

#[derive(MallocSizeOf)]
pub struct KvsOperation {
    pub store: IndexedDBObjectStoreId,
    /// The id of the request in its transaction; `None` for a schema operation, which only
    /// reports a failure.
    pub request_id: Option<u64>,
    pub operation: AsyncOperation,
}

impl KvsOperation {
    /// What to report when the operation fails with `error`.
    pub fn failure_event(&self, error: BackendError) -> TransactionEvent {
        match self.request_id {
            Some(request_id) => TransactionEvent::Request {
                request_id,
                result: Err(error),
            },
            None => TransactionEvent::SchemaChangeFailed(error),
        }
    }
}

/// Where the results of a transaction go: the result channel of its connection, which keeps the
/// order in which they are sent.
#[derive(Clone, MallocSizeOf)]
pub struct TransactionResults {
    transaction: IndexedDBTxnId,
    /// `None` for a transaction of a connection the backend doesn't know, whose results are
    /// dropped.
    channel: Option<GenericCallback<ConnectionEvent>>,
}

impl TransactionResults {
    pub fn new(
        transaction: IndexedDBTxnId,
        channel: Option<GenericCallback<ConnectionEvent>>,
    ) -> Self {
        Self {
            transaction,
            channel,
        }
    }

    pub fn send(&self, event: TransactionEvent) {
        let Some(channel) = &self.channel else {
            return debug!(
                "Dropped a result of IndexedDB transaction {}, whose connection is unknown",
                self.transaction
            );
        };
        let message = ConnectionEvent::Transaction {
            serial: self.transaction.serial,
            event,
        };
        if channel.send(message).is_err() {
            debug!(
                "Script went away before a result of IndexedDB transaction {}",
                self.transaction
            );
        }
    }
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

    /// Runs a batch of requests of `transaction` after all work handed over before, sends each
    /// result to `results` as the request has run, and calls `on_complete` once they have run.
    fn process_transaction(
        &self,
        transaction: IndexedDBTxnId,
        batch: KvsTransaction,
        results: TransactionResults,
        on_complete: Box<dyn FnOnce() + Send + 'static>,
    );

    /// Makes the database's version `version` as part of the upgrade `transaction`, so that
    /// aborting the transaction reverts it.
    fn upgrade_version(&self, transaction: IndexedDBTxnId, version: u64);

    /// Commits `transaction` after all work handed over before, and calls `on_done` with the
    /// result once it is durable or has failed.
    fn commit(
        &self,
        transaction: IndexedDBTxnId,
        on_done: Box<dyn FnOnce(BackendResult<()>) + Send>,
    );

    /// Undoes everything `transaction` changed, after all work handed over before, and calls
    /// `on_done` once it is undone.
    fn rollback(&self, transaction: IndexedDBTxnId, on_done: Box<dyn FnOnce() + Send>);

    // The metadata reads answer as of the last commit, without waiting for the work handed over.
    /// The object stores of the database, in the order they were created.
    fn object_stores(&self) -> Vec<IndexedDBObjectStore>;
    fn version(&self) -> Result<u64, Self::Error>;
}
