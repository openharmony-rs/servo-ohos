/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

mod backend;
mod engines;

use std::borrow::ToOwned;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::thread;

use log::{debug, error, warn};
use malloc_size_of::MallocSizeOf;
use malloc_size_of_derive::MallocSizeOf;
use profile_traits::generic_callback::GenericCallback;
use profile_traits::mem::{
    ProcessReports, ProfilerChan as MemProfilerChan, Report, ReportKind, perform_memory_report,
};
use profile_traits::path;
#[cfg(feature = "sqlite-backend")]
use rusqlite::Error as RusqliteError;
use rustc_hash::{FxHashMap, FxHashSet};
use servo_base::generic_channel::{self, GenericReceiver, GenericSender, ReceiveError};
use servo_base::threadpool::ThreadPool;
use servo_url::origin::ImmutableOrigin;
use storage_traits::client_storage::{StorageIdentifier, StorageProxyMap, StorageType};
use storage_traits::indexeddb::{
    AsyncOperation, BackendError, BackendResult, ConnectionEvent, ConnectionMsg, DatabaseInfo,
    IndexedDBObjectStore, IndexedDBObjectStoreId, IndexedDBThreadMsg, IndexedDBTxnId,
    IndexedDBTxnMode, StorageBottleSource, SyncOperation, TransactionEvent, Utf16String,
};
use uuid::Uuid;

use crate::indexeddb::engines::{
    ActiveKvsEngine, KvsEngine, KvsOperation, KvsTransaction, TransactionResults,
};
#[cfg(feature = "sqlite-backend")]
use crate::shared::is_sqlite_disk_full_error;

pub trait IndexedDBThreadFactory {
    fn new(mem_profiler_chan: MemProfilerChan, reporter_name: String) -> Self;
}

impl IndexedDBThreadFactory for GenericSender<IndexedDBThreadMsg> {
    fn new(
        mem_profiler_chan: MemProfilerChan,
        reporter_name: String,
    ) -> GenericSender<IndexedDBThreadMsg> {
        let (chan, port) = generic_channel::channel().unwrap();
        let chan2 = chan.clone();

        let manager_sender = chan.clone();

        thread::Builder::new()
            .name("IndexedDBManager".to_owned())
            .spawn(move || {
                mem_profiler_chan.run_with_memory_reporting(
                    || IndexedDBManager::new(port, manager_sender).start(),
                    reporter_name,
                    chan2,
                    IndexedDBThreadMsg::CollectMemoryReport,
                );
            })
            .expect("Thread spawning failed");

        chan
    }
}

/// A key used to track databases.
#[derive(Clone, Debug, Eq, Hash, MallocSizeOf, PartialEq)]
pub struct IndexedDBDescription {
    pub origin: ImmutableOrigin,
    pub name: Utf16String,
}

#[derive(MallocSizeOf)]
struct TxnInfo {
    created_seq: u64,
    mode: IndexedDBTxnMode,
    scope: Vec<IndexedDBObjectStoreId>,
    live: bool,
    /// Whether the engine has been handed a batch of this transaction.
    started: bool,
    /// The result channel of the transaction's connection.
    results: GenericCallback<ConnectionEvent>,
}

#[derive(MallocSizeOf)]
struct IndexedDBEnvironment<E: KvsEngine> {
    engine: E,
    manager_sender: GenericSender<IndexedDBThreadMsg>,
    transactions: FxHashMap<IndexedDBTxnId, KvsTransaction>,
    next_created_seq: u64,
    txn_info: FxHashMap<IndexedDBTxnId, TxnInfo>,
    queued_readwrite: VecDeque<IndexedDBTxnId>,
    queued_readonly: VecDeque<IndexedDBTxnId>,
    // Fast membership checks + de-dup for queued_*.
    queued_readwrite_set: FxHashSet<IndexedDBTxnId>,
    queued_readonly_set: FxHashSet<IndexedDBTxnId>,
    running_readwrite: Option<IndexedDBTxnId>,
    running_readonly: HashSet<IndexedDBTxnId>,
    /// Transactions that script asked to commit, which commit once they can start and all their
    /// requests have been handed to the engine.
    pending_commits: FxHashSet<IndexedDBTxnId>,
}

impl<E: KvsEngine> IndexedDBEnvironment<E> {
    fn new(
        engine: E,
        manager_sender: GenericSender<IndexedDBThreadMsg>,
    ) -> IndexedDBEnvironment<E> {
        IndexedDBEnvironment {
            engine,
            manager_sender,
            transactions: FxHashMap::default(),
            next_created_seq: 0,
            txn_info: FxHashMap::default(),
            queued_readwrite: VecDeque::new(),
            queued_readonly: VecDeque::new(),
            queued_readwrite_set: FxHashSet::default(),
            queued_readonly_set: FxHashSet::default(),
            running_readwrite: None,
            running_readonly: HashSet::new(),
            pending_commits: FxHashSet::default(),
        }
    }

    /// Registers the transaction `txn`, whose results go to `results`.
    fn register_transaction(
        &mut self,
        txn: IndexedDBTxnId,
        mode: IndexedDBTxnMode,
        scope: Vec<IndexedDBObjectStoreId>,
        results: GenericCallback<ConnectionEvent>,
    ) {
        if self.txn_info.contains_key(&txn) {
            return;
        }
        let scope: HashSet<IndexedDBObjectStoreId> = scope.into_iter().collect();
        let created_seq = self.next_created_seq;
        self.next_created_seq += 1;
        self.txn_info.insert(
            txn,
            TxnInfo {
                created_seq,
                mode: mode.clone(),
                scope: scope.into_iter().collect(),
                live: true,
                started: false,
                results,
            },
        );
        self.transactions
            .entry(txn)
            .or_insert_with(|| KvsTransaction {
                requests: VecDeque::new(),
                mode,
            });
    }

    fn knows_transaction(&self, txn: IndexedDBTxnId) -> bool {
        self.txn_info.contains_key(&txn)
    }

    /// Why `operation` can't run in `txn`, if it can't: DbNotFound for a transaction that isn't
    /// registered, Abort for one that was aborted, and DbErr for an operation that its mode or
    /// scope doesn't allow, which script never sends.
    fn check_operation(
        &self,
        txn: IndexedDBTxnId,
        operation: &KvsOperation,
    ) -> Result<(), BackendError> {
        let info = self.check_usable(txn)?;
        let allowed = match (&info.mode, &operation.operation) {
            (IndexedDBTxnMode::Versionchange, _) => true,
            (_, AsyncOperation::Schema(_)) => false,
            (IndexedDBTxnMode::Readonly, AsyncOperation::ReadWrite(_)) => false,
            _ => info.scope.contains(&operation.store),
        };
        if !allowed {
            return Err(BackendError::DbErr(format!(
                "A {:?} transaction can't run this request on store {}",
                info.mode, operation.store
            )));
        }
        Ok(())
    }

    /// The scheduling state of `txn`, if it is registered and wasn't aborted.
    fn check_usable(&self, txn: IndexedDBTxnId) -> Result<&TxnInfo, BackendError> {
        let info = self.txn_info.get(&txn).ok_or(BackendError::DbNotFound)?;
        if !self.transactions.contains_key(&txn) {
            return Err(BackendError::Abort);
        }
        Ok(info)
    }

    /// Where the results of `txn` go.
    fn results(&self, txn: IndexedDBTxnId) -> TransactionResults {
        TransactionResults::new(
            txn,
            self.txn_info.get(&txn).map(|info| info.results.clone()),
        )
    }

    fn scopes_overlap(a: &TxnInfo, b: &TxnInfo) -> bool {
        // From <https://w3c.github.io/IndexedDB/#upgrade-transaction-construct>
        // > An upgrade transaction is exclusive. The steps to open a database connection
        // > ensure that only one connection to the database is open when an upgrade
        // > transaction is live.
        //
        // This mean that versionupgrade transactions are always overlapping with others.
        // They are defined to contain all of the object stores in the database.
        if a.mode == IndexedDBTxnMode::Versionchange || b.mode == IndexedDBTxnMode::Versionchange {
            return true;
        }

        a.scope.iter().any(|store| b.scope.contains(store))
    }

    fn earlier_overlapping_live_exists<F>(&self, transaction: IndexedDBTxnId, predicate: F) -> bool
    where
        F: Fn(&TxnInfo) -> bool,
    {
        let Some(current) = self.txn_info.get(&transaction) else {
            return false;
        };

        let comes_before = |other_transaction: &TxnInfo| {
            other_transaction.live && other_transaction.created_seq < current.created_seq
        };

        self.txn_info.iter().any(|(other_transaction, other)| {
            *other_transaction != transaction &&
                comes_before(other) &&
                Self::scopes_overlap(current, other) &&
                predicate(other)
        })
    }

    fn can_start_by_spec(&self, txn: IndexedDBTxnId) -> bool {
        let Some(info) = self.txn_info.get(&txn) else {
            return false;
        };
        match info.mode {
            IndexedDBTxnMode::Readonly => !self.earlier_overlapping_live_exists(txn, |other| {
                other.mode != IndexedDBTxnMode::Readonly
            }),
            IndexedDBTxnMode::Readwrite | IndexedDBTxnMode::Versionchange => {
                !self.earlier_overlapping_live_exists(txn, |_other| true) &&
                    !self.other_writer_started(txn)
            },
        }
    }

    /// Whether a readwrite or versionchange transaction other than `txn` has started and is
    /// still live. The engine runs each such transaction as the one SQL transaction of its
    /// connection, so writers run one after another, also those with disjoint scopes.
    fn other_writer_started(&self, txn: IndexedDBTxnId) -> bool {
        self.txn_info.iter().any(|(other_txn, other)| {
            *other_txn != txn &&
                other.live &&
                other.started &&
                other.mode != IndexedDBTxnMode::Readonly
        })
    }

    fn enqueue_txn(&mut self, txn: IndexedDBTxnId, mode: &IndexedDBTxnMode) {
        match mode {
            IndexedDBTxnMode::Readonly => {
                if self.queued_readonly_set.insert(txn) {
                    self.queued_readonly.push_back(txn);
                }
            },
            _ => {
                if self.queued_readwrite_set.insert(txn) {
                    self.queued_readwrite.push_back(txn);
                }
            },
        }
    }

    fn queue_operation(
        &mut self,
        serial_number: IndexedDBTxnId,
        mode: IndexedDBTxnMode,
        operation: KvsOperation,
    ) {
        let mut enqueue_mode = None;
        match self.transactions.entry(serial_number) {
            Entry::Occupied(mut entry) => {
                let transaction = entry.get_mut();
                let transaction_mode = transaction.mode.clone();
                let was_empty = transaction.requests.is_empty();
                transaction.requests.push_back(operation);
                if was_empty {
                    // If the queue was empty and we just added work, we must enqueue the txn
                    // even if it is currently running a prior batch. The next batch will run
                    // after EngineTxnBatchComplete triggers scheduling again.
                    enqueue_mode = Some(transaction_mode);
                }
            },
            Entry::Vacant(entry) => {
                entry
                    .insert(KvsTransaction {
                        requests: VecDeque::new(),
                        mode: mode.clone(),
                    })
                    .requests
                    .push_back(operation);
                enqueue_mode = Some(mode);
            },
        };
        if let Some(mode) = enqueue_mode {
            self.enqueue_txn(serial_number, &mode);
        }
    }

    /// <https://w3c.github.io/IndexedDB/#transaction-scheduling>
    fn schedule_transactions(&mut self, origin: ImmutableOrigin, db_name: &Utf16String) {
        let readonly_len = self.queued_readonly.len();
        for _ in 0..readonly_len {
            let Some(txn) = self.queued_readonly.pop_front() else {
                break;
            };

            self.queued_readonly_set.remove(&txn);

            let Some(transaction) = self.transactions.get(&txn) else {
                continue;
            };
            if self.running_readonly.contains(&txn) {
                if !transaction.requests.is_empty() && self.queued_readonly_set.insert(txn) {
                    self.queued_readonly.push_back(txn);
                }
                continue;
            }
            if transaction.requests.is_empty() {
                continue;
            }
            if !self.can_start_by_spec(txn) {
                if self.queued_readonly_set.insert(txn) {
                    self.queued_readonly.push_back(txn);
                }
                continue;
            }

            // Mark running and start async.
            // DO NOT clear here; clear on EngineTxnBatchComplete.
            self.running_readonly.insert(txn);
            self.start_transaction(origin.clone(), db_name.clone(), txn, None);
        }

        // Start at most one readwrite txn.
        if self.running_readwrite.is_some() {
            return;
        }
        let readwrite_len = self.queued_readwrite.len();
        for _ in 0..readwrite_len {
            let Some(txn) = self.queued_readwrite.pop_front() else {
                break;
            };

            self.queued_readwrite_set.remove(&txn);

            let Some(transaction) = self.transactions.get(&txn) else {
                continue;
            };

            if self.running_readwrite == Some(txn) {
                if !transaction.requests.is_empty() && self.queued_readwrite_set.insert(txn) {
                    self.queued_readwrite.push_back(txn);
                }
                continue;
            }

            if transaction.requests.is_empty() {
                continue;
            }
            if !self.can_start_by_spec(txn) {
                if self.queued_readwrite_set.insert(txn) {
                    self.queued_readwrite.push_back(txn);
                }
                continue;
            }

            // Mark running and start async.
            // DO NOT clear here; clear on EngineTxnBatchComplete.
            self.running_readwrite = Some(txn);
            self.start_transaction(origin, db_name.clone(), txn, None);
            return;
        }
    }

    // Executes all requests for a transaction (without committing)
    fn start_transaction(
        &mut self,
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
        sender: Option<GenericSender<BackendResult<()>>>,
    ) {
        // https://w3c.github.io/IndexedDB/#transaction-lifetime
        // Take the current queued batch of requests for this txn.
        // If more requests arrive while the engine is running, they’ll be queued into
        // transaction.requests and scheduled in a later batch once we receive EngineTxnBatchComplete.
        let (mode, requests) = match self.transactions.get_mut(&txn) {
            Some(transaction) => {
                let mode = transaction.mode.clone();
                let requests = std::mem::take(&mut transaction.requests);
                (mode, requests)
            },
            None => {
                // If a manual starter is waiting, treat as "nothing to do".
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(()));
                }
                return;
            },
        };

        if requests.is_empty() {
            if let Some(sender) = sender {
                let _ = sender.send(Ok(()));
            }
            // Important: if there was no work, do NOT send EngineTxnBatchComplete,
            // otherwise we can create a pointless reschedule loop.
            return;
        }

        if let Some(info) = self.txn_info.get_mut(&txn) {
            info.started = true;
        }
        let manager_sender = self.manager_sender.clone();
        self.engine.process_transaction(
            txn,
            KvsTransaction { mode, requests },
            self.results(txn),
            Box::new(move || {
                // Notify the manager thread when the engine finishes so it can:
                // - clear running_readonly / running_readwrite
                // - re-run scheduling (maybe start next queued txn or next batch)
                if let Err(err) = manager_sender.send(IndexedDBThreadMsg::EngineTxnBatchComplete {
                    origin,
                    db_name,
                    txn,
                }) {
                    error!(
                        "Failed to send IndexedDBThreadMsg::EngineTxnBatchComplete: {:?}",
                        err
                    );
                    // TODO: Prevent backend/manager shutdown while a transaction batch is
                    // running so completion notification cannot be dropped in normal flow.
                }

                // We have a sender if the transaction is started manually, and they
                // probably want to know when it is finished.
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(()));
                }
            }),
        );
    }

    /// Whether no request of `txn` waits to be handed to the engine or runs.
    fn is_idle(&self, txn: IndexedDBTxnId) -> bool {
        if self.running_readwrite == Some(txn) || self.running_readonly.contains(&txn) {
            return false;
        }
        // Avoid if the txn is still queued or has queued operations.
        if self.queued_readonly_set.contains(&txn) || self.queued_readwrite_set.contains(&txn) {
            return false;
        }
        match self.transactions.get(&txn) {
            Some(t) => t.requests.is_empty(),
            None => true,
        }
    }

    fn can_commit_now(&self, txn: IndexedDBTxnId) -> bool {
        self.can_start_by_spec(txn) && self.is_idle(txn)
    }

    /// The transactions that script asked to commit and that can commit now, in the order they
    /// were created.
    fn commit_candidates(&self) -> Vec<IndexedDBTxnId> {
        let mut candidates: Vec<(IndexedDBTxnId, u64)> = self
            .pending_commits
            .iter()
            .filter_map(|txn| {
                let info = self.txn_info.get(txn)?;
                if !info.live || !self.can_commit_now(*txn) {
                    return None;
                }
                Some((*txn, info.created_seq))
            })
            .collect();
        candidates.sort_by_key(|(_, created_seq)| *created_seq);
        candidates.into_iter().map(|(txn, _)| txn).collect()
    }

    /// Commits the transactions that script asked to commit and that can commit now.
    fn commit_ready_transactions(&mut self) {
        for txn in self.commit_candidates() {
            self.pending_commits.remove(&txn);
            self.commit(txn);
        }
    }

    fn finish_transaction(&mut self, txn: IndexedDBTxnId) {
        if let Some(info) = self.txn_info.get_mut(&txn) {
            info.live = false;
        }
        self.txn_info.remove(&txn);
        self.transactions.remove(&txn);
        self.queued_readonly.retain(|queued| *queued != txn);
        self.queued_readwrite.retain(|queued| *queued != txn);
        self.queued_readonly_set.remove(&txn);
        self.queued_readwrite_set.remove(&txn);
        if self.running_readwrite == Some(txn) {
            self.running_readwrite = None;
        }
        self.running_readonly.remove(&txn);
        self.pending_commits.remove(&txn);
    }

    fn abort_transaction(&mut self, txn: IndexedDBTxnId) {
        // Keep scheduling metadata until script reports TransactionFinished.
        // https://w3c.github.io/IndexedDB/#transaction-lifetime
        self.transactions.remove(&txn);
        self.queued_readonly.retain(|queued| *queued != txn);
        self.queued_readwrite.retain(|queued| *queued != txn);
        self.queued_readonly_set.remove(&txn);
        self.queued_readwrite_set.remove(&txn);
        if self.running_readwrite == Some(txn) {
            self.running_readwrite = None;
        }
        self.running_readonly.remove(&txn);
        self.pending_commits.remove(&txn);
    }

    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>
    ///
    /// Reports the result to the transaction's connection once the engine has written the
    /// transaction's changes, or failed to.
    fn commit(&self, txn: IndexedDBTxnId) {
        let results = self.results(txn);
        self.engine.commit(
            txn,
            Box::new(move |result| results.send(TransactionEvent::Committed(result))),
        );
    }

    /// The committed metadata of the object stores, for a connection's object store set.
    fn object_stores(&self) -> Vec<IndexedDBObjectStore> {
        self.engine.object_stores()
    }

    fn version(&self) -> Result<u64, E::Error> {
        self.engine.version()
    }

    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>
    ///
    /// > When a transaction is aborted the implementation must undo (roll back) any changes that
    /// > were made to the database during that transaction.
    ///
    /// The engine rolls the transaction back after the work handed over before, which also
    /// reverts the key generators and, for an upgrade transaction, the version and the object
    /// stores and indexes. A commit that script asked for and that hasn't been handed over is
    /// dropped.
    fn abort(
        &mut self,
        origin: &ImmutableOrigin,
        database_name: &Utf16String,
        transaction: IndexedDBTxnId,
    ) {
        self.engine.rollback(transaction, Box::new(|| {}));
        self.abort_transaction(transaction);
        self.schedule_transactions(origin.clone(), database_name);
    }
}

trait BackendErrorMapper: std::error::Error {
    fn into_backend_error(self) -> BackendError;
}

#[cfg(feature = "sqlite-backend")]
impl BackendErrorMapper for RusqliteError {
    fn into_backend_error(self) -> BackendError {
        if is_sqlite_disk_full_error(&self) {
            BackendError::QuotaExceeded
        } else {
            BackendError::DbErr(format!("{self:?}"))
        }
    }
}

fn backend_error_from_storage_error<E: BackendErrorMapper>(err: E) -> BackendError {
    err.into_backend_error()
}

/// <https://storage.spec.whatwg.org/#obtain-a-local-storage-bottle-map>
fn obtain_a_local_storage_bottle_map(
    source: &StorageBottleSource,
    origin: &ImmutableOrigin,
) -> BackendResult<StorageProxyMap> {
    source
        .client_storage
        .obtain_a_storage_bottle_map(
            StorageType::Local,
            source.webview,
            StorageIdentifier::IndexedDB,
            origin.clone(),
        )
        .recv()
        .map_err(|_| BackendError::DbErr("Failed to communicate with client storage.".to_owned()))?
        .map_err(|error| BackendError::DbErr(format!("Client storage error: {error}")))
}

/// <https://w3c.github.io/IndexedDB/#request-open-request>
/// Used here to implement the
/// <https://w3c.github.io/IndexedDB/#connection-queue>
#[derive(MallocSizeOf)]
enum OpenRequest {
    Open {
        /// The name of the database.
        db_name: Utf16String,

        /// Optionnaly, a requested db version.
        /// Note: when the open algorithm starts, this will be mutated and set to something as per the algo.
        version: Option<u64>,

        /// <https://w3c.github.io/IndexedDB/#request-processed-flag>
        processed: bool,

        /// Optionally, a version pending upgrade.
        pending_upgrade: Option<VersionUpgrade>,

        /// This request is pending on these connections to close.
        pending_close: HashSet<Uuid>,

        /// This request is pending on these connections to fire a versionchange event.
        /// Note: This starts as equal to `pending_close`, but when all events have fired,
        /// not all connections need to have closed, in which case the `blocked` event
        /// is fired on this request.
        pending_versionchange: HashSet<Uuid>,

        id: Uuid,

        /// <https://storage.spec.whatwg.org/#storage-proxy-map>
        proxy_map: StorageProxyMap,

        /// The result channel of the request and of the connection it opens: the request's
        /// outcome and, once it opened, everything about the connection go there, in order.
        results: GenericCallback<ConnectionEvent>,
    },
    Delete {
        /// The callback used to send a result to script.
        sender: GenericCallback<BackendResult<u64>>,

        _origin: ImmutableOrigin,

        /// The name of the database.
        /// Note: will be used when the full spec is implemented.
        db_name: Utf16String,

        /// <https://w3c.github.io/IndexedDB/#request-processed-flag>
        processed: bool,

        id: Uuid,

        /// <https://storage.spec.whatwg.org/#storage-proxy-map>
        proxy_map: StorageProxyMap,
    },
}

impl OpenRequest {
    fn get_id(&self) -> Uuid {
        let id = match self {
            OpenRequest::Open {
                db_name: _,
                version: _,
                processed: _,
                pending_upgrade: _,
                pending_close: _,
                pending_versionchange: _,
                id,
                proxy_map: _,
                results: _,
            } => id,
            OpenRequest::Delete {
                sender: _,
                _origin: _,
                db_name: _,
                processed: _,
                proxy_map: _,
                id,
            } => id,
        };
        *id
    }

    fn proxy_map(&self) -> &StorageProxyMap {
        match self {
            OpenRequest::Open { proxy_map, .. } | OpenRequest::Delete { proxy_map, .. } => {
                proxy_map
            },
        }
    }

    fn is_open(&self) -> bool {
        match self {
            OpenRequest::Open {
                db_name: _,
                version: _,
                processed: _,
                pending_upgrade: _,
                pending_close: _,
                pending_versionchange: _,
                id: _,
                proxy_map: _,
                results: _,
            } => true,
            OpenRequest::Delete {
                sender: _,
                _origin: _,
                db_name: _,
                processed: _,
                proxy_map: _,
                id: _,
            } => false,
        }
    }

    /// An open request remains pending until it has been processed,
    /// and while waiting on upgrade completion or other connections.
    fn is_pending(&self) -> bool {
        match self {
            OpenRequest::Open {
                db_name: _,
                version: _,
                processed,
                pending_upgrade,
                pending_close,
                pending_versionchange,
                id: _,
                proxy_map: _,
                results: _,
            } => {
                !processed ||
                    pending_upgrade.is_some() ||
                    !pending_close.is_empty() ||
                    !pending_versionchange.is_empty()
            },
            OpenRequest::Delete {
                sender: _,
                _origin: _,
                db_name: _,
                processed,
                id: _,
                proxy_map: _,
            } => !processed,
        }
    }

    /// Abort the open request,
    /// optionally returning a version to revert to.
    fn abort(&self) -> Option<VersionUpgrade> {
        match self {
            OpenRequest::Open {
                db_name,
                version: _,
                processed: _,
                pending_close: _,
                pending_versionchange: _,
                pending_upgrade,
                id,
                proxy_map: _,
                results,
            } => {
                if results
                    .send(ConnectionEvent::Open(ConnectionMsg::AbortError {
                        name: db_name.clone(),
                        id: *id,
                    }))
                    .is_err()
                {
                    error!("Failed to send ConnectionMsg::Connection to script.");
                };
                pending_upgrade.clone()
            },
            OpenRequest::Delete {
                sender,
                _origin: _,
                db_name: _,
                processed: _,
                id: _,
                proxy_map: _,
            } => {
                if sender.send(Err(BackendError::DbNotFound)).is_err() {
                    error!("Failed to send result of database delete to script.");
                };
                None
            },
        }
    }
}

#[derive(Clone, MallocSizeOf)]
struct VersionUpgrade {
    old: u64,
    new: u64,
    transaction: IndexedDBTxnId,
}

/// <https://w3c.github.io/IndexedDB/#connection>
#[derive(MallocSizeOf)]
struct Connection {
    /// <https://w3c.github.io/IndexedDB/#connection-close-pending-flag>
    close_pending: bool,

    /// The result channel of the connection, for its transactions and versionchange events.
    results: GenericCallback<ConnectionEvent>,
}

struct IndexedDBManager {
    port: GenericReceiver<IndexedDBThreadMsg>,
    manager_sender: GenericSender<IndexedDBThreadMsg>,
    databases: HashMap<IndexedDBDescription, IndexedDBEnvironment<ActiveKvsEngine>>,
    thread_pool: Arc<ThreadPool>,

    /// <https://w3c.github.io/IndexedDB/#connection-queue>
    connection_queues: HashMap<IndexedDBDescription, VecDeque<OpenRequest>>,

    /// <https://w3c.github.io/IndexedDB/#connection>
    connections: HashMap<IndexedDBDescription, HashMap<Uuid, Connection>>,
}

impl IndexedDBManager {
    fn new(
        port: GenericReceiver<IndexedDBThreadMsg>,
        manager_sender: GenericSender<IndexedDBThreadMsg>,
    ) -> IndexedDBManager {
        debug!("New indexedDBManager");

        IndexedDBManager {
            port,
            manager_sender,
            databases: HashMap::new(),
            thread_pool: ThreadPool::global(),
            connection_queues: Default::default(),
            connections: Default::default(),
        }
    }
}

impl IndexedDBManager {
    fn start(&mut self) {
        loop {
            // FIXME:(arihant2math) No message *most likely* means that
            // the ipc sender has been dropped, so we break the look
            let message = match self.port.recv() {
                Ok(msg) => msg,
                Err(ReceiveError::Disconnected) => {
                    break;
                },
                Err(e) => {
                    warn!("Error in IndexedDB thread: {e:?}");
                    continue;
                },
            };
            match message {
                IndexedDBThreadMsg::Sync(SyncOperation::Exit(sender)) => {
                    let _ = sender.send(());
                    break;
                },
                IndexedDBThreadMsg::Sync(operation) => {
                    self.handle_sync_operation(operation);
                },
                IndexedDBThreadMsg::Async(
                    origin,
                    db_name,
                    store,
                    txn,
                    request_id,
                    mode,
                    operation,
                ) => {
                    let operation = KvsOperation {
                        store,
                        request_id: Some(request_id),
                        operation,
                    };
                    self.queue_operation(origin, db_name, txn, mode, operation);
                },
                IndexedDBThreadMsg::EngineTxnBatchComplete {
                    origin,
                    db_name,
                    txn,
                } => {
                    if let Some(db) = self.get_database_mut(origin.clone(), db_name.clone()) {
                        // Decide which running flag to clear based on txn mode.
                        match db.transactions.get(&txn).map(|t| t.mode.clone()) {
                            Some(IndexedDBTxnMode::Readonly) => {
                                db.running_readonly.remove(&txn);
                            },
                            Some(_) if db.running_readwrite == Some(txn) => {
                                db.running_readwrite = None;
                            },
                            Some(_) => {},
                            None => {
                                // txn might have been aborted/removed; nothing to clear
                            },
                        }

                        // If more requests were queued while this batch was running,
                        // schedule again now.
                        db.schedule_transactions(origin, &db_name);
                        db.commit_ready_transactions();
                    }
                },
                IndexedDBThreadMsg::CollectMemoryReport(sender) => {
                    let reports = self.collect_memory_reports();
                    sender.send(ProcessReports::new(reports));
                },
                IndexedDBThreadMsg::AsyncSchemaOperation {
                    origin,
                    database_name,
                    store_id,
                    operation,
                    transaction,
                } => {
                    let operation = KvsOperation {
                        store: store_id,
                        request_id: None,
                        operation: AsyncOperation::Schema(operation),
                    };
                    self.queue_operation(
                        origin,
                        database_name,
                        transaction,
                        IndexedDBTxnMode::Versionchange,
                        operation,
                    );
                },
            }
        }
    }

    /// Queues `operation` for `txn` and schedules it. An operation for a database or a
    /// transaction the backend doesn't know fails with DbNotFound, and one of an aborted
    /// transaction with Abort. An operation that the transaction's mode or scope doesn't allow
    /// fails and aborts the transaction.
    fn queue_operation(
        &mut self,
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
        mode: IndexedDBTxnMode,
        operation: KvsOperation,
    ) {
        let key = IndexedDBDescription {
            origin,
            name: db_name,
        };
        let check = self
            .databases
            .get(&key)
            .map_or(Err(BackendError::DbNotFound), |db| {
                db.check_operation(txn, &operation)
            });
        let error = match check {
            Ok(()) => {
                let db = self.databases.get_mut(&key).expect("Checked above");
                // Queues an operation for a transaction without starting it
                db.queue_operation(txn, mode, operation);
                db.schedule_transactions(key.origin, &key.name);
                return;
            },
            Err(error) => error,
        };
        self.transaction_results(&key, txn)
            .send(operation.failure_event(error.clone()));
        if let BackendError::DbErr(reason) = error &&
            let Some(db) = self.databases.get_mut(&key)
        {
            warn!("Aborting IndexedDB transaction {txn}: {reason}");
            db.abort(&key.origin, &key.name, txn);
        }
    }

    /// Where the results of `txn` go: the result channel it was registered with, or for a
    /// transaction the database doesn't know, the result channel of its connection, if the
    /// connection is open.
    fn transaction_results(
        &self,
        key: &IndexedDBDescription,
        txn: IndexedDBTxnId,
    ) -> TransactionResults {
        if let Some(db) = self.databases.get(key) &&
            db.knows_transaction(txn)
        {
            return db.results(txn);
        }
        let channel = self
            .connections
            .get(key)
            .and_then(|connections| connections.get(&txn.connection))
            .map(|connection| connection.results.clone());
        TransactionResults::new(txn, channel)
    }

    /// Handle when an upgrade transaction finishes in script.
    fn handle_upgrade_transaction_finished(
        &mut self,
        name: Utf16String,
        origin: ImmutableOrigin,
        txn: IndexedDBTxnId,
        committed: bool,
    ) {
        let key = IndexedDBDescription {
            name: name.clone(),
            origin: origin.clone(),
        };

        if committed {
            let Some(queue) = self.connection_queues.get_mut(&key) else {
                return debug_assert!(false, "A connection queue should exist.");
            };
            let Some(front) = queue.front() else {
                return debug_assert!(false, "A pending open request should exist.");
            };
            let OpenRequest::Open {
                pending_upgrade: Some(pending_upgrade),
                ..
            } = front
            else {
                return;
            };
            if pending_upgrade.transaction != txn {
                return;
            }

            let Some(open_request) = queue.pop_front() else {
                return;
            };
            let OpenRequest::Open {
                db_name,
                version: _,
                processed: _,
                pending_upgrade: Some(pending_upgrade),
                pending_close: _,
                pending_versionchange: _,
                id,
                proxy_map: _,
                results,
            } = open_request
            else {
                return;
            };
            let VersionUpgrade { new, .. } = pending_upgrade;
            let object_stores = self
                .databases
                .get(&key)
                .map(|db| db.object_stores())
                .unwrap_or_default();
            if results
                .send(ConnectionEvent::Open(ConnectionMsg::Connection {
                    id,
                    name: db_name,
                    version: new,
                    upgraded: true,
                    object_stores,
                }))
                .is_err()
            {
                error!("Failed to send ConnectionMsg::Connection to script.");
            };

            self.advance_connection_queue(key);
            return;
        }

        let (request_id, proxy_map) = {
            let Some(queue) = self.connection_queues.get_mut(&key) else {
                return debug_assert!(false, "A connection queue should exist.");
            };
            let Some(front) = queue.front() else {
                return debug_assert!(false, "A pending open request should exist.");
            };
            let OpenRequest::Open {
                pending_upgrade: Some(pending_upgrade),
                id,
                proxy_map,
                ..
            } = front
            else {
                return;
            };
            if pending_upgrade.transaction != txn {
                return;
            }
            (*id, (*proxy_map).clone())
        };

        self.abort_pending_upgrade(name, request_id, origin, &proxy_map);
    }

    /// Run the next open request in the queue.
    fn advance_connection_queue(&mut self, key: IndexedDBDescription) {
        loop {
            let is_open = {
                let Some(queue) = self.connection_queues.get_mut(&key) else {
                    return;
                };
                if queue.is_empty() {
                    return;
                }
                queue.front().expect("Queue is not empty.").is_open()
            };

            if is_open {
                self.open_database(key.clone());
            } else {
                self.delete_database(key.clone());
            }

            let was_pruned = self.maybe_remove_front_from_queue(&key);

            if !was_pruned {
                // Note: requests to delete a database are, at this point in the implementation,
                // done in one step; so we can continue on to the next request.
                // Request to open a connection consists of multiple async steps, so we must break if
                // it is still pending.
                break;
            }
        }
    }

    /// Remove the record at the front if it is not pending.
    fn maybe_remove_front_from_queue(&mut self, key: &IndexedDBDescription) -> bool {
        let (is_empty, was_pruned) = {
            let Some(queue) = self.connection_queues.get_mut(key) else {
                debug_assert!(false, "A connection queue should exist.");
                return false;
            };
            let mut pruned = false;
            let front_is_pending = queue.front().map(|record| record.is_pending());
            if let Some(is_pending) = front_is_pending &&
                !is_pending
            {
                queue.pop_front().expect("Queue has a non-pending item.");
                pruned = true
            }
            (queue.is_empty(), pruned)
        };
        if is_empty {
            self.connection_queues.remove(key);
        }
        was_pruned
    }

    fn remove_connection(&mut self, key: &IndexedDBDescription, id: &Uuid) {
        let is_empty = {
            let Some(connections) = self.connections.get_mut(key) else {
                return debug!("Connection already removed.");
            };
            connections.remove(id);
            connections.is_empty()
        };

        if is_empty {
            self.connections.remove(key);
        }
    }

    /// Revert the backing database state after aborting an upgrade transaction.
    ///
    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>
    /// IndexedDB §5.8 step 3 restores the previous version, or `0` if the database
    /// was newly created. Step 4 restores the previous object store set, or the
    /// empty set if the database was newly created. Servo eagerly creates the
    /// backing database with version `0` and no stores during open, so aborting
    /// that first upgrade must roll back to the pre-creation state by deleting the
    /// placeholder backing store entirely. Otherwise rolling back the upgrade
    /// transaction restores both; it has been rolled back already unless the
    /// upgrade is aborted without its transaction, as when a global goes away.
    ///
    /// Related: <https://github.com/servo/servo/pull/42998>
    fn revert_aborted_upgrade(
        &mut self,
        key: &IndexedDBDescription,
        upgrade: &VersionUpgrade,
        proxy_map: &StorageProxyMap,
    ) {
        if upgrade.old == 0 {
            if let Some(db) = self.databases.remove(key) {
                // Note: ensure db is dropped before deleting directory,
                // to get around windows file locks.
                drop(db);
                let response = proxy_map
                    .handle
                    .delete_database(proxy_map.bottle_id, key.name.clone())
                    .recv();
                if response.is_err() {
                    error!("Failed to communicate with client storage.");
                    return;
                }
                if response.unwrap().is_err() {
                    error!("Failed to delete database {:?}", key.name);
                }
            }
            return;
        }

        let Some(db) = self.databases.get_mut(key) else {
            return debug_assert!(false, "Db should have been created");
        };
        db.finish_transaction(upgrade.transaction);
        db.engine.rollback(upgrade.transaction, Box::new(|| {}));
    }

    /// Aborting the current upgrade for an origin.
    /// <https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction>
    fn abort_pending_upgrade(
        &mut self,
        name: Utf16String,
        id: Uuid,
        origin: ImmutableOrigin,
        proxy_map: &StorageProxyMap,
    ) {
        let key = IndexedDBDescription { name, origin };
        let upgrade = {
            let Some(queue) = self.connection_queues.get_mut(&key) else {
                return debug_assert!(
                    false,
                    "There should be a connection queue for the aborted upgrade."
                );
            };
            let Some(open_request) = queue.pop_front() else {
                return debug_assert!(false, "There should be an open request to upgrade.");
            };
            if open_request.get_id() != id {
                return debug_assert!(
                    false,
                    "Open request to abort should be at the head of the queue."
                );
            }
            open_request.abort()
        };
        if let Some(upgrade) = upgrade {
            self.revert_aborted_upgrade(&key, &upgrade, proxy_map);
        }

        self.remove_connection(&key, &id);

        self.advance_connection_queue(key);
    }

    /// Aborting all upgrades for an origin
    // https://w3c.github.io/IndexedDB/#abort-an-upgrade-transaction
    /// Note: this only reverts the version at this point.
    fn abort_pending_upgrades(
        &mut self,
        pending_upgrades: HashMap<Utf16String, HashSet<Uuid>>,
        origin: ImmutableOrigin,
    ) {
        for (name, ids) in pending_upgrades.into_iter() {
            let mut upgrade_to_revert: Option<(VersionUpgrade, StorageProxyMap)> = None;
            let key = IndexedDBDescription {
                name: name.clone(),
                origin: origin.clone(),
            };
            for id in ids.iter() {
                self.remove_connection(&key, id);
            }
            {
                let is_empty = {
                    let Some(queue) = self.connection_queues.get_mut(&key) else {
                        continue;
                    };
                    queue.retain_mut(|open_request| {
                        if ids.contains(&open_request.get_id()) {
                            let upgrade = open_request.abort();
                            if upgrade_to_revert.is_none() &&
                                let Some(upgrade) = upgrade
                            {
                                upgrade_to_revert =
                                    Some((upgrade, open_request.proxy_map().clone()));
                            }
                            false
                        } else {
                            true
                        }
                    });
                    queue.is_empty()
                };
                if is_empty {
                    self.connection_queues.remove(&key);
                }
            }
            if let Some((upgrade, proxy_map)) = upgrade_to_revert {
                self.revert_aborted_upgrade(&key, &upgrade, &proxy_map);
            }
        }
    }

    /// <https://w3c.github.io/IndexedDB/#open-a-database-connection>
    fn open_a_database_connection(
        &mut self,
        origin: ImmutableOrigin,
        db_name: Utf16String,
        version: Option<u64>,
        id: Uuid,
        bottle_source: StorageBottleSource,
        results: GenericCallback<ConnectionEvent>,
    ) {
        let proxy_map = match obtain_a_local_storage_bottle_map(&bottle_source, &origin) {
            Ok(proxy_map) => proxy_map,
            Err(error) => {
                if results
                    .send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                        name: db_name,
                        id,
                        error,
                    }))
                    .is_err()
                {
                    debug!("Script exit during indexeddb database open");
                }
                return;
            },
        };
        let key = IndexedDBDescription {
            name: db_name.clone(),
            origin,
        };
        let open_request = OpenRequest::Open {
            db_name,
            version,
            processed: false,
            pending_close: Default::default(),
            pending_versionchange: Default::default(),
            pending_upgrade: None,
            id,
            proxy_map,
            results,
        };
        let should_continue = {
            // Step 1: Let queue be the connection queue for storageKey and name.
            let queue = self.connection_queues.entry(key.clone()).or_default();

            // Step 2: Add request to queue.
            queue.push_back(open_request);
            queue.len() == 1
        };

        // Step 3: Wait until all previous requests in queue have been processed.
        if should_continue {
            self.open_database(key.clone());
            self.maybe_remove_front_from_queue(&key);
        }
    }

    fn get_database_mut(
        &mut self,
        origin: ImmutableOrigin,
        db_name: Utf16String,
    ) -> Option<&mut IndexedDBEnvironment<ActiveKvsEngine>> {
        let idb_description = IndexedDBDescription {
            origin,
            name: db_name,
        };

        self.databases.get_mut(&idb_description)
    }

    /// <https://w3c.github.io/IndexedDB/#upgrade-a-database>
    /// To upgrade a database with connection (a connection),
    /// a new version, and a request, run these steps:
    fn upgrade_database(&mut self, key: IndexedDBDescription, new_version: u64) {
        let Some(queue) = self.connection_queues.get_mut(&key) else {
            return debug_assert!(false, "A connection queue should exist.");
        };
        let Some(open_request) = queue.front_mut() else {
            return debug_assert!(false, "An open request should be in the queue.");
        };
        let OpenRequest::Open {
            db_name,
            version: _,
            processed,
            id,
            pending_close: _,
            pending_versionchange: _,
            pending_upgrade,
            proxy_map: _,
            results,
        } = open_request
        else {
            return;
        };

        // Step 1: Let db be connection’s database.
        let db = self
            .databases
            .get_mut(&key)
            .expect("Db should have been opened.");

        // Step 2: Let transaction be a new upgrade transaction with connection used as connection.
        let transaction = IndexedDBTxnId::upgrade(*id);

        // Step 3: Set transaction’s scope to connection’s object store set.
        let object_stores = db.object_stores();
        let scope = object_stores.iter().map(|store| store.id).collect();

        // Step 4: Set db’s upgrade transaction to transaction.
        // Backend tracks the active upgrade transaction in `pending_upgrade` below.
        db.register_transaction(
            transaction,
            IndexedDBTxnMode::Versionchange,
            scope,
            results.clone(),
        );

        // Step 5: Set transaction’s state to inactive.
        // Step 6: Start transaction.
        // Backend transactions are started by the scheduler when requests are queued;
        // newly created upgrade transactions are therefore initially inactive.

        // Step 7: Let old version be db’s version.
        let old_version = db.version().expect("DB should have a version.");

        // Step 8: Set db’s version to version. This change is considered part of the
        // transaction, and so if the transaction is aborted, this change is reverted.
        db.engine.upgrade_version(transaction, new_version);

        // Step 9: Set request’s processed flag to true.
        *processed = true;
        let _ = pending_upgrade.insert(VersionUpgrade {
            old: old_version,
            new: new_version,
            transaction,
        });

        // Step 10: Queue a database task to run these steps.
        if results
            .send(ConnectionEvent::Open(ConnectionMsg::Upgrade {
                id: *id,
                name: db_name.clone(),
                version: new_version,
                old_version,
                transaction,
                object_stores,
            }))
            .is_err()
        {
            error!("Couldn't queue task for indexeddb upgrade event.");
        }

        // Step 11: Wait for transaction to finish.
        // Queue progression remains blocked while `pending_upgrade` is set.
    }

    /// <https://w3c.github.io/IndexedDB/#open-a-database-connection>
    fn handle_version_change_done(
        &mut self,
        name: Utf16String,
        from_id: Uuid,
        old_version: u64,
        origin: ImmutableOrigin,
    ) {
        let key = IndexedDBDescription {
            name: name.clone(),
            origin,
        };
        let (can_upgrade, version) = {
            let Some(queue) = self.connection_queues.get_mut(&key) else {
                return debug_assert!(false, "A connection queue should exist.");
            };
            let Some(open_request) = queue.front_mut() else {
                return debug_assert!(false, "An open request should be in the queue.");
            };
            let OpenRequest::Open {
                db_name: _,
                version,
                id,
                pending_upgrade: _,
                processed: _,
                pending_versionchange,
                pending_close,
                proxy_map: _,
                results,
            } = open_request
            else {
                return debug_assert!(
                    false,
                    "An request to open a connection should be in the queue."
                );
            };
            // Note: a connection that closed first no longer counts, see `close_database`.
            if !pending_versionchange.remove(&from_id) {
                return;
            }

            // Step 10.3: Wait for all of the events to be fired.
            if !pending_versionchange.is_empty() {
                return;
            }

            let Some(version) = *version else {
                return debug_assert!(
                    false,
                    "An upgrade version should have been determined by now."
                );
            };

            // Step 10.4: If any of the connections in openConnections are still not closed,
            // queue a database task to fire a version change event named blocked
            // at request with db’s version and version.
            if !pending_close.is_empty() &&
                results
                    .send(ConnectionEvent::Open(ConnectionMsg::Blocked {
                        name,
                        id: *id,
                        version,
                        old_version,
                    }))
                    .is_err()
            {
                return debug!("Script exit during indexeddb database open");
            }

            (pending_close.is_empty(), version)
        };

        // Step 10.5: Wait until all connections in openConnections are closed.
        // Note: if we still need to wait, the algorithm will continue in the handling of the close message.
        if can_upgrade {
            // Step 10.6: Run upgrade a database using connection, version and request.
            self.upgrade_database(key.clone(), version);

            let was_pruned = self.maybe_remove_front_from_queue(&key);
            if was_pruned {
                self.advance_connection_queue(key);
            }
        }
    }

    /// <https://w3c.github.io/IndexedDB/#open-a-database-connection>
    /// The part where the open request is ready for processing.
    fn open_database(&mut self, key: IndexedDBDescription) {
        let Some(queue) = self.connection_queues.get_mut(&key) else {
            return debug_assert!(false, "A connection queue should exist.");
        };
        let Some(open_request) = queue.front_mut() else {
            return debug_assert!(false, "An open request should be in the queue.");
        };
        let OpenRequest::Open {
            db_name,
            version,
            id,
            processed,
            pending_upgrade: _pending_upgrade,
            pending_close,
            pending_versionchange,
            proxy_map,
            results,
        } = open_request
        else {
            return debug_assert!(
                false,
                "An request to open a connection should be in the queue."
            );
        };

        let requested_version = *version;

        // Step 4: Let db be the database named name in origin, or null otherwise.
        let db_version = match self.databases.entry(key.clone()) {
            Entry::Vacant(e) => {
                // Step 5: If version is undefined, let version be 1 if db is null, or db’s version otherwise.
                // Note: done below with the zero as first tuple item.

                // https://www.w3.org/TR/IndexedDB/#open-a-database-connection
                // Step 6: If db is null, let db be a new database
                // with name name, version 0 (zero), and with no object stores.
                // If this fails for any reason, return an appropriate error
                // (e.g. a "QuotaExceededError" or "UnknownError" DOMException).
                let Ok(response) = proxy_map
                    .handle
                    .create_database(proxy_map.bottle_id, db_name.clone())
                    .recv()
                else {
                    if let Err(e) =
                        results.send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                            id: *id,
                            name: db_name.clone(),
                            error: BackendError::DbErr(
                                "Failed to communicate with client storage.".to_string(),
                            ),
                        }))
                    {
                        debug!("Script exit during indexeddb database open {:?}", e);
                    }
                    return;
                };
                let (path, created) = match response {
                    Ok((path, created)) => (path, created),
                    Err(err) => {
                        if let Err(e) =
                            results.send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                                id: *id,
                                name: db_name.clone(),
                                error: BackendError::DbErr(format!("{err:?}")),
                            }))
                        {
                            debug!("Script exit during indexeddb database open {:?}", e);
                        }
                        return;
                    },
                };
                let engine =
                    match ActiveKvsEngine::new(path, created, &key, self.thread_pool.clone()) {
                        Ok(engine) => engine,
                        Err(err) => {
                            let error = backend_error_from_storage_error(err);
                            if let Err(e) =
                                results.send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                                    id: *id,
                                    name: db_name.clone(),
                                    error,
                                }))
                            {
                                debug!("Script exit during indexeddb database open {:?}", e);
                            }
                            *processed = true;
                            return;
                        },
                    };
                let created_db_path = engine.created_db_path();
                let db = IndexedDBEnvironment::new(engine, self.manager_sender.clone());
                let db_version = match db.version() {
                    Ok(version) => version,
                    Err(err) => {
                        let error = backend_error_from_storage_error(err);
                        if let Err(e) =
                            results.send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                                id: *id,
                                name: db_name.clone(),
                                error,
                            }))
                        {
                            debug!("Script exit during indexeddb database open {:?}", e);
                        }
                        *processed = true;
                        return;
                    },
                };

                *version = if created_db_path {
                    Some(requested_version.unwrap_or(1))
                } else {
                    Some(requested_version.unwrap_or(db_version))
                };

                e.insert(db);
                db_version
            },
            Entry::Occupied(db) => {
                let db_version = match db.get().version() {
                    Ok(version) => version,
                    Err(err) => {
                        let error = backend_error_from_storage_error(err);
                        if let Err(e) =
                            results.send(ConnectionEvent::Open(ConnectionMsg::DatabaseError {
                                id: *id,
                                name: db_name.clone(),
                                error,
                            }))
                        {
                            debug!("Script exit during indexeddb database open {:?}", e);
                        }
                        *processed = true;
                        return;
                    },
                };
                // Step 5: If version is undefined, let version be 1 if db is null, or db’s version otherwise.
                *version = Some(requested_version.unwrap_or(db_version));
                db_version
            },
        };

        let Some(version) = *version else {
            return debug_assert!(
                false,
                "An upgrade version should have been determined by now."
            );
        };

        // Step 7: If db’s version is greater than version,
        // return a newly created "VersionError" DOMException
        // and abort these steps.
        if version < db_version {
            if results
                .send(ConnectionEvent::Open(ConnectionMsg::VersionError {
                    name: db_name.clone(),
                    id: *id,
                }))
                .is_err()
            {
                debug!("Script exit during indexeddb database open");
            }
            *processed = true;
            return;
        }

        // Step 8: Let connection be a new connection to db.
        // Step 9: Set connection’s version to version.
        let connection = Connection {
            close_pending: false,
            results: results.clone(),
        };
        let entry = self.connections.entry(key.clone()).or_default();
        entry.insert(*id, connection);

        // Step 10: If db’s version is less than version, then:
        if db_version < version {
            // Step 10.1: Let openConnections be the set of all connections,
            // except connection, associated with db.
            let open_connections = entry
                .iter_mut()
                .filter(|(other_id, conn)| !conn.close_pending && *other_id != id);
            for (id_to_close, conn) in open_connections {
                // Step 10.2: For each entry of openConnections
                // queue a database task to fire a version change event
                // named versionchange at entry with db’s version and version.
                if conn
                    .results
                    .send(ConnectionEvent::VersionChange {
                        old_version: db_version,
                        new_version: version,
                    })
                    .is_err()
                {
                    error!("Failed to send ConnectionEvent::VersionChange to script.");
                };
                pending_close.insert(*id_to_close);
                pending_versionchange.insert(*id_to_close);
            }
            if !pending_close.is_empty() {
                // Step 10.3: Wait for all of the events to be fired.
                return;
            }

            // Step 10.6: Run upgrade a database using connection, version and request.
            self.upgrade_database(key, version);
            return;
        }

        // Step 11:
        let object_stores = self
            .databases
            .get(&key)
            .map(|db| db.object_stores())
            .unwrap_or_default();
        *processed = true;
        if results
            .send(ConnectionEvent::Open(ConnectionMsg::Connection {
                name: db_name.clone(),
                id: *id,
                version: db_version,
                upgraded: false,
                object_stores,
            }))
            .is_err()
        {
            error!("Failed to send ConnectionMsg::Connection to script.");
        };
    }

    /// <https://www.w3.org/TR/IndexedDB/#delete-a-database>
    /// The part adding the request to the connection queue.
    fn start_delete_database(
        &mut self,
        key: IndexedDBDescription,
        id: Uuid,
        proxy_map: StorageProxyMap,
        sender: GenericCallback<BackendResult<u64>>,
    ) {
        let open_request = OpenRequest::Delete {
            sender,
            _origin: key.origin.clone(),
            db_name: key.name.clone(),
            processed: false,
            proxy_map,
            id,
        };

        let should_continue = {
            // Step 1: Let queue be the connection queue for storageKey and name.
            let queue = self.connection_queues.entry(key.clone()).or_default();

            // Step 2: Add request to queue.
            queue.push_back(open_request);
            queue.len() == 1
        };

        // Step 3: Wait until all previous requests in queue have been processed.
        if should_continue {
            self.delete_database(key.clone());
            self.maybe_remove_front_from_queue(&key);
        }
    }

    /// <https://www.w3.org/TR/IndexedDB/#delete-a-database>
    fn delete_database(&mut self, key: IndexedDBDescription) {
        let Some(queue) = self.connection_queues.get_mut(&key) else {
            return debug_assert!(false, "A connection queue should exist.");
        };
        let Some(open_request) = queue.front_mut() else {
            return debug_assert!(false, "An open request should be in the queue.");
        };
        let OpenRequest::Delete {
            sender,
            _origin: _,
            db_name,
            processed,
            id: _,
            proxy_map,
        } = open_request
        else {
            return debug_assert!(
                false,
                "An request to open a connection should be in the queue."
            );
        };

        // Step 4: Let db be the database named name in storageKey, if one exists. Otherwise, return 0 (zero).
        let version = if let Some(db) = self.databases.remove(&key) {
            // Step 5: Let openConnections be the set of all connections associated with db.
            // Step6: For each entry of openConnections that does not have its close pending flag set to true,
            // queue a database task to fire a version change event named versionchange
            // at entry with db’s version and null.
            // Step 7: Wait for all of the events to be fired.
            // Step 8: If any of the connections in openConnections are still not closed,
            // queue a database task to fire a version change event
            // named blocked at request with db’s version and null.
            // Step 9: Wait until all connections in openConnections are closed.
            // TODO: implement connections.

            // Step 10: Let version be db’s version.
            let res = db.version();
            let Ok(version) = res else {
                *processed = true;
                if sender
                    .send(BackendResult::Err(BackendError::DbErr(
                        res.unwrap_err().to_string(),
                    )))
                    .is_err()
                {
                    debug!("Script went away during pending database delete.");
                }
                return;
            };

            // Note: ensure db is dropped before deleting directory,
            // to get around windows file locks.
            drop(db);

            // Step 11: Delete db.
            // If this fails for any reason,
            // return an appropriate error (e.g. a QuotaExceededError, or an "UnknownError" DOMException).
            let Ok(response) = proxy_map
                .handle
                .delete_database(proxy_map.bottle_id, db_name.clone())
                .recv()
            else {
                if sender
                    .send(BackendResult::Err(BackendError::DbErr(
                        "Failed to communicate with client storage.".to_string(),
                    )))
                    .is_err()
                {
                    debug!("Script went away during pending database delete.");
                }
                return;
            };
            if let Err(err) = response {
                if sender
                    .send(BackendResult::Err(BackendError::DbErr(format!(
                        "Client storage error: {err:?}"
                    ))))
                    .is_err()
                {
                    debug!("Script went away during pending database delete.");
                }
                return;
            }
            version
        } else {
            0
        };

        // step 12: Return version.
        if sender.send(BackendResult::Ok(version)).is_err() {
            debug!("Script went away during pending database delete.");
        }

        *processed = true;
    }

    /// <https://w3c.github.io/IndexedDB/#closing-connection>
    fn close_database(&mut self, origin: ImmutableOrigin, id: Uuid, name: Utf16String) {
        // Step 1: Set connection’s close pending flag to true.
        // TODO: seems like a script only flag.

        // Step 2: If the forced flag is true,
        // then for each transaction created using connection
        // run abort a transaction with transaction and newly created "AbortError" DOMException.
        // Step 3: Wait for all transactions created using connection to complete.
        // Once they are complete, connection is closed.
        // TODO: transaction lifecycle.

        // Step 4: If the forced flag is true, then fire an event named close at connection.
        // TODO: implement, probably only on the script side of things.

        // Note: below we are continuing
        // <https://w3c.github.io/IndexedDB/#open-a-database-connection>
        // in the case that an open request is waiting for connections to close.
        let key = IndexedDBDescription { origin, name };
        let (can_upgrade, version) = {
            self.remove_connection(&key, &id);

            let Some(queue) = self.connection_queues.get_mut(&key) else {
                return;
            };
            let Some(open_request) = queue.front_mut() else {
                return;
            };
            if let OpenRequest::Open {
                db_name: _,
                version,
                id: request_id,
                processed: _,
                pending_upgrade,
                pending_versionchange,
                pending_close,
                proxy_map: _,
                results,
            } = open_request
            {
                pending_close.remove(&id);
                // A closed connection fires no versionchange event, so the request stops waiting
                // for it: the connection's global may be gone and never report the event.
                if pending_versionchange.remove(&id) &&
                    pending_versionchange.is_empty() &&
                    !pending_close.is_empty() &&
                    let Some(version) = *version
                {
                    // Step 10.4: If any of the connections in openConnections are still not
                    // closed, queue a database task to fire a version change event named blocked
                    // at request with db’s version and version.
                    let old_version = self
                        .databases
                        .get(&key)
                        .and_then(|db| db.version().ok())
                        .unwrap_or_default();
                    if results
                        .send(ConnectionEvent::Open(ConnectionMsg::Blocked {
                            name: key.name.clone(),
                            id: *request_id,
                            version,
                            old_version,
                        }))
                        .is_err()
                    {
                        debug!("Script exit during indexeddb database open");
                    }
                }
                (
                    // Note: need to exclude requests that have already started upgrading.
                    pending_close.is_empty() &&
                        pending_versionchange.is_empty() &&
                        !pending_upgrade.is_some(),
                    *version,
                )
            } else {
                (false, None)
            }
        };

        // <https://w3c.github.io/IndexedDB/#open-a-database-connection>
        // Step 10.3: Wait for all of the events to be fired.
        // Step 10.5: Wait until all connections in openConnections are closed.
        // Note: both conditions must be checked here,
        // because that is the condition enabling the upgrade to proceed.
        if can_upgrade {
            // Step 10.6: Run upgrade a database using connection, version and request.
            let Some(version) = version else {
                return debug_assert!(
                    false,
                    "An upgrade version should have been determined by now."
                );
            };
            self.upgrade_database(key.clone(), version);

            let was_pruned = self.maybe_remove_front_from_queue(&key);
            if was_pruned {
                self.advance_connection_queue(key);
            }
        }
    }

    fn handle_sync_operation(&mut self, operation: SyncOperation) {
        match operation {
            SyncOperation::GetDatabases(sender, origin) => {
                // The in-parallel steps of https://www.w3.org/TR/IndexedDB/#dom-idbfactory-databases

                // Step 4.1 Let databases be the set of databases in storageKey.
                // If this cannot be determined for any reason,
                // then queue a database task to reject p with an appropriate error
                // (e.g. an "UnknownError" DOMException) and terminate these steps.
                // TODO: separate database and connection concepts.
                // For now using `self.databases`, which track connections.

                // Step 4.2: Let result be a new list.
                let info_list: Vec<DatabaseInfo> = self
                    .databases
                    .iter()
                    .filter(|(description, _)| description.origin == origin)
                    .filter_map(|(description, info)| {
                        // Step 4.3: For each db of databases:
                        if let Ok(version) = info.version() {
                            // Step 4.3.4: If db’s version is 0, then continue.
                            if version == 0 {
                                None
                            } else {
                                // Step 4.3.5: Let info be a new IDBDatabaseInfo dictionary.
                                // Step 4.3.6: Set info’s name dictionary member to db’s name.
                                // Step 4.3.7: Set info’s version dictionary member to db’s version.
                                // Step 4.3.8: Append info to result.
                                Some(DatabaseInfo {
                                    name: description.name.clone(),
                                    version,
                                })
                            }
                        } else {
                            None
                        }
                    })
                    .collect();

                // IndexedDB `databases()` / "get a list of databases" returns the visible list;
                // filtering out non-user-visible entries must not turn success into an internal error.
                // https://w3c.github.io/IndexedDB/#dom-idbfactory-databases
                let result = Ok(info_list);

                // Step 4.4: Queue a database task to resolve p with result.
                if sender.send(result).is_err() {
                    debug!("Couldn't send SyncOperation::GetDatabases reply.");
                }
            },
            SyncOperation::CloseDatabase(origin, id, db_name) => {
                self.close_database(origin, id, db_name);
            },
            SyncOperation::OpenDatabase(origin, db_name, version, id, bottle_source, results) => {
                self.open_a_database_connection(
                    origin,
                    db_name,
                    version,
                    id,
                    bottle_source,
                    results,
                );
            },
            SyncOperation::AbortPendingUpgrades {
                pending_upgrades,
                origin,
            } => {
                self.abort_pending_upgrades(pending_upgrades, origin);
            },
            SyncOperation::DeleteDatabase(callback, origin, db_name, bottle_source, id) => {
                let proxy_map = match obtain_a_local_storage_bottle_map(&bottle_source, &origin) {
                    Ok(proxy_map) => proxy_map,
                    Err(error) => {
                        if callback.send(Err(error)).is_err() {
                            debug!("Script went away during pending database delete.");
                        }
                        return;
                    },
                };
                let idb_description = IndexedDBDescription {
                    origin,
                    name: db_name,
                };
                self.start_delete_database(idb_description, id, proxy_map, callback);
            },
            SyncOperation::Commit {
                origin,
                db_name,
                txn,
            } => {
                // https://w3c.github.io/IndexedDB/#commit-a-transaction
                // The reply follows the engine's COMMIT, or its failure.
                let key = IndexedDBDescription {
                    origin: origin.clone(),
                    name: db_name.clone(),
                };
                match self
                    .databases
                    .get(&key)
                    .map(|db| db.check_usable(txn).map(|_| ()))
                {
                    Some(Ok(())) => {
                        let db = self.databases.get_mut(&key).expect("Checked above");
                        if db.can_commit_now(txn) {
                            db.commit(txn);
                        } else {
                            db.pending_commits.insert(txn);
                        }
                        db.schedule_transactions(origin, &db_name);
                    },
                    Some(Err(error)) => {
                        debug!("Commit of IndexedDB transaction {txn} failed: {error}");
                        self.transaction_results(&key, txn)
                            .send(TransactionEvent::Committed(Err(error)));
                    },
                    None => {
                        // If the database entry has already been removed, treat commit as a
                        // no-op success so script side completion does not spuriously abort.
                        self.transaction_results(&key, txn)
                            .send(TransactionEvent::Committed(Ok(())));
                    },
                }
            },
            SyncOperation::Abort {
                origin,
                db_name,
                txn,
            } => {
                // Done at once, also while a batch of the transaction runs: the engine runs the
                // rollback after it, and before any batch the scheduler hands over later.
                if let Some(db) = self.get_database_mut(origin.clone(), db_name.clone()) {
                    db.abort(&origin, &db_name, txn);
                }
            },
            SyncOperation::UpgradeTransactionFinished {
                origin,
                db_name,
                txn,
                committed,
            } => {
                self.handle_upgrade_transaction_finished(db_name, origin, txn, committed);
            },
            SyncOperation::TransactionFinished {
                origin,
                db_name,
                txn,
            } => {
                if let Some(db) = self.get_database_mut(origin.clone(), db_name.clone()) {
                    db.finish_transaction(txn);
                    // A no-op after a commit or an abort, but a transaction that finished
                    // otherwise, such as one whose global went away, must not leave its SQL
                    // transaction open.
                    db.engine.rollback(txn, Box::new(|| {}));
                    db.schedule_transactions(origin, &db_name);
                    db.commit_ready_transactions();
                }
            },
            SyncOperation::CreateTransaction {
                origin,
                db_name,
                txn,
                mode,
                scope,
            } => {
                // https://w3c.github.io/IndexedDB/#transaction-scheduling
                // A transaction counts as created when this message arrives. Script sends it
                // before transaction() returns, and every global's messages reach this thread
                // through the one channel, so of two transactions whose creations are ordered,
                // in one global or across globals, the earlier one is registered first.
                let key = IndexedDBDescription {
                    origin: origin.clone(),
                    name: db_name.clone(),
                };
                // A transaction of a connection that isn't open isn't registered, so that it
                // holds back no other transaction; its requests and its commit fail, and with no
                // channel to report on, the failures are dropped. Script doesn't create such
                // transactions: a connection is closed before the backend drops it.
                let Some(results) = self
                    .connections
                    .get(&key)
                    .and_then(|connections| connections.get(&txn.connection))
                    .map(|connection| connection.results.clone())
                else {
                    return warn!("Transaction {txn} created for a connection that isn't open.");
                };
                if let Some(db) = self.databases.get_mut(&key) {
                    db.register_transaction(txn, mode, scope, results);
                    db.schedule_transactions(origin, &db_name);
                } else {
                    // Its requests fail with DbNotFound, see `IndexedDBThreadMsg::Async`.
                    debug!("Transaction {txn} created for a database that is gone.");
                }
            },
            SyncOperation::NotifyEndOfVersionChange {
                name,
                id,
                old_version,
                origin,
            } => {
                self.handle_version_change_done(name, id, old_version, origin);
            },
            SyncOperation::Exit(_) => {
                unreachable!("We must've already broken out of event loop.");
            },
        }
    }

    fn collect_memory_reports(&self) -> Vec<Report> {
        let mut reports = vec![];
        perform_memory_report(|ops| {
            reports.push(Report {
                path: path!["indexeddb"],
                kind: ReportKind::ExplicitJemallocHeapSize,
                size: self.connections.size_of(ops) +
                    self.databases.size_of(ops) +
                    self.connection_queues.size_of(ops),
            });
        });
        reports
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::time::Duration;

    use profile_traits::generic_callback::GenericCallback;
    use servo_base::generic_channel::{self, GenericReceiver};
    use servo_base::threadpool::ThreadPool;
    use servo_url::ImmutableOrigin;
    use storage_traits::indexeddb::{
        AsyncOperation, AsyncReadWriteOperation, AsyncSchemaOperation, BackendError, BackendResult,
        ConnectionEvent, IndexedDBThreadMsg, IndexedDBTxnMode, KeyPath, TransactionEvent,
    };
    use url::Host;

    use super::{IndexedDBDescription, IndexedDBEnvironment};
    use crate::indexeddb::backend::SqlEngine;
    use crate::indexeddb::backend::executor::SqlExecutor;
    use crate::indexeddb::backend::tests::{
        Faulty, executor_tests, faults_for, stored_version_and_store_names, txn,
    };
    use crate::indexeddb::engines::{KvsEngine, KvsOperation};

    executor_tests!(
        test_aborted_upgrade_rolls_back_created_store_and_index,
        test_abort_transaction_restores_readwrite_key_generator_current_number,
        test_failed_commit_reports_the_error_and_rolls_back,
        test_only_a_started_writer_holds_back_other_writers,
        test_readonly_waits_for_an_earlier_overlapping_writer,
        test_readwrite_without_requests_commits,
    );

    fn test_origin() -> ImmutableOrigin {
        ImmutableOrigin::Tuple(
            "test_origin".to_string(),
            Host::Domain("localhost".to_string()),
            80,
        )
    }

    fn open_environment<X: SqlExecutor>(
        base_dir: &tempfile::TempDir,
    ) -> (
        IndexedDBEnvironment<SqlEngine<X>>,
        GenericReceiver<IndexedDBThreadMsg>,
    ) {
        let description = IndexedDBDescription {
            name: "test_db".into(),
            origin: test_origin(),
        };
        let engine = SqlEngine::<X>::new(
            base_dir.path().to_path_buf(),
            true,
            &description,
            ThreadPool::global(),
        )
        .unwrap();
        let (sender, receiver) = generic_channel::channel().unwrap();
        (IndexedDBEnvironment::new(engine, sender), receiver)
    }

    thread_local! {
        /// The receivers of the result channels of the test's transactions, by serial number.
        static RESULTS: RefCell<HashMap<u64, mpsc::Receiver<ConnectionEvent>>> =
            Default::default();
    }

    /// Registers the transaction `serial` with a result channel that the test reads.
    fn register<X: SqlExecutor>(
        env: &mut IndexedDBEnvironment<SqlEngine<X>>,
        serial: u64,
        mode: IndexedDBTxnMode,
        scope: &[i64],
    ) {
        let (sender, receiver) = mpsc::channel();
        let channel = GenericCallback::new(move |event| {
            let _ = sender.send(event.unwrap());
        })
        .unwrap();
        RESULTS.with(|results| results.borrow_mut().insert(serial, receiver));
        env.register_transaction(txn(serial), mode, scope.to_vec(), channel);
    }

    /// The next thing the backend reports about the transaction `serial`.
    fn next_event(serial: u64) -> TransactionEvent {
        let event = RESULTS.with(|results| {
            results.borrow()[&serial]
                .recv_timeout(Duration::from_secs(10))
                .expect("The transaction should report")
        });
        match event {
            ConnectionEvent::Transaction {
                serial: reported,
                event,
            } => {
                assert_eq!(reported, serial);
                event
            },
            other => panic!("unexpected connection event {other:?}"),
        }
    }

    /// Commits `serial` and returns the result the backend reports.
    fn commit_result<X: SqlExecutor>(
        env: &mut IndexedDBEnvironment<SqlEngine<X>>,
        serial: u64,
    ) -> BackendResult<()> {
        env.commit(txn(serial));
        loop {
            match next_event(serial) {
                TransactionEvent::Committed(result) => return result,
                TransactionEvent::Request { .. } => {},
                other => panic!("expected the commit result, got {other:?}"),
            }
        }
    }

    /// Queues `operation` for `txn`, runs it, and waits until it has run.
    fn run<X: SqlExecutor>(
        env: &mut IndexedDBEnvironment<SqlEngine<X>>,
        batches: &GenericReceiver<IndexedDBThreadMsg>,
        serial: u64,
        mode: IndexedDBTxnMode,
        store: i64,
        operation: AsyncOperation,
    ) {
        let request_id = match operation {
            AsyncOperation::Schema(_) => None,
            _ => Some(0),
        };
        let operation = KvsOperation {
            store,
            request_id,
            operation,
        };
        env.queue_operation(txn(serial), mode, operation);
        env.schedule_transactions(test_origin(), &"test_db".into());
        match batches.recv().unwrap() {
            IndexedDBThreadMsg::EngineTxnBatchComplete { txn: done, .. } => {
                assert_eq!(done, txn(serial));
                env.running_readonly.remove(&done);
                env.running_readwrite = None;
            },
            _ => panic!("expected the batch to complete"),
        }
    }

    /// Aborts `serial` and waits until the engine has rolled it back.
    fn abort<X: SqlExecutor>(env: &mut IndexedDBEnvironment<SqlEngine<X>>, serial: u64) {
        env.abort(&test_origin(), &"test_db".into(), txn(serial));
        let (sender, rolled_back) = mpsc::channel();
        // A second rollback is a no-op, which the engine runs after the first.
        env.engine.rollback(
            txn(serial),
            Box::new(move || {
                let _ = sender.send(());
            }),
        );
        rolled_back.recv_timeout(Duration::from_secs(10)).unwrap();
        env.finish_transaction(txn(serial));
    }

    fn commit<X: SqlExecutor>(env: &mut IndexedDBEnvironment<SqlEngine<X>>, serial: u64) {
        assert!(env.can_commit_now(txn(serial)));
        assert_eq!(commit_result(env, serial), Ok(()));
        env.finish_transaction(txn(serial));
    }

    fn create_store_operation(name: &str, auto_increment: bool) -> AsyncOperation {
        AsyncOperation::Schema(AsyncSchemaOperation::CreateObjectStore {
            name: name.into(),
            key_path: None,
            auto_increment,
        })
    }

    fn store_names<X: SqlExecutor>(env: &IndexedDBEnvironment<SqlEngine<X>>) -> Vec<String> {
        env.object_stores()
            .into_iter()
            .map(|store| store.name.to_string())
            .collect()
    }

    fn test_aborted_upgrade_rolls_back_created_store_and_index<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, batches) = open_environment::<X>(&base_dir);
        assert!(store_names(&env).is_empty());

        register(&mut env, 1, IndexedDBTxnMode::Versionchange, &[]);
        env.engine.upgrade_version(txn(1), 1);
        let mode = IndexedDBTxnMode::Versionchange;
        run(
            &mut env,
            &batches,
            1,
            mode.clone(),
            1,
            create_store_operation("not_books", false),
        );
        run(
            &mut env,
            &batches,
            1,
            mode,
            1,
            AsyncOperation::Schema(AsyncSchemaOperation::CreateIndex {
                index_id: 1,
                name: "not_by_author".into(),
                key_path: KeyPath::String("author".to_string()),
                unique: false,
                multi_entry: false,
            }),
        );
        assert_eq!(
            stored_version_and_store_names(&env.engine),
            (1, vec!["not_books".to_owned()])
        );
        assert!(store_names(&env).is_empty());

        abort(&mut env, 1);

        assert_eq!(stored_version_and_store_names(&env.engine), (0, vec![]));
        assert_eq!(store_names(&env), Vec::<String>::new());
        assert_eq!(env.version().unwrap(), 0);
    }

    /// The id of the store `books`.
    const BOOKS: i64 = 1;

    /// Creates a store `books` with a key generator in a committed upgrade transaction 1, and
    /// puts a record with a generated key in the readwrite transaction 2, which stays open.
    fn put_generated_key_in_new_store<X: SqlExecutor>(
        env: &mut IndexedDBEnvironment<SqlEngine<X>>,
        batches: &GenericReceiver<IndexedDBThreadMsg>,
    ) {
        register(env, 1, IndexedDBTxnMode::Versionchange, &[]);
        env.engine.upgrade_version(txn(1), 1);
        run(
            env,
            batches,
            1,
            IndexedDBTxnMode::Versionchange,
            BOOKS,
            create_store_operation("books", true),
        );
        commit(env, 1);
        assert_eq!(env.engine.key_generator_current_number(BOOKS), Some(1));

        register(env, 2, IndexedDBTxnMode::Readwrite, &[BOOKS]);
        run(
            env,
            batches,
            2,
            IndexedDBTxnMode::Readwrite,
            BOOKS,
            AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                key: None,
                value: vec![1],
                should_overwrite: false,
                index_keys: vec![],
            }),
        );
        assert_eq!(env.engine.key_generator_current_number(BOOKS), Some(2));
    }

    fn test_abort_transaction_restores_readwrite_key_generator_current_number<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, batches) = open_environment::<X>(&base_dir);
        put_generated_key_in_new_store(&mut env, &batches);

        abort(&mut env, 2);

        assert_eq!(env.engine.key_generator_current_number(BOOKS), Some(1));
    }

    fn test_failed_commit_reports_the_error_and_rolls_back<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, batches) = open_environment::<Faulty<X>>(&base_dir);
        put_generated_key_in_new_store(&mut env, &batches);

        faults_for(base_dir.path()).lock().unwrap().commit = true;
        let result = commit_result(&mut env, 2);
        faults_for(base_dir.path()).lock().unwrap().commit = false;

        assert!(matches!(result, Err(BackendError::DbErr(_))));
        assert_eq!(env.engine.key_generator_current_number(BOOKS), Some(1));
    }

    fn clear_operation() -> AsyncOperation {
        AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear)
    }

    /// Writers start one at a time, also with disjoint scopes, since each is the one SQL
    /// transaction of the connection; one that hasn't started yet holds back no one.
    fn test_only_a_started_writer_holds_back_other_writers<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, batches) = open_environment::<X>(&base_dir);
        register(&mut env, 1, IndexedDBTxnMode::Readwrite, &[1]);
        register(&mut env, 2, IndexedDBTxnMode::Readwrite, &[2]);
        assert!(env.can_start_by_spec(txn(1)));
        assert!(env.can_start_by_spec(txn(2)));

        run(
            &mut env,
            &batches,
            1,
            IndexedDBTxnMode::Readwrite,
            1,
            clear_operation(),
        );
        assert!(!env.can_start_by_spec(txn(2)));
        register(&mut env, 3, IndexedDBTxnMode::Readonly, &[3]);
        assert!(env.can_start_by_spec(txn(3)));

        commit(&mut env, 1);
        assert!(env.can_start_by_spec(txn(2)));
    }

    /// <https://w3c.github.io/IndexedDB/#transaction-scheduling>: a readonly transaction waits
    /// for every earlier readwrite transaction with an overlapping scope, also one that hasn't
    /// started.
    fn test_readonly_waits_for_an_earlier_overlapping_writer<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, _batches) = open_environment::<X>(&base_dir);
        register(&mut env, 1, IndexedDBTxnMode::Readwrite, &[1]);
        register(&mut env, 2, IndexedDBTxnMode::Readonly, &[1, 2]);
        register(&mut env, 3, IndexedDBTxnMode::Readonly, &[2]);
        assert!(!env.can_start_by_spec(txn(2)));
        assert!(env.can_start_by_spec(txn(3)));
        env.finish_transaction(txn(1));
        assert!(env.can_start_by_spec(txn(2)));
    }

    /// A readwrite transaction without requests has no SQL transaction, and commits.
    fn test_readwrite_without_requests_commits<X: SqlExecutor>() {
        let base_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (mut env, _batches) = open_environment::<X>(&base_dir);
        register(&mut env, 1, IndexedDBTxnMode::Readwrite, &[1]);
        commit(&mut env, 1);
        register(&mut env, 2, IndexedDBTxnMode::Readwrite, &[2]);
        assert!(env.can_start_by_spec(txn(2)));
    }

    #[derive(Debug)]
    struct DummyError {
        quota_exceeded: bool,
    }

    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DummyError(quota_exceeded={})", self.quota_exceeded)
        }
    }

    impl std::error::Error for DummyError {}

    impl super::BackendErrorMapper for DummyError {
        fn into_backend_error(self) -> storage_traits::indexeddb::BackendError {
            if self.quota_exceeded {
                storage_traits::indexeddb::BackendError::QuotaExceeded
            } else {
                storage_traits::indexeddb::BackendError::DbErr(format!("{self:?}"))
            }
        }
    }

    #[test]
    fn test_backend_error_from_storage_error_maps_quota_exceeded() {
        assert_eq!(
            super::backend_error_from_storage_error(DummyError {
                quota_exceeded: true,
            }),
            storage_traits::indexeddb::BackendError::QuotaExceeded
        );
    }

    #[test]
    fn test_backend_error_from_storage_error_preserves_other_errors() {
        match super::backend_error_from_storage_error(DummyError {
            quota_exceeded: false,
        }) {
            storage_traits::indexeddb::BackendError::DbErr(message) => {
                assert!(message.contains("DummyError"));
            },
            other => panic!("unexpected backend error: {other:?}"),
        }
    }
}
