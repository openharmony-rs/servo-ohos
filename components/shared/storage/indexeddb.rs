/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cmp::{Ordering, PartialEq, PartialOrd};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use malloc_size_of_derive::MallocSizeOf;
use profile_traits::generic_callback::GenericCallback;
use profile_traits::mem::ReportsChan;
use serde::{Deserialize, Serialize};
use servo_base::generic_channel::GenericSender;
use servo_base::id::WebViewId;
use servo_url::origin::ImmutableOrigin;
use uuid::Uuid;

use crate::client_storage::ClientStorageThreadHandle;

// TODO Box<dyn Error> is not serializable, fix needs to be found
pub type DbError = String;
/// A DbResult wraps any part of a call that has to reach into the backend (in this case sqlite.rs)
/// These errors could be anything, depending on the backend
pub type DbResult<T> = Result<T, DbError>;

/// Any error from the backend, a super-set of [`DbError`]
#[derive(Clone, Debug, Deserialize, Eq, MallocSizeOf, PartialEq, Serialize)]
pub enum BackendError {
    /// The requested database does not exist
    DbNotFound,
    /// The requested object store does not exist
    StoreNotFound,
    /// The storage quota was exceeded
    QuotaExceeded,
    /// The transaction was aborted
    Abort,

    DbErr(DbError),
}

impl From<DbError> for BackendError {
    fn from(value: DbError) -> Self {
        BackendError::DbErr(value)
    }
}

impl Display for BackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::DbNotFound => write!(f, "DbNotFound"),
            BackendError::StoreNotFound => write!(f, "StoreNotFound"),
            BackendError::QuotaExceeded => write!(f, "QuotaExceeded"),
            BackendError::Abort => write!(f, "Abort"),
            BackendError::DbErr(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BackendError {}

pub type BackendResult<T> = Result<T, BackendError>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, MallocSizeOf, Serialize)]
pub enum KeyPath {
    String(String),
    Sequence(Vec<String>),
}

/// A transaction: its connection, and a serial number that is unique among the transactions of
/// the connection. The backend numbers the upgrade transaction of an open request 0
/// ([`IndexedDBTxnId::upgrade`]); script numbers the transactions it creates from 1.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, MallocSizeOf, PartialEq, Serialize)]
pub struct IndexedDBTxnId {
    /// The id of the connection, which is also the id of the request that opened it.
    pub connection: Uuid,
    pub serial: u64,
}

impl IndexedDBTxnId {
    /// The upgrade transaction of the connection `connection`.
    pub fn upgrade(connection: Uuid) -> Self {
        Self {
            connection,
            serial: 0,
        }
    }
}

impl Display for IndexedDBTxnId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.connection, self.serial)
    }
}

// https://www.w3.org/TR/IndexedDB-3/#enumdef-idbtransactionmode
#[derive(Clone, Debug, Deserialize, Eq, MallocSizeOf, PartialEq, Serialize)]
pub enum IndexedDBTxnMode {
    Readonly,
    Readwrite,
    Versionchange,
}

/// <https://www.w3.org/TR/IndexedDB-3/#key-type>
#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum IndexedDBKeyType {
    Number(f64),
    String(String),
    Binary(Vec<u8>),
    Date(f64),
    Array(Vec<IndexedDBKeyType>),
    // FIXME:(arihant2math) implment ArrayBuffer
}

/// <https://www.w3.org/TR/IndexedDB-3/#compare-two-keys>
impl PartialOrd for IndexedDBKeyType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // 1. Let ta be the type of a.
        // 2. Let tb be the type of b.

        match (self, other) {
            // Step 3: If ta is array and tb is binary, string, date or number, return 1.
            (
                IndexedDBKeyType::Array(_),
                IndexedDBKeyType::Binary(_) |
                IndexedDBKeyType::Date(_) |
                IndexedDBKeyType::Number(_) |
                IndexedDBKeyType::String(_),
            ) => Some(Ordering::Greater),
            // Step 4: If tb is array and ta is binary, string, date or number, return -1.
            (
                IndexedDBKeyType::Binary(_) |
                IndexedDBKeyType::Date(_) |
                IndexedDBKeyType::Number(_) |
                IndexedDBKeyType::String(_),
                IndexedDBKeyType::Array(_),
            ) => Some(Ordering::Less),
            // Step 5: If ta is binary and tb is string, date or number, return 1.
            (
                IndexedDBKeyType::Binary(_),
                IndexedDBKeyType::String(_) |
                IndexedDBKeyType::Date(_) |
                IndexedDBKeyType::Number(_),
            ) => Some(Ordering::Greater),
            // Step 6: If tb is binary and ta is string, date or number, return -1.
            (
                IndexedDBKeyType::String(_) |
                IndexedDBKeyType::Date(_) |
                IndexedDBKeyType::Number(_),
                IndexedDBKeyType::Binary(_),
            ) => Some(Ordering::Less),
            // Step 7: If ta is string and tb is date or number, return 1.
            (
                IndexedDBKeyType::String(_),
                IndexedDBKeyType::Date(_) | IndexedDBKeyType::Number(_),
            ) => Some(Ordering::Greater),
            // Step 8: If tb is string and ta is date or number, return -1.
            (
                IndexedDBKeyType::Date(_) | IndexedDBKeyType::Number(_),
                IndexedDBKeyType::String(_),
            ) => Some(Ordering::Less),
            // Step 9: If ta is date and tb is number, return 1.
            (IndexedDBKeyType::Date(_), IndexedDBKeyType::Number(_)) => Some(Ordering::Greater),
            // Step 10: If tb is date and ta is number, return -1.
            (IndexedDBKeyType::Number(_), IndexedDBKeyType::Date(_)) => Some(Ordering::Less),
            // Step 11 skipped
            // TODO: Likely a tiny bit wrong (use js number comparison)
            (IndexedDBKeyType::Number(a), IndexedDBKeyType::Number(b)) => a.partial_cmp(b),
            // TODO: Likely a tiny bit wrong (use js string comparison)
            (IndexedDBKeyType::String(a), IndexedDBKeyType::String(b)) => a.partial_cmp(b),
            // TODO: Likely a little wrong (use js binary comparison)
            (IndexedDBKeyType::Binary(a), IndexedDBKeyType::Binary(b)) => a.partial_cmp(b),
            (IndexedDBKeyType::Date(a), IndexedDBKeyType::Date(b)) => a.partial_cmp(b),
            // TODO: Probably also wrong (the items in a and b should be compared, double check against the spec)
            (IndexedDBKeyType::Array(a), IndexedDBKeyType::Array(b)) => a.partial_cmp(b),
            // No catch-all is used, rust ensures that all variants are handled
        }
    }
}

impl PartialEq for IndexedDBKeyType {
    fn eq(&self, other: &Self) -> bool {
        let cmp = self.partial_cmp(other);
        match cmp {
            Some(Ordering::Equal) => true,
            Some(Ordering::Less) | Some(Ordering::Greater) => false,
            None => {
                // If we can't compare the two keys, we assume they are not equal.
                false
            },
        }
    }
}

/// <https://www.w3.org/TR/IndexedDB-3/#key-range>
#[derive(Clone, Debug, Default, Deserialize, MallocSizeOf, Serialize)]
pub struct IndexedDBKeyRange {
    pub lower: Option<IndexedDBKeyType>,
    pub upper: Option<IndexedDBKeyType>,
    pub lower_open: bool,
    pub upper_open: bool,
}

impl From<IndexedDBKeyType> for IndexedDBKeyRange {
    fn from(key: IndexedDBKeyType) -> Self {
        IndexedDBKeyRange {
            lower: Some(key.clone()),
            upper: Some(key),
            ..Default::default()
        }
    }
}

impl IndexedDBKeyRange {
    pub fn only(key: IndexedDBKeyType) -> Self {
        Self::from(key)
    }

    pub fn new(
        lower: Option<IndexedDBKeyType>,
        upper: Option<IndexedDBKeyType>,
        lower_open: bool,
        upper_open: bool,
    ) -> Self {
        IndexedDBKeyRange {
            lower,
            upper,
            lower_open,
            upper_open,
        }
    }

    pub fn lower_bound(key: IndexedDBKeyType, open: bool) -> Self {
        IndexedDBKeyRange {
            lower: Some(key),
            upper: None,
            lower_open: open,
            upper_open: true,
        }
    }

    pub fn upper_bound(key: IndexedDBKeyType, open: bool) -> Self {
        IndexedDBKeyRange {
            lower: None,
            upper: Some(key),
            lower_open: true,
            upper_open: open,
        }
    }

    // <https://www.w3.org/TR/IndexedDB-3/#in>
    pub fn contains(&self, key: &IndexedDBKeyType) -> bool {
        // A key is in a key range if both of the following conditions are fulfilled:
        // The lower bound is null, or it is less than key,
        // or it is both equal to key and the lower open flag is unset.
        // The upper bound is null, or it is greater than key,
        // or it is both equal to key and the upper open flag is unset
        let lower_bound_condition = self
            .lower
            .as_ref()
            .is_none_or(|lower| lower < key || (!self.lower_open && lower == key));
        let upper_bound_condition = self
            .upper
            .as_ref()
            .is_none_or(|upper| key < upper || (!self.upper_open && key == upper));
        lower_bound_condition && upper_bound_condition
    }

    pub fn is_singleton(&self) -> bool {
        self.lower.is_some() && self.lower == self.upper && !self.lower_open && !self.upper_open
    }

    pub fn as_singleton(&self) -> Option<&IndexedDBKeyType> {
        if self.is_singleton() {
            return Some(self.lower.as_ref().unwrap());
        }
        None
    }

    /// Raises the lower bound to `key` (excluded if `open`), unless the current one is tighter.
    pub fn restrict_lower(&mut self, key: IndexedDBKeyType, open: bool) {
        let tighter = self
            .lower
            .as_ref()
            .is_none_or(|lower| &key > lower || (&key == lower && open && !self.lower_open));
        if tighter {
            self.lower = Some(key);
            self.lower_open = open;
        }
    }

    /// Lowers the upper bound to `key` (excluded if `open`), unless the current one is tighter.
    pub fn restrict_upper(&mut self, key: IndexedDBKeyType, open: bool) {
        let tighter = self
            .upper
            .as_ref()
            .is_none_or(|upper| &key < upper || (&key == upper && open && !self.upper_open));
        if tighter {
            self.upper = Some(key);
            self.upper_open = open;
        }
    }
}

/// A serialized value read from an object store, with the primary key of its record. A value
/// stored under a generated key in a store with an in-line key path doesn't contain the key: script
/// injects it into the deserialized value.
#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub struct IndexedDBValue {
    pub primary_key: IndexedDBKeyType,
    pub value: Vec<u8>,
}

/// <https://w3c.github.io/IndexedDB/#record-snapshot>
#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
pub struct IndexedDBRecord {
    pub key: IndexedDBKeyType,
    pub primary_key: IndexedDBKeyType,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
pub struct IndexedDBIndex {
    pub name: String,
    pub key_path: KeyPath,
    pub multi_entry: bool,
    pub unique: bool,
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
pub struct IndexedDBObjectStore {
    pub name: String,
    pub key_path: Option<KeyPath>,
    pub has_key_generator: bool,
    pub indexes: Vec<IndexedDBIndex>,
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub enum PutItemResult {
    Key(IndexedDBKeyType),
    CannotOverwrite,
    /// The store's key generator can't generate another key.
    KeyGeneratorExhausted,
}

#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncReadOnlyOperation {
    /// Gets the value associated with the given key in the associated idb data
    GetKey {
        callback: GenericCallback<BackendResult<Option<IndexedDBKeyType>>>,
        key_range: IndexedDBKeyRange,
    },
    GetItem {
        callback: GenericCallback<BackendResult<Option<IndexedDBValue>>>,
        key_range: IndexedDBKeyRange,
    },

    GetAllKeys {
        callback: GenericCallback<BackendResult<Vec<IndexedDBKeyType>>>,
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
    },
    GetAllItems {
        callback: GenericCallback<BackendResult<Vec<IndexedDBValue>>>,
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
    },

    Count {
        callback: GenericCallback<BackendResult<u64>>,
        key_range: IndexedDBKeyRange,
    },
    /// Gets the records in `key_range` in ascending key order: the first `count` of them, or the
    /// last `count` if `reverse` is set, or all of them if `count` is `None`.
    Iterate {
        callback: GenericCallback<BackendResult<Vec<IndexedDBRecord>>>,
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
        reverse: bool,
    },
}

impl AsyncReadOnlyOperation {
    fn notify_error(&self, error: BackendError) {
        let _ = match self {
            Self::GetKey { callback, .. } => callback.send(Err(error)),
            Self::GetItem { callback, .. } => callback.send(Err(error)),
            Self::GetAllKeys { callback, .. } => callback.send(Err(error)),
            Self::GetAllItems { callback, .. } => callback.send(Err(error)),
            Self::Count { callback, .. } => callback.send(Err(error)),
            Self::Iterate { callback, .. } => callback.send(Err(error)),
        };
    }
}

#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncReadWriteOperation {
    /// Sets the value of the given key in the associated idb data, or of a key from the store's
    /// key generator if `key` is `None`.
    PutItem {
        callback: GenericCallback<BackendResult<PutItemResult>>,
        key: Option<IndexedDBKeyType>,
        value: Vec<u8>,
        should_overwrite: bool,
    },

    /// Removes the key/value pair for the given key in the associated idb data
    RemoveItem {
        callback: GenericCallback<BackendResult<()>>,
        key_range: IndexedDBKeyRange,
    },
    /// Clears all key/value pairs in the associated idb data
    Clear(GenericCallback<BackendResult<()>>),
}

impl AsyncReadWriteOperation {
    fn notify_error(&self, error: BackendError) {
        let _ = match self {
            Self::PutItem { callback, .. } => callback.send(Err(error)),
            Self::RemoveItem { callback, .. } => callback.send(Err(error)),
            Self::Clear(callback) => callback.send(Err(error)),
        };
    }
}

#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncSchemaOperation {
    /// Creates a new index for the database
    CreateIndex {
        callback: GenericCallback<BackendError>,
        index_name: String,
        key_path: KeyPath,
        unique: bool,
        multi_entry: bool,
    },
    /// Rename an index
    RenameIndex {
        callback: GenericCallback<BackendError>,
        index_name: String,
        new_name: String,
    },
    /// Delete an index
    DeleteIndex {
        callback: GenericCallback<BackendError>,
        index_name: String,
    },
    /// Creates a new store for the database
    CreateObjectStore {
        callback: GenericCallback<BackendError>,
        key_path: Option<KeyPath>,
        auto_increment: bool,
    },
    /// Delete an existing object store in the database
    DeleteObjectStore {
        callback: GenericCallback<BackendError>,
    },
}

impl AsyncSchemaOperation {
    pub fn notify_error(&self, error: BackendError) {
        match self {
            AsyncSchemaOperation::CreateIndex { .. } |
            AsyncSchemaOperation::RenameIndex { .. } |
            AsyncSchemaOperation::DeleteIndex { .. } => {},
            AsyncSchemaOperation::CreateObjectStore { callback, .. } => {
                let _ = callback.send(error);
            },
            AsyncSchemaOperation::DeleteObjectStore { callback, .. } => {
                let _ = callback.send(error);
            },
        };
    }
}

/// Operations that are not executed instantly, but rather added to a
/// queue that is eventually run.
#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncOperation {
    ReadOnly(AsyncReadOnlyOperation),
    ReadWrite(AsyncReadWriteOperation),
    Schema(AsyncSchemaOperation),
}

impl AsyncOperation {
    pub fn notify_error(&self, error: BackendError) {
        match self {
            Self::ReadOnly(operation) => operation.notify_error(error),
            Self::ReadWrite(operation) => operation.notify_error(error),
            Self::Schema(operation) => operation.notify_error(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub enum CreateObjectResult {
    Created,
    AlreadyExists,
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
/// Messaging used in the context of connection lifecycle management.
pub enum ConnectionMsg {
    /// Error if a DB is opened for a version lower than the current db version.
    VersionError { name: String, id: Uuid },
    /// Opening a connection was aborted.
    AbortError { name: String, id: Uuid },
    /// A newly created connection with a version,
    /// updgraded or not.
    Connection {
        name: String,
        id: Uuid,
        version: u64,
        upgraded: bool,
        // https://w3c.github.io/IndexedDB/#upgrade-transaction-steps
        // Step 3. Set transaction’s scope to connection’s object store set.
        object_store_names: Vec<String>,
    },
    /// An upgrade transaction for a version started.
    Upgrade {
        name: String,
        id: Uuid,
        version: u64,
        old_version: u64,
        transaction: IndexedDBTxnId,
        // https://w3c.github.io/IndexedDB/#upgrade-transaction-steps
        // Step 3. Set transaction’s scope to connection’s object store set.
        object_store_names: Vec<String>,
    },
    /// A `versionchange` event should be fired for a connection.
    VersionChange {
        /// The id of the connection.
        id: Uuid,
        /// The name of the connection.
        name: String,
        version: u64,
        old_version: u64,
    },
    /// A `blocked` event should be fired for a connection.
    Blocked {
        name: String,
        id: Uuid,
        version: u64,
        old_version: u64,
    },
    /// A backend error related to the database occurred.
    DatabaseError {
        name: String,
        id: Uuid,
        error: BackendError,
    },
    /// Ask script to recheck whether a transaction can commit now.
    TxnMaybeCommit {
        db_name: String,
        txn: IndexedDBTxnId,
    },
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
pub struct TxnCompleteMsg {
    pub origin: ImmutableOrigin,
    pub db_name: String,
    pub txn: IndexedDBTxnId,
    pub result: BackendResult<()>,
}

/// What the IndexedDB manager needs to obtain a global's local storage bottle map
/// (<https://storage.spec.whatwg.org/#obtain-a-local-storage-bottle-map>), so that script doesn't
/// wait for it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageBottleSource {
    pub client_storage: ClientStorageThreadHandle,
    pub webview: Option<WebViewId>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub version: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SyncOperation {
    /// Gets existing databases.
    GetDatabases(
        GenericCallback<BackendResult<Vec<DatabaseInfo>>>,
        ImmutableOrigin,
    ),
    /// Get object store info
    GetObjectStore(
        GenericSender<BackendResult<IndexedDBObjectStore>>,
        ImmutableOrigin,
        String, // Database
        String, // Store
    ),
    /// Commits changes of a transaction to the database
    Commit(
        GenericCallback<TxnCompleteMsg>,
        ImmutableOrigin,
        String, // Database
        IndexedDBTxnId,
    ),
    /// Aborts a transaction in the backend
    Abort(
        GenericCallback<TxnCompleteMsg>,
        ImmutableOrigin,
        String, // Database
        IndexedDBTxnId,
    ),
    /// Upgrade transaction finished after its event was fired in script.
    UpgradeTransactionFinished {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
        committed: bool,
    },
    /// <https://w3c.github.io/IndexedDB/#transaction-lifetime
    /// Step 3:
    /// When each request associated with a transaction is processed,
    /// a success or error event will be fired. While the event is
    /// being dispatched, the transaction state is set to active, allowing
    /// additional requests to be made against the transaction. Once the
    /// event dispatch is complete, the transaction’s state is set to inactive again.
    RequestHandled {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
        request_id: u64,
    },
    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-transaction>: a transaction was created
    /// with the id script gave it. Transactions are created before each other in the order in
    /// which their messages reach the backend, see
    /// <https://w3c.github.io/IndexedDB/#transaction-scheduling>.
    CreateTransaction {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
        mode: IndexedDBTxnMode,
        scope: Vec<String>,
    },
    /// Request script to recheck transaction commit eligibility.
    TxnMaybeCommit {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
    },
    TransactionFinished {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
    },

    CloseDatabase(
        ImmutableOrigin,
        Uuid,
        String, // Database
    ),

    OpenDatabase(
        // Callback for the result.
        GenericCallback<ConnectionMsg>,
        // Origin of the request.
        ImmutableOrigin,
        // Name of the database.
        String,
        // Requested db version(optional).
        Option<u64>,
        // The id of the request.
        Uuid,
        // Where to obtain the storage proxy map.
        StorageBottleSource,
    ),

    /// Deletes the database
    DeleteDatabase(
        GenericCallback<BackendResult<u64>>,
        ImmutableOrigin,
        // Database name.
        String,
        // Where to obtain the storage proxy map.
        StorageBottleSource,
        Uuid,
    ),

    /// Returns the version of the database
    Version(
        GenericSender<BackendResult<u64>>,
        ImmutableOrigin,
        String, // Database
    ),

    /// Abort pending database upgrades
    AbortPendingUpgrades {
        pending_upgrades: HashMap<String, HashSet<Uuid>>,
        origin: ImmutableOrigin,
    },

    NotifyEndOfVersionChange {
        id: Uuid,
        name: String,
        old_version: u64,
        origin: ImmutableOrigin,
    },

    /// Send a reply when done cleaning up thread resources and then shut it down
    Exit(GenericSender<()>),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum IndexedDBThreadMsg {
    Sync(SyncOperation),
    Async(
        ImmutableOrigin,
        String, // Database
        String, // ObjectStore
        IndexedDBTxnId,
        u64, // Monotonic request id in the transaction
        IndexedDBTxnMode,
        AsyncOperation,
    ),
    AsyncSchemaOperation {
        origin: ImmutableOrigin,
        database_name: String,
        store_name: String,
        operation: AsyncSchemaOperation,
        transaction: IndexedDBTxnId,
    },
    EngineTxnBatchComplete {
        origin: ImmutableOrigin,
        db_name: String,
        txn: IndexedDBTxnId,
    },

    /// Measure memory used by this thread and send the report over the provided channel.
    CollectMemoryReport(ReportsChan),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_as_singleton() {
        let key = IndexedDBKeyType::Number(1.0);
        let key2 = IndexedDBKeyType::Number(2.0);
        let range = IndexedDBKeyRange::only(key.clone());
        assert!(range.is_singleton());
        assert!(range.as_singleton().is_some());
        let range = IndexedDBKeyRange::new(Some(key), Some(key2.clone()), false, false);
        assert!(!range.is_singleton());
        assert!(range.as_singleton().is_none());
        let full_range = IndexedDBKeyRange::new(None, None, false, false);
        assert!(!full_range.is_singleton());
        assert!(full_range.as_singleton().is_none());
    }

    #[test]
    fn test_restrict_bounds() {
        let one = IndexedDBKeyType::Number(1.0);
        let two = IndexedDBKeyType::Number(2.0);
        let three = IndexedDBKeyType::Number(3.0);

        let mut range =
            IndexedDBKeyRange::new(Some(two.clone()), Some(three.clone()), false, false);
        range.restrict_lower(one.clone(), true);
        assert_eq!(range.lower, Some(two.clone()));
        assert!(!range.lower_open);
        range.restrict_lower(two.clone(), false);
        assert!(!range.lower_open);
        range.restrict_lower(two.clone(), true);
        assert!(range.lower_open);
        range.restrict_lower(three.clone(), false);
        assert_eq!(range.lower, Some(three.clone()));
        assert!(!range.lower_open);

        let mut range = IndexedDBKeyRange::default();
        range.restrict_upper(two.clone(), true);
        assert_eq!(range.upper, Some(two.clone()));
        assert!(range.upper_open);
        range.restrict_upper(two.clone(), false);
        assert!(range.upper_open);
        range.restrict_upper(three, true);
        assert_eq!(range.upper, Some(two));
        range.restrict_upper(one.clone(), false);
        assert_eq!(range.upper, Some(one));
        assert!(!range.upper_open);
        assert!(range.lower.is_none());
    }
}
