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

/// A DOMString as its UTF-16 code units, which may include unpaired surrogates
/// (<https://webidl.spec.whatwg.org/#idl-DOMString>). String keys and the names of databases,
/// object stores and indexes take this form, so that any JavaScript string survives unchanged,
/// and it orders by code units, as IndexedDB compares keys and sorts names.
#[derive(
    Clone, Default, Deserialize, Eq, Hash, MallocSizeOf, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Utf16String(Vec<u16>);

impl Utf16String {
    pub fn from_units(units: Vec<u16>) -> Self {
        Self(units)
    }

    pub fn units(&self) -> &[u16] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The string, unless it has an unpaired surrogate.
    pub fn to_utf8(&self) -> Option<String> {
        String::from_utf16(&self.0).ok()
    }

    /// The string with each unpaired surrogate replaced by U+FFFD.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf16_lossy(&self.0)
    }

    /// The code units in big-endian byte order, which sorts like the code units.
    pub fn to_be_bytes(&self) -> Vec<u8> {
        self.0.iter().flat_map(|unit| unit.to_be_bytes()).collect()
    }

    /// The string of [`Utf16String::to_be_bytes`], or `None` for an odd number of bytes.
    pub fn from_be_bytes(bytes: &[u8]) -> Option<Self> {
        let (pairs, remainder) = bytes.as_chunks::<2>();
        if !remainder.is_empty() {
            return None;
        }
        Some(Self(
            pairs.iter().map(|pair| u16::from_be_bytes(*pair)).collect(),
        ))
    }
}

impl From<&str> for Utf16String {
    fn from(string: &str) -> Self {
        Self(string.encode_utf16().collect())
    }
}

impl From<String> for Utf16String {
    fn from(string: String) -> Self {
        Self::from(string.as_str())
    }
}

impl PartialEq<&str> for Utf16String {
    fn eq(&self, other: &&str) -> bool {
        self.0.iter().copied().eq(other.encode_utf16())
    }
}

impl Debug for Utf16String {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"")?;
        for unit in char::decode_utf16(self.0.iter().copied()) {
            match unit {
                Ok(character) => write!(f, "{}", character.escape_debug())?,
                Err(error) => write!(f, "\\u{{{:x}}}", error.unpaired_surrogate())?,
            }
        }
        f.write_str("\"")
    }
}

impl Display for Utf16String {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

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
    String(Utf16String),
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
            // Step 11. string: If va is code unit less than vb, then return -1. If vb is code
            // unit less than va, then return 1. Return 0.
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
#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub struct IndexedDBRecord {
    pub key: IndexedDBKeyType,
    pub primary_key: IndexedDBKeyType,
    pub value: Vec<u8>,
}

/// The id of an object store, unique in its database. An upgrade transaction in script gives a
/// store it creates one more than the largest id its connection has seen in the transaction, and
/// the backend checks that it is larger than the id of every store in the database. The id of a
/// store deleted in an earlier transaction can be given again, so nothing that outlives a store's
/// rows may be keyed by its id.
pub type IndexedDBObjectStoreId = i64;

/// The id of an index, unique in its database, given like an [`IndexedDBObjectStoreId`], and
/// reused like it.
pub type IndexedDBIndexId = i64;

/// <https://w3c.github.io/IndexedDB/#index-construct>
#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub struct IndexedDBIndex {
    pub id: IndexedDBIndexId,
    pub name: Utf16String,
    pub key_path: KeyPath,
    pub multi_entry: bool,
    pub unique: bool,
}

/// <https://w3c.github.io/IndexedDB/#object-store-construct>, without its records: what a
/// connection knows about an object store.
#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub struct IndexedDBObjectStore {
    pub id: IndexedDBObjectStoreId,
    pub name: Utf16String,
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
        key_range: IndexedDBKeyRange,
    },
    GetItem {
        key_range: IndexedDBKeyRange,
    },

    GetAllKeys {
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
    },
    GetAllItems {
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
    },

    Count {
        key_range: IndexedDBKeyRange,
    },
    /// Gets the records in `key_range` in ascending key order: the first `count` of them, or the
    /// last `count` if `reverse` is set, or all of them if `count` is `None`.
    Iterate {
        key_range: IndexedDBKeyRange,
        count: Option<u32>,
        reverse: bool,
    },
}

#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncReadWriteOperation {
    /// Sets the value of the given key in the associated idb data, or of a key from the store's
    /// key generator if `key` is `None`.
    PutItem {
        key: Option<IndexedDBKeyType>,
        value: Vec<u8>,
        should_overwrite: bool,
    },

    /// Removes the key/value pair for the given key in the associated idb data
    RemoveItem { key_range: IndexedDBKeyRange },
    /// Clears all key/value pairs in the associated idb data
    Clear,
}

/// A change to the object stores and indexes in an upgrade transaction, of the object store that
/// [`IndexedDBThreadMsg::AsyncSchemaOperation`] names. Script doesn't wait for it: only a failure
/// is reported, as [`TransactionEvent::SchemaChangeFailed`], and it fails the transaction.
#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncSchemaOperation {
    /// Creates the object store, with its id from script.
    CreateObjectStore {
        name: Utf16String,
        key_path: Option<KeyPath>,
        auto_increment: bool,
    },
    /// Deletes the object store and its records and indexes.
    DeleteObjectStore,
    /// Renames the object store, which keeps its records and indexes.
    RenameObjectStore { new_name: Utf16String },
    /// Creates an index of the object store, with its id from script.
    CreateIndex {
        index_id: IndexedDBIndexId,
        name: Utf16String,
        key_path: KeyPath,
        unique: bool,
        multi_entry: bool,
    },
    /// Renames an index of the object store.
    RenameIndex {
        index_id: IndexedDBIndexId,
        new_name: Utf16String,
    },
    /// Deletes an index of the object store.
    DeleteIndex { index_id: IndexedDBIndexId },
}

/// Operations that are not executed instantly, but rather added to a
/// queue that is eventually run.
#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum AsyncOperation {
    ReadOnly(AsyncReadOnlyOperation),
    ReadWrite(AsyncReadWriteOperation),
    Schema(AsyncSchemaOperation),
}

/// The result of a request that the backend ran, by kind of operation.
#[derive(Clone, Debug, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub enum RequestOutput {
    /// Of [`AsyncReadOnlyOperation::GetKey`].
    Key(Option<IndexedDBKeyType>),
    /// Of [`AsyncReadOnlyOperation::GetItem`].
    Value(Option<IndexedDBValue>),
    /// Of [`AsyncReadOnlyOperation::GetAllKeys`].
    Keys(Vec<IndexedDBKeyType>),
    /// Of [`AsyncReadOnlyOperation::GetAllItems`].
    Values(Vec<IndexedDBValue>),
    /// Of [`AsyncReadOnlyOperation::Count`].
    Count(u64),
    /// Of [`AsyncReadOnlyOperation::Iterate`].
    Records(Vec<IndexedDBRecord>),
    /// Of [`AsyncReadWriteOperation::PutItem`].
    Put(PutItemResult),
    /// Of [`AsyncReadWriteOperation::RemoveItem`] and [`AsyncReadWriteOperation::Clear`].
    Done,
}

/// What the backend reports about a transaction to its connection.
#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum TransactionEvent {
    /// The result of the request `request_id` of the transaction.
    Request {
        request_id: u64,
        result: BackendResult<RequestOutput>,
    },
    /// A schema operation failed, which fails the transaction.
    SchemaChangeFailed(BackendError),
    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>: the changes of the transaction
    /// were written, or writing them failed and they were rolled back.
    Committed(BackendResult<()>),
}

/// A message on the result channel of a connection, which script creates for each open request
/// and which the backend uses for everything it reports about the open request and, once it
/// opened, the connection. The backend sends the messages in the order the work ran, and the
/// channel keeps that order, so the results of a transaction's requests reach script in request
/// order, and the open request's outcome comes before the connection's events.
#[derive(Debug, Deserialize, MallocSizeOf, Serialize)]
pub enum ConnectionEvent {
    /// About the open request: its outcome, or that it is blocked.
    Open(ConnectionMsg),
    /// Something happened to the transaction of the connection with the serial number `serial`.
    Transaction {
        serial: u64,
        event: TransactionEvent,
    },
    /// <https://w3c.github.io/IndexedDB/#eventdef-idbdatabase-versionchange>: another
    /// connection waits for this one to close, to open the database with `new_version`.
    VersionChange { old_version: u64, new_version: u64 },
}

#[derive(Clone, Debug, Deserialize, MallocSizeOf, Serialize)]
/// Messaging used in the context of connection lifecycle management.
pub enum ConnectionMsg {
    /// Error if a DB is opened for a version lower than the current db version.
    VersionError { name: Utf16String, id: Uuid },
    /// Opening a connection was aborted.
    AbortError { name: Utf16String, id: Uuid },
    /// A newly created connection with a version,
    /// updgraded or not.
    Connection {
        name: Utf16String,
        id: Uuid,
        version: u64,
        upgraded: bool,
        /// <https://w3c.github.io/IndexedDB/#connection-object-store-set>, as committed.
        object_stores: Vec<IndexedDBObjectStore>,
    },
    /// An upgrade transaction for a version started.
    Upgrade {
        name: Utf16String,
        id: Uuid,
        version: u64,
        old_version: u64,
        transaction: IndexedDBTxnId,
        /// <https://w3c.github.io/IndexedDB/#connection-object-store-set>, as committed, which
        /// is also the scope of the upgrade transaction.
        object_stores: Vec<IndexedDBObjectStore>,
    },
    /// A `blocked` event should be fired for a connection.
    Blocked {
        name: Utf16String,
        id: Uuid,
        version: u64,
        old_version: u64,
    },
    /// A backend error related to the database occurred.
    DatabaseError {
        name: Utf16String,
        id: Uuid,
        error: BackendError,
    },
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
    pub name: Utf16String,
    pub version: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SyncOperation {
    /// Gets existing databases.
    GetDatabases(
        GenericCallback<BackendResult<Vec<DatabaseInfo>>>,
        ImmutableOrigin,
    ),
    /// <https://w3c.github.io/IndexedDB/#commit-a-transaction>: commits the transaction once its
    /// requests have run, and reports [`TransactionEvent::Committed`]. Script sends it once the
    /// results of all the transaction's requests have been delivered.
    Commit {
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
    },
    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>: rolls the transaction back, after
    /// the work handed over before. Nothing is reported: script has aborted the transaction
    /// already, and results that arrive after this are discarded.
    Abort {
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
    },
    /// Upgrade transaction finished after its event was fired in script.
    UpgradeTransactionFinished {
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
        committed: bool,
    },
    /// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-transaction>: a transaction was created
    /// with the id script gave it. Transactions are created before each other in the order in
    /// which their messages reach the backend, see
    /// <https://w3c.github.io/IndexedDB/#transaction-scheduling>.
    CreateTransaction {
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
        mode: IndexedDBTxnMode,
        scope: Vec<IndexedDBObjectStoreId>,
    },
    TransactionFinished {
        origin: ImmutableOrigin,
        db_name: Utf16String,
        txn: IndexedDBTxnId,
    },

    CloseDatabase(
        ImmutableOrigin,
        Uuid,
        Utf16String, // Database
    ),

    OpenDatabase(
        // Origin of the request.
        ImmutableOrigin,
        // Name of the database.
        Utf16String,
        // Requested db version(optional).
        Option<u64>,
        // The id of the request.
        Uuid,
        // Where to obtain the storage proxy map.
        StorageBottleSource,
        // The result channel of the open request and of the connection, if it opens.
        GenericCallback<ConnectionEvent>,
    ),

    /// Deletes the database
    DeleteDatabase(
        GenericCallback<BackendResult<u64>>,
        ImmutableOrigin,
        // Database name.
        Utf16String,
        // Where to obtain the storage proxy map.
        StorageBottleSource,
        Uuid,
    ),

    /// Abort pending database upgrades
    AbortPendingUpgrades {
        pending_upgrades: HashMap<Utf16String, HashSet<Uuid>>,
        origin: ImmutableOrigin,
    },

    NotifyEndOfVersionChange {
        id: Uuid,
        name: Utf16String,
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
        Utf16String, // Database
        IndexedDBObjectStoreId,
        IndexedDBTxnId,
        u64, // Monotonic request id in the transaction
        IndexedDBTxnMode,
        AsyncOperation,
    ),
    AsyncSchemaOperation {
        origin: ImmutableOrigin,
        database_name: Utf16String,
        store_id: IndexedDBObjectStoreId,
        operation: AsyncSchemaOperation,
        transaction: IndexedDBTxnId,
    },
    EngineTxnBatchComplete {
        origin: ImmutableOrigin,
        db_name: Utf16String,
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
