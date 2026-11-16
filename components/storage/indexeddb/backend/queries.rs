/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The IndexedDB operations on the tables of `schema.rs`.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use rustc_hash::FxHashSet;
use storage_traits::indexeddb::{
    IndexedDBCursorDirection, IndexedDBCursorStep, IndexedDBIndex, IndexedDBIndexId,
    IndexedDBIndexKeys, IndexedDBKeyRange, IndexedDBKeyType, IndexedDBObjectStore,
    IndexedDBObjectStoreId, IndexedDBRecord, IndexedDBRecordIndexKeys, IndexedDBValue, KeyPath,
    PutItemResult, Utf16String,
};

use super::executor::{SqlError, SqlExecutor, SqlParam, SqlRow, query_row, query_rows};
use crate::indexeddb::engines::encoding;

/// A row of `object_store`, with the indexes of the store.
#[derive(Clone, Debug)]
pub(crate) struct ObjectStore {
    pub(crate) id: i64,
    /// The key generator's current number, or 0 for a store without a key generator.
    pub(crate) auto_increment: i64,
    /// In the order of their ids.
    pub(crate) indexes: Vec<StoreIndex>,
}

impl ObjectStore {
    pub(crate) fn index(&self, id: IndexedDBIndexId) -> Option<StoreIndex> {
        self.indexes.iter().copied().find(|index| index.id == id)
    }

    /// The tables that hold the records of the store's indexes.
    fn index_tables(&self) -> impl Iterator<Item = &'static str> {
        let has = |unique| self.indexes.iter().any(|index| index.unique == unique);
        [(has(false), "index_data"), (has(true), "unique_index_data")]
            .into_iter()
            .filter_map(|(has, table)| has.then_some(table))
    }
}

/// An index of an object store, as reading and writing its records needs it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct StoreIndex {
    pub(crate) id: IndexedDBIndexId,
    pub(crate) unique: bool,
}

impl StoreIndex {
    /// The table of the index's records, `(index_id, value, object_data_key, object_store_id)`,
    /// where `value` is the encoded index key and `object_data_key` the encoded primary key of
    /// the record in the store. `unique_index_data`'s primary key `(index_id, value)` allows one
    /// record per key; `index_data`'s is `(index_id, value, object_data_key)`. Both read in
    /// (key, primary key) order.
    pub(crate) fn table(&self) -> &'static str {
        if self.unique {
            "unique_index_data"
        } else {
            "index_data"
        }
    }

    fn insert_statement(&self) -> &'static str {
        if self.unique {
            "INSERT INTO unique_index_data (index_id, value, object_data_key, object_store_id) \
             VALUES (?, ?, ?, ?)"
        } else {
            "INSERT INTO index_data (index_id, value, object_data_key, object_store_id) \
             VALUES (?, ?, ?, ?)"
        }
    }
}

pub(crate) fn encode_key_path(key_path: &KeyPath) -> Result<Vec<u8>, SqlError> {
    postcard::to_stdvec(key_path).map_err(SqlError::KeyPath)
}

pub(crate) fn decode_key_path(data: &[u8]) -> Result<KeyPath, SqlError> {
    postcard::from_bytes(data).map_err(SqlError::KeyPath)
}

fn decode_key(data: &[u8]) -> Result<IndexedDBKeyType, SqlError> {
    encoding::deserialize(data).ok_or(SqlError::InvalidKey)
}

fn decode_name(data: &[u8]) -> Result<Utf16String, SqlError> {
    Utf16String::from_be_bytes(data).ok_or(SqlError::InvalidName)
}

fn expect_one_changed_row(changed: u64, context: &'static str) -> Result<(), SqlError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(SqlError::MissingRow(context))
    }
}

pub(crate) fn object_store<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
) -> Result<Option<ObjectStore>, SqlError> {
    let rows = query_rows(
        executor,
        "SELECT s.id, s.auto_increment, i.id, i.unique_index FROM object_store AS s \
         LEFT JOIN object_store_index AS i ON i.object_store_id = s.id \
         WHERE s.id = ? ORDER BY i.id",
        &[SqlParam::Integer(store_id)],
        |row| {
            let index = row
                .optional_integer(2)?
                .map(|id| {
                    Ok::<_, SqlError>(StoreIndex {
                        id,
                        unique: row.integer(3)? != 0,
                    })
                })
                .transpose()?;
            Ok((row.integer(0)?, row.integer(1)?, index))
        },
    )?;
    let Some(&(id, auto_increment, _)) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(ObjectStore {
        id,
        auto_increment,
        indexes: rows.into_iter().filter_map(|(_, _, index)| index).collect(),
    }))
}

fn required_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
) -> Result<ObjectStore, SqlError> {
    object_store(executor, store_id)?.ok_or(SqlError::MissingRow("object store"))
}

/// The metadata of every object store and its indexes, in the order they were created.
pub(crate) fn object_stores<X: SqlExecutor>(
    executor: &mut X,
) -> Result<Vec<IndexedDBObjectStore>, SqlError> {
    let mut stores = query_rows(
        executor,
        "SELECT id, name, key_path, auto_increment FROM object_store ORDER BY id",
        &[],
        |row| {
            Ok(IndexedDBObjectStore {
                id: row.integer(0)?,
                name: decode_name(&row.blob(1)?)?,
                key_path: row
                    .optional_blob(2)?
                    .map(|key_path| decode_key_path(&key_path))
                    .transpose()?,
                has_key_generator: row.integer(3)? != 0,
                indexes: Vec::new(),
            })
        },
    )?;
    let indexes = query_rows(
        executor,
        "SELECT object_store_id, id, name, key_path, unique_index, multi_entry_index \
         FROM object_store_index ORDER BY id",
        &[],
        |row| {
            Ok((
                row.integer(0)?,
                IndexedDBIndex {
                    id: row.integer(1)?,
                    name: decode_name(&row.blob(2)?)?,
                    key_path: decode_key_path(&row.blob(3)?)?,
                    unique: row.integer(4)? != 0,
                    multi_entry: row.integer(5)? != 0,
                },
            ))
        },
    )?;
    for (store_id, index) in indexes {
        let store = stores
            .iter_mut()
            .find(|store| store.id == store_id)
            .ok_or(SqlError::MissingRow("object store of an index"))?;
        store.indexes.push(index);
    }
    Ok(stores)
}

/// Whether `id` is larger than every value in the `id` column of `table`.
fn is_new_id<X: SqlExecutor>(executor: &mut X, table: &str, id: i64) -> Result<bool, SqlError> {
    let largest = query_row(
        executor,
        &format!("SELECT MAX(id) FROM {table}"),
        &[],
        |row| row.optional_integer(0),
    )?
    .flatten();
    Ok(largest.is_none_or(|largest| id > largest))
}

/// <https://w3c.github.io/IndexedDB/#dom-idbdatabase-createobjectstore> step 9, with the id
/// script gave the store, which must be larger than the id of every store in the database.
pub(crate) fn create_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    name: &Utf16String,
    key_path: Option<&KeyPath>,
    auto_increment: bool,
) -> Result<(), SqlError> {
    if store_id <= 0 || !is_new_id(executor, "object_store", store_id)? {
        return Err(SqlError::InvalidMetadata("object store id is not new"));
    }
    if object_store_named(executor, name)?.is_some() {
        return Err(SqlError::InvalidMetadata("object store name exists"));
    }
    let key_path = key_path.map(encode_key_path).transpose()?;
    executor.execute(
        "INSERT INTO object_store (id, name, key_path, auto_increment) VALUES (?, ?, ?, ?)",
        &[
            SqlParam::Integer(store_id),
            SqlParam::Blob(&name.to_be_bytes()),
            key_path.as_deref().map_or(SqlParam::Null, SqlParam::Blob),
            SqlParam::Integer(auto_increment as i64),
        ],
    )?;
    Ok(())
}

/// The id of the object store named `name`, if there is one.
fn object_store_named<X: SqlExecutor>(
    executor: &mut X,
    name: &Utf16String,
) -> Result<Option<IndexedDBObjectStoreId>, SqlError> {
    query_row(
        executor,
        "SELECT id FROM object_store WHERE name = ?",
        &[SqlParam::Blob(&name.to_be_bytes())],
        |row| row.integer(0),
    )
}

pub(crate) fn delete_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
) -> Result<(), SqlError> {
    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-deleteobjectstore
    // Step 7. Destroy store.
    let store = required_object_store(executor, store_id)?;
    let params = [SqlParam::Integer(store.id)];
    executor.execute("DELETE FROM index_data WHERE object_store_id = ?", &params)?;
    executor.execute(
        "DELETE FROM unique_index_data WHERE object_store_id = ?",
        &params,
    )?;
    executor.execute(
        "DELETE FROM object_store_index WHERE object_store_id = ?",
        &params,
    )?;
    executor.execute("DELETE FROM object_data WHERE object_store_id = ?", &params)?;
    let changed = executor.execute("DELETE FROM object_store WHERE id = ?", &params)?;
    expect_one_changed_row(changed, "object store delete")
}

/// <https://w3c.github.io/IndexedDB/#dom-idbobjectstore-name> step 9. The records and indexes
/// stay with the store.
pub(crate) fn rename_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    new_name: &Utf16String,
) -> Result<(), SqlError> {
    if object_store_named(executor, new_name)?.is_some_and(|other| other != store_id) {
        return Err(SqlError::InvalidMetadata("object store name exists"));
    }
    let changed = executor.execute(
        "UPDATE object_store SET name = ? WHERE id = ?",
        &[
            SqlParam::Blob(&new_name.to_be_bytes()),
            SqlParam::Integer(store_id),
        ],
    )?;
    expect_one_changed_row(changed, "object store rename")
}

/// <https://w3c.github.io/IndexedDB/#key-generator-current-number>, or `None` for a store without
/// a key generator.
#[cfg(test)]
pub(crate) fn key_generator_current_number<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
) -> Result<Option<i64>, SqlError> {
    Ok(object_store(executor, store_id)?
        .and_then(|store| (store.auto_increment != 0).then_some(store.auto_increment)))
}

/// The id of the index named `name` of the store `store_id`, if there is one.
fn index_named<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    name: &Utf16String,
) -> Result<Option<IndexedDBIndexId>, SqlError> {
    query_row(
        executor,
        "SELECT id FROM object_store_index WHERE object_store_id = ? AND name = ?",
        &[
            SqlParam::Integer(store_id),
            SqlParam::Blob(&name.to_be_bytes()),
        ],
        |row| row.integer(0),
    )
}

/// <https://w3c.github.io/IndexedDB/#dom-idbobjectstore-createindex> step 11, with the id script
/// gave the index, which must be larger than the id of every index in the database.
pub(crate) fn create_index<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    index_id: IndexedDBIndexId,
    name: &Utf16String,
    key_path: &KeyPath,
    unique: bool,
    multi_entry: bool,
) -> Result<(), SqlError> {
    let store = required_object_store(executor, store_id)?;
    if index_id <= 0 || !is_new_id(executor, "object_store_index", index_id)? {
        return Err(SqlError::InvalidMetadata("index id is not new"));
    }
    if index_named(executor, store.id, name)?.is_some() {
        return Err(SqlError::InvalidMetadata("index name exists"));
    }
    executor.execute(
        "INSERT INTO object_store_index \
         (id, object_store_id, name, key_path, unique_index, multi_entry_index) \
         VALUES (?, ?, ?, ?, ?, ?)",
        &[
            SqlParam::Integer(index_id),
            SqlParam::Integer(store.id),
            SqlParam::Blob(&name.to_be_bytes()),
            SqlParam::Blob(&encode_key_path(key_path)?),
            SqlParam::Integer(unique as i64),
            SqlParam::Integer(multi_entry as i64),
        ],
    )?;
    Ok(())
}

/// <https://w3c.github.io/IndexedDB/#dom-idbindex-name> step 9.
pub(crate) fn rename_index<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    index_id: IndexedDBIndexId,
    new_name: &Utf16String,
) -> Result<(), SqlError> {
    if index_named(executor, store_id, new_name)?.is_some_and(|other| other != index_id) {
        return Err(SqlError::InvalidMetadata("index name exists"));
    }
    let changed = executor.execute(
        "UPDATE object_store_index SET name = ? WHERE id = ? AND object_store_id = ?",
        &[
            SqlParam::Blob(&new_name.to_be_bytes()),
            SqlParam::Integer(index_id),
            SqlParam::Integer(store_id),
        ],
    )?;
    expect_one_changed_row(changed, "index rename")
}

/// <https://w3c.github.io/IndexedDB/#dom-idbobjectstore-deleteindex> step 8: deletes the index and
/// its records.
pub(crate) fn delete_index<X: SqlExecutor>(
    executor: &mut X,
    store_id: IndexedDBObjectStoreId,
    index_id: IndexedDBIndexId,
) -> Result<(), SqlError> {
    let params = [SqlParam::Integer(index_id)];
    executor.execute("DELETE FROM index_data WHERE index_id = ?", &params)?;
    executor.execute("DELETE FROM unique_index_data WHERE index_id = ?", &params)?;
    let changed = executor.execute(
        "DELETE FROM object_store_index WHERE id = ? AND object_store_id = ?",
        &[SqlParam::Integer(index_id), SqlParam::Integer(store_id)],
    )?;
    expect_one_changed_row(changed, "index delete")
}

pub(crate) fn version<X: SqlExecutor>(executor: &mut X) -> Result<u64, SqlError> {
    let version = query_row(
        executor,
        "SELECT version FROM database LIMIT 1",
        &[],
        |row| row.integer(0),
    )?
    .ok_or(SqlError::MissingRow("database version"))?;
    Ok(u64::from_ne_bytes(version.to_ne_bytes()))
}

pub(crate) fn set_version<X: SqlExecutor>(executor: &mut X, version: u64) -> Result<(), SqlError> {
    let changed = executor.execute(
        "UPDATE database SET version = ?",
        &[SqlParam::Integer(i64::from_ne_bytes(version.to_ne_bytes()))],
    )?;
    if changed == 0 {
        return Err(SqlError::MissingRow("database version update"));
    }
    Ok(())
}

/// The conditions of a key range on an encoded key column, to append to a `WHERE` clause, and
/// the encoded bounds to bind for their placeholders, in order.
#[derive(Default)]
struct RangeCondition {
    /// The conditions, with `{}` for the column.
    sql: String,
    bounds: Vec<Vec<u8>>,
}

impl RangeCondition {
    fn push(&mut self, sql: &str, bound: &IndexedDBKeyType) {
        self.sql.push_str(sql);
        self.bounds.push(encoding::serialize(bound));
    }

    /// Adds the condition `operator` (such as `>`) on the (key, primary key) pairs of an index's
    /// records, compared with (`key`, `primary_key`) in that order.
    fn push_pair(
        &mut self,
        operator: &str,
        key: &IndexedDBKeyType,
        primary_key: &IndexedDBKeyType,
    ) {
        self.sql
            .push_str(&format!(" AND ({{}}, i.object_data_key) {operator} (?, ?)"));
        self.bounds.push(encoding::serialize(key));
        self.bounds.push(encoding::serialize(primary_key));
    }

    /// Keeps, of the records of `index` with the same key, only the one with the lowest primary
    /// key.
    fn first_record_of_each_key(&mut self, index: StoreIndex) {
        // NOTE: Not `MIN(i.object_data_key)` with `GROUP BY i.value`: RDB fails to read a column
        // that holds the MIN() or MAX() of BLOBs (E_ERROR from OH_Cursor_IsNull).
        self.sql.push_str(&format!(
            " AND NOT EXISTS (SELECT 1 FROM {} AS f WHERE f.index_id = i.index_id \
             AND f.value = i.value AND f.object_data_key < i.object_data_key)",
            index.table()
        ));
    }

    /// The range of the one encoded key `key`.
    fn only(key: Vec<u8>) -> Self {
        Self {
            sql: " AND {} = ?".to_owned(),
            bounds: vec![key],
        }
    }

    /// The conditions on the column `column`.
    fn on(&self, column: &str) -> String {
        self.sql.replace("{}", column)
    }

    /// The parameters for an `id = ?` condition with `id` followed by this condition.
    fn params(&self, id: i64) -> Vec<SqlParam<'_>> {
        std::iter::once(SqlParam::Integer(id))
            .chain(self.bounds.iter().map(|bound| SqlParam::Blob(bound)))
            .collect()
    }
}

fn range_condition(range: &IndexedDBKeyRange) -> RangeCondition {
    let mut condition = RangeCondition::default();
    if let Some(singleton) = range.as_singleton() {
        condition.push(" AND {} = ?", singleton);
        return condition;
    }
    if let Some(lower) = range.lower.as_ref() {
        let sql = if range.lower_open {
            " AND {} > ?"
        } else {
            " AND {} >= ?"
        };
        condition.push(sql, lower);
    }
    if let Some(upper) = range.upper.as_ref() {
        let sql = if range.upper_open {
            " AND {} < ?"
        } else {
            " AND {} <= ?"
        };
        condition.push(sql, upper);
    }
    condition
}

/// The records that a read reads: those of an object store, or those of an index, whose keys are
/// index keys and whose values are the keys of the records of the index's store, which hold the
/// referenced values. Both are in key order, and an index's secondarily in value order.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecordSource<'a> {
    ObjectStore(&'a ObjectStore),
    Index(StoreIndex),
}

/// What a read selects of each record, in this order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum RecordColumns {
    /// The primary key: the key of a store's record, the value of an index's record.
    PrimaryKey,
    /// The primary key and the (referenced) value.
    Value,
    /// The key, the primary key and the (referenced) value.
    Record,
    /// The key and the primary key.
    Keys,
}

impl RecordSource<'_> {
    /// The `FROM` and `WHERE` clause of a read of `columns` of the records whose keys meet
    /// `condition`, which needs [`RecordSource::id`] and the condition's bounds as parameters,
    /// and the selected columns.
    fn table_and_condition(
        &self,
        columns: RecordColumns,
        condition: &RangeCondition,
    ) -> (String, String) {
        let (from, key, primary_key, value) = match self {
            Self::ObjectStore(_) => (
                "object_data AS o WHERE o.object_store_id = ?".to_owned(),
                "o.key",
                "o.key",
                "o.data",
            ),
            Self::Index(index)
                if matches!(columns, RecordColumns::PrimaryKey | RecordColumns::Keys) =>
            {
                (
                    format!("{} AS i WHERE i.index_id = ?", index.table()),
                    "i.value",
                    "i.object_data_key",
                    "",
                )
            },
            Self::Index(index) => (
                format!(
                    "{} AS i JOIN object_data AS o \
                     ON o.object_store_id = i.object_store_id AND o.key = i.object_data_key \
                     WHERE i.index_id = ?",
                    index.table()
                ),
                "i.value",
                "i.object_data_key",
                "o.data",
            ),
        };
        let selected = match columns {
            RecordColumns::PrimaryKey => primary_key.to_owned(),
            RecordColumns::Value => format!("{primary_key}, {value}"),
            RecordColumns::Record => format!("{key}, {primary_key}, {value}"),
            RecordColumns::Keys => format!("{key}, {primary_key}"),
        };
        (format!("{from}{}", condition.on(key)), selected)
    }

    /// The `ORDER BY` terms of the records in key order, or in reverse order.
    fn order(&self, reverse: bool) -> &'static str {
        match (self, reverse) {
            (Self::ObjectStore(_), false) => "o.key ASC",
            (Self::ObjectStore(_), true) => "o.key DESC",
            // A unique index has one record per key.
            (Self::Index(StoreIndex { unique: true, .. }), false) => "i.value ASC",
            (Self::Index(StoreIndex { unique: true, .. }), true) => "i.value DESC",
            (Self::Index(_), false) => "i.value ASC, i.object_data_key ASC",
            (Self::Index(_), true) => "i.value DESC, i.object_data_key DESC",
        }
    }

    /// The id that the `WHERE` clause of [`RecordSource::table_and_condition`] compares.
    fn id(&self) -> i64 {
        match self {
            Self::ObjectStore(store) => store.id,
            Self::Index(index) => index.id,
        }
    }
}

/// Selects `columns` of the records of `source` in `range`, in ascending key order or descending
/// if `reverse` is set, at most `count` of them.
fn select_records<X: SqlExecutor, T>(
    executor: &mut X,
    source: RecordSource,
    columns: RecordColumns,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
    reverse: bool,
    map_row: impl Fn(&dyn SqlRow) -> Result<T, SqlError>,
) -> Result<Vec<T>, SqlError> {
    let condition = range_condition(range);
    select(
        executor, source, columns, &condition, reverse, count, 0, map_row,
    )
}

/// Selects `columns` of the records of `source` whose keys meet `condition`, in ascending key
/// order or descending if `reverse` is set, at most `count` of them, after skipping `offset`.
#[allow(clippy::too_many_arguments)]
fn select<X: SqlExecutor, T>(
    executor: &mut X,
    source: RecordSource,
    columns: RecordColumns,
    condition: &RangeCondition,
    reverse: bool,
    count: Option<u32>,
    offset: u32,
    map_row: impl Fn(&dyn SqlRow) -> Result<T, SqlError>,
) -> Result<Vec<T>, SqlError> {
    let (table_and_condition, selected) = source.table_and_condition(columns, condition);
    let mut sql = format!(
        "SELECT {selected} FROM {table_and_condition} ORDER BY {}",
        source.order(reverse)
    );
    let mut params = condition.params(source.id());
    if let Some(count) = count {
        sql.push_str(" LIMIT ?");
        params.push(SqlParam::Integer(count.into()));
        if offset > 0 {
            sql.push_str(" OFFSET ?");
            params.push(SqlParam::Integer(offset.into()));
        }
    }
    query_rows(executor, &sql, &params, map_row)
}

fn primary_key_of_row(row: &dyn SqlRow) -> Result<IndexedDBKeyType, SqlError> {
    decode_key(&row.blob(0)?)
}

fn value_of_row(row: &dyn SqlRow) -> Result<IndexedDBValue, SqlError> {
    Ok(IndexedDBValue {
        primary_key: decode_key(&row.blob(0)?)?,
        value: row.blob(1)?,
    })
}

/// <https://w3c.github.io/IndexedDB/#retrieve-a-value-from-an-object-store> and
/// <https://w3c.github.io/IndexedDB/#retrieve-a-referenced-value-from-an-index>, with the
/// primary key of the record, which script needs to inject a generated key.
pub(crate) fn get_value<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
) -> Result<Option<IndexedDBValue>, SqlError> {
    // Step 1. Let record be the first record in source’s list of records whose key is in range,
    // if any.
    // Step 2. If record was not found, return undefined.
    // Step 3. Let serialized be record’s (referenced) value.
    // NOTE: Script deserializes it.
    Ok(select_records(
        executor,
        source,
        RecordColumns::Value,
        range,
        Some(1),
        false,
        value_of_row,
    )?
    .pop())
}

/// <https://w3c.github.io/IndexedDB/#retrieve-a-key-from-an-object-store> and
/// <https://w3c.github.io/IndexedDB/#retrieve-a-value-from-an-index>: the primary key of the
/// first record in `range`.
pub(crate) fn get_key<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
) -> Result<Option<IndexedDBKeyType>, SqlError> {
    Ok(select_records(
        executor,
        source,
        RecordColumns::PrimaryKey,
        range,
        Some(1),
        false,
        primary_key_of_row,
    )?
    .pop())
}

/// <https://w3c.github.io/IndexedDB/#retrieve-multiple-items-from-an-object-store> and
/// <https://w3c.github.io/IndexedDB/#retrieve-multiple-items-from-an-index>, step 1: a count
/// that is not given or 0 means every record.
fn retrieved_count(count: Option<u32>) -> Option<u32> {
    count.filter(|count| *count != 0)
}

/// The primary keys of the first `count` records of `source` in `range`: retrieve multiple
/// items with kind "key" and direction "next".
pub(crate) fn get_all_keys<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
) -> Result<Vec<IndexedDBKeyType>, SqlError> {
    select_records(
        executor,
        source,
        RecordColumns::PrimaryKey,
        range,
        retrieved_count(count),
        false,
        primary_key_of_row,
    )
}

/// The (referenced) values of the first `count` records of `source` in `range`, with their
/// primary keys: retrieve multiple items with kind "value" and direction "next".
pub(crate) fn get_all_values<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
) -> Result<Vec<IndexedDBValue>, SqlError> {
    select_records(
        executor,
        source,
        RecordColumns::Value,
        range,
        retrieved_count(count),
        false,
        value_of_row,
    )
}

/// The bounds on the records that a step of a cursor can find, on the side of its position that
/// it moves to: `range`'s bound there, or `pair`, a bound on the (key, primary key) pairs of an
/// index's records, whichever is tighter, and `range`'s other bound.
struct StepBounds {
    range: IndexedDBKeyRange,
    /// The (key, primary key) pair, and whether the bound excludes it.
    pair: Option<(IndexedDBKeyType, IndexedDBKeyType, bool)>,
    reverse: bool,
}

impl StepBounds {
    /// Requires the records' keys to be at or beyond `key`, or beyond it if `open`.
    fn restrict_key(&mut self, key: &IndexedDBKeyType, open: bool) {
        if self.reverse {
            self.range.restrict_upper(key.clone(), open);
        } else {
            self.range.restrict_lower(key.clone(), open);
        }
        self.drop_looser_bound();
    }

    /// Requires the records to be at or beyond (`key`, `primary_key`) in (key, primary key)
    /// order, or beyond it if `open`.
    fn restrict_pair(
        &mut self,
        key: &IndexedDBKeyType,
        primary_key: &IndexedDBKeyType,
        open: bool,
    ) {
        let tighter =
            self.pair
                .as_ref()
                .is_none_or(|(current_key, current_primary_key, current_open)| {
                    let beyond = |a: &IndexedDBKeyType, b: &IndexedDBKeyType| {
                        if self.reverse { a < b } else { a > b }
                    };
                    beyond(key, current_key) ||
                        (key == current_key &&
                            (beyond(primary_key, current_primary_key) ||
                                (primary_key == current_primary_key &&
                                    open &&
                                    !current_open)))
                });
        if tighter {
            self.pair = Some((key.clone(), primary_key.clone(), open));
        }
        self.drop_looser_bound();
    }

    /// Keeps only the tighter of the range's bound on the side the cursor moves to and the
    /// pair, so that the statement's seek starts at the tighter one.
    fn drop_looser_bound(&mut self) {
        let Some((pair_key, _, _)) = &self.pair else {
            return;
        };
        let (bound, bound_open) = if self.reverse {
            (&mut self.range.upper, self.range.upper_open)
        } else {
            (&mut self.range.lower, self.range.lower_open)
        };
        let Some(bound_key) = bound.as_ref() else {
            return;
        };
        let pair_is_beyond = if self.reverse {
            pair_key < bound_key
        } else {
            pair_key > bound_key
        };
        // At the same key, an open key bound excludes every pair with the key, a closed one none.
        if pair_is_beyond || (pair_key == bound_key && !bound_open) {
            *bound = None;
        } else {
            self.pair = None;
        }
    }

    /// The conditions of these bounds on the records of an index or store.
    fn condition(&self) -> RangeCondition {
        let mut condition = range_condition(&self.range);
        if let Some((key, primary_key, open)) = &self.pair {
            let operator = match (self.reverse, open) {
                (false, true) => ">",
                (false, false) => ">=",
                (true, true) => "<",
                (true, false) => "<=",
            };
            condition.push_pair(operator, key, primary_key);
        }
        condition
    }
}

/// <https://w3c.github.io/IndexedDB/#iterate-a-cursor> step 9, for a cursor over the records of
/// `source` in `range`: the record that `step` moves the cursor to in `direction`, then the
/// records that further steps without a key would move it to, at most `records` records in all,
/// in the order of the steps. Without values if `key_only`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cursor_records<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
    direction: IndexedDBCursorDirection,
    step: &IndexedDBCursorStep,
    records: u32,
    key_only: bool,
) -> Result<Vec<IndexedDBRecord>, SqlError> {
    let IndexedDBCursorStep {
        position,
        object_store_position,
        key,
        primary_key,
        count,
    } = step;
    let reverse = matches!(
        direction,
        IndexedDBCursorDirection::Prev | IndexedDBCursorDirection::PrevUnique
    );
    let unique = matches!(
        direction,
        IndexedDBCursorDirection::NextUnique | IndexedDBCursorDirection::PrevUnique
    );
    // Step 9.1. Switch on direction:
    // "next": Let found record be the first record in records which satisfy all of the following
    // requirements:
    // - If key is defined, the record’s key is greater than or equal to key.
    // - If primaryKey is defined, the record’s key is equal to key and the record’s value is
    //   greater than or equal to primaryKey, or the record’s key is greater than key.
    // - If position is defined, and source is an object store, the record’s key is greater than
    //   position.
    // - If position is defined, and source is an index, the record’s key is equal to position and
    //   the record’s value is greater than object store position or the record’s key is greater
    //   than position.
    // - The record’s key is in range.
    // "nextunique": Let found record be the first record in records which satisfy all of the
    // following requirements:
    // - If key is defined, the record’s key is greater than or equal to key.
    // - If position is defined, the record’s key is greater than position.
    // - The record’s key is in range.
    // "prev": Let found record be the last record in records which satisfy all of the following
    // requirements: the requirements of "next", with "less" for "greater".
    // "prevunique": Let temp record be the last record in records which satisfy all of the
    // following requirements: those of "nextunique", with "less" for "greater". If temp record is
    // defined, let found record be the first record in records whose key is equal to temp
    // record’s key.
    // NOTE: The requirements bound the records on the side the cursor moves to; the statement
    // seeks to the tightest bound. The records of a source are in (key, primary key) order, so
    // the found record is the first one in the direction, and for "prevunique" the one with the
    // lowest primary key of the first key.
    let mut bounds = StepBounds {
        range: range.clone(),
        pair: None,
        reverse,
    };
    let index = match source {
        RecordSource::Index(index) => Some(index),
        RecordSource::ObjectStore(_) => None,
    };
    if let Some(key) = key {
        match primary_key {
            Some(primary_key) => bounds.restrict_pair(key, primary_key, false),
            None => bounds.restrict_key(key, false),
        }
    }
    if let Some(position) = position {
        match object_store_position {
            Some(object_store_position) if index.is_some() && !unique => {
                bounds.restrict_pair(position, object_store_position, true)
            },
            _ => bounds.restrict_key(position, true),
        }
    }
    // Step 9.5. Decrease count by 1.
    // NOTE: Each of the count steps moves on to the next record that the steps find, or to the
    // next key for the unique directions: the step skips count - 1 of them.
    let offset = count.saturating_sub(1);
    let mut condition = bounds.condition();
    // A unique index has one record per key.
    if let Some(index) = index.filter(|index| unique && !index.unique) {
        condition.first_record_of_each_key(index);
    }
    let columns = if key_only {
        RecordColumns::Keys
    } else {
        RecordColumns::Record
    };
    select(
        executor,
        source,
        columns,
        &condition,
        reverse,
        Some(records),
        offset,
        |row| {
            Ok(IndexedDBRecord {
                key: decode_key(&row.blob(0)?)?,
                primary_key: decode_key(&row.blob(1)?)?,
                value: if key_only { None } else { Some(row.blob(2)?) },
            })
        },
    )
}

/// <https://w3c.github.io/IndexedDB/#count-the-records-in-a-range>
pub(crate) fn count<X: SqlExecutor>(
    executor: &mut X,
    source: RecordSource,
    range: &IndexedDBKeyRange,
) -> Result<u64, SqlError> {
    // Step 1. Let count be the number of records, if any, in source’s list of records with key
    // in range.
    let condition = range_condition(range);
    let (table_and_condition, _) =
        source.table_and_condition(RecordColumns::PrimaryKey, &condition);
    let sql = format!("SELECT COUNT(*) FROM {table_and_condition}");
    let params = condition.params(source.id());
    let count = query_row(executor, &sql, &params, |row| row.integer(0))?.unwrap_or_default();
    // Step 2. Return count.
    Ok(count as u64)
}

/// 2^53, the largest key that a key generator generates.
const MAX_GENERATED_KEY: i64 = 1 << 53;

/// <https://w3c.github.io/IndexedDB/#generate-a-key>, without step 4: the caller increases the
/// current number once the record is stored.
fn generate_key(current_number: i64) -> Option<IndexedDBKeyType> {
    // Step 1. Let generator be store’s key generator.
    // Step 2. Let key be generator’s current number.
    // Step 3. If key is greater than 2^53 (9007199254740992), then return failure.
    if current_number > MAX_GENERATED_KEY {
        return None;
    }
    // Step 5. Return key.
    Some(IndexedDBKeyType::Number(current_number as f64))
}

/// <https://w3c.github.io/IndexedDB/#possibly-update-the-key-generator>: the new current number,
/// if `key` changes it.
fn possibly_update_the_key_generator(current_number: i64, key: &IndexedDBKeyType) -> Option<i64> {
    // Step 1. If the type of key is not number, abort these steps.
    let IndexedDBKeyType::Number(value) = key else {
        return None;
    };
    // Step 2. Let value be the value of key.
    // Step 3. Set value to the minimum of value and 2^53 (9007199254740992).
    // Step 4. Set value to the largest integer not greater than value.
    // The conversion is exact for integers up to 2^53, and saturates below i64::MIN, which is
    // less than any current number.
    let value = value.min(MAX_GENERATED_KEY as f64).floor() as i64;
    // Step 5. Let generator be store’s key generator.
    // Step 6. If value is greater than or equal to generator’s current number, then set
    // generator’s current number to value + 1.
    (value >= current_number).then_some(value + 1)
}

/// The encoded keys of the records that an index gets for a record, without duplicates.
pub(crate) struct IndexRecords {
    pub(crate) index: StoreIndex,
    pub(crate) keys: BTreeSet<Vec<u8>>,
}

/// The index records of the record with the key `primary_key` in `store`, from the keys that
/// script computed for each index of the store, with `primary_key` in place of each placeholder.
/// Keys for other indexes than exactly the store's mean that script's metadata isn't the
/// database's.
pub(crate) fn index_records(
    store: &ObjectStore,
    primary_key: &IndexedDBKeyType,
    index_keys: Vec<IndexedDBIndexKeys>,
) -> Result<Vec<IndexRecords>, SqlError> {
    let mut records: Vec<IndexRecords> = Vec::with_capacity(index_keys.len());
    for IndexedDBIndexKeys { index_id, keys } in index_keys {
        let index = store.index(index_id).ok_or(SqlError::InvalidMetadata(
            "index keys for an index the store doesn't have",
        ))?;
        if records.iter().any(|records| records.index == index) {
            return Err(SqlError::InvalidMetadata("index keys given twice"));
        }
        let keys = keys
            .into_iter()
            .map(|key| encoding::serialize(&key.with_primary_key(primary_key)))
            .collect();
        records.push(IndexRecords { index, keys });
    }
    if records.len() != store.indexes.len() {
        return Err(SqlError::InvalidMetadata("index keys missing for an index"));
    }
    Ok(records)
}

/// The first of the unique indexes in `records` that has one of its keys for another record
/// than the one with the encoded key `primary_key`.
pub(crate) fn unique_index_conflict<X: SqlExecutor>(
    executor: &mut X,
    records: &[IndexRecords],
    primary_key: &[u8],
) -> Result<Option<IndexedDBIndexId>, SqlError> {
    for records in records.iter().filter(|records| records.index.unique) {
        for key in &records.keys {
            let taken = query_row(
                executor,
                "SELECT 1 FROM unique_index_data \
                 WHERE index_id = ? AND value = ? AND object_data_key != ?",
                &[
                    SqlParam::Integer(records.index.id),
                    SqlParam::Blob(key),
                    SqlParam::Blob(primary_key),
                ],
                |_| Ok(()),
            )?
            .is_some();
            if taken {
                return Ok(Some(records.index.id));
            }
        }
    }
    Ok(None)
}

/// Stores `records`, the index records of the record of `store` with the encoded key
/// `primary_key`. The record must be stored already. A unique index's primary key rejects a key
/// that another record has: callers check with [`unique_index_conflict`] first, and the failed
/// statement fails the transaction.
pub(crate) fn insert_index_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    primary_key: &[u8],
    records: &[IndexRecords],
) -> Result<(), SqlError> {
    for records in records {
        for key in &records.keys {
            executor.execute(
                records.index.insert_statement(),
                &[
                    SqlParam::Integer(records.index.id),
                    SqlParam::Blob(key),
                    SqlParam::Blob(primary_key),
                    SqlParam::Integer(store.id),
                ],
            )?;
        }
    }
    Ok(())
}

/// Deletes the index records of the records of `store` whose keys meet `condition`.
fn delete_index_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    condition: &RangeCondition,
) -> Result<(), SqlError> {
    let params = condition.params(store.id);
    for table in store.index_tables() {
        let sql = format!(
            "DELETE FROM {table} WHERE object_store_id = ?{}",
            condition.on("object_data_key")
        );
        executor.execute(&sql, &params)?;
    }
    Ok(())
}

/// <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store>: stores `value` under
/// `key`, or under a key from the store's key generator if `key` is `None`, which requires that
/// the store has one, with `index_keys`, the keys script computed for each index of the store. A
/// record that can't be stored changes nothing, not even the key generator.
pub(crate) fn put_record<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    key: Option<IndexedDBKeyType>,
    value: &[u8],
    overwrite: bool,
    index_keys: Vec<IndexedDBIndexKeys>,
) -> Result<PutItemResult, SqlError> {
    let has_key_generator = store.auto_increment != 0;
    debug_assert!(key.is_some() || has_key_generator);
    // Step 1. If store uses a key generator, then:
    let (key, new_current_number) = match key {
        // Step 1.1. If key is undefined, then:
        None => {
            // Step 1.1.1. Let key be the result of generating a key for store.
            // Step 1.1.2. If key is failure, then this operation failed with a "ConstraintError"
            // DOMException. Abort this algorithm without taking any further steps.
            let Some(key) = generate_key(store.auto_increment) else {
                return Ok(PutItemResult::KeyGeneratorExhausted);
            };
            // Step 1.1.3. If store also uses in-line keys, then run inject a key into a value
            // using a key path with value, key and store’s key path.
            // NOTE: The value is stored without the key; script injects it when it reads the
            // value. The index keys that come from the key are placeholders.
            (key, Some(store.auto_increment + 1))
        },
        // Step 1.2. Otherwise, run possibly update the key generator for store with key.
        Some(key) if has_key_generator => {
            let new_current_number = possibly_update_the_key_generator(store.auto_increment, &key);
            (key, new_current_number)
        },
        Some(key) => (key, None),
    };

    // Step 2. If the no-overwrite flag was given to these steps and is true, and a record already
    // exists in store with its key equal to key, then this operation failed with a
    // "ConstraintError" DOMException. Abort this algorithm without taking any further steps.
    let encoded_key = encoding::serialize(&key);
    let exists = query_row(
        executor,
        "SELECT 1 FROM object_data WHERE object_store_id = ? AND key = ?",
        &[SqlParam::Integer(store.id), SqlParam::Blob(&encoded_key)],
        |_| Ok(()),
    )?
    .is_some();
    if exists && !overwrite {
        return Ok(PutItemResult::CannotOverwrite);
    }

    // Step 5. For each index which references store:
    // Step 5.1. Let index key be the result of extracting a key from a value using a key path
    // with value, index’s key path, and index’s multiEntry flag.
    // Step 5.2. If index key is an exception, or invalid, or failure, take no further actions for
    // index, and continue these steps for the next index.
    // NOTE: Script ran these steps.
    let index_records = index_records(store, &key, index_keys)?;
    // Step 5.3. If index’s multiEntry flag is false, or if index key is not an array key, and if
    // index already contains a record with key equal to index key, and index’s unique flag is
    // true, then this operation failed with a "ConstraintError" DOMException. Abort this
    // algorithm without taking any further steps.
    // Step 5.4. If index’s multiEntry flag is true and index key is an array key, and if index
    // already contains a record with key equal to any of the subkeys of index key, and index’s
    // unique flag is true, then this operation failed with a "ConstraintError" DOMException.
    // Abort this algorithm without taking any further steps.
    // NOTE: These checks run before anything is written, so that a failed request has no effects.
    // Step 3 removes the index records of the record that is replaced, so they don't count.
    if let Some(index) = unique_index_conflict(executor, &index_records, &encoded_key)? {
        return Ok(PutItemResult::UniqueIndexKeyExists(index));
    }

    // Step 3. If a record already exists in store with its key equal to key, then remove the
    // record from store using delete records from an object store.
    // Step 4. Store a record in store containing key as its key and !
    // StructuredSerializeForStorage(value) as its value. The record is stored in the object
    // store’s list of records such that the list is sorted according to the key of the records in
    // ascending order.
    if exists {
        delete_index_records(executor, store, &RangeCondition::only(encoded_key.clone()))?;
        executor.execute(
            "UPDATE object_data SET data = ? WHERE object_store_id = ? AND key = ?",
            &[
                SqlParam::Blob(value),
                SqlParam::Integer(store.id),
                SqlParam::Blob(&encoded_key),
            ],
        )?;
    } else {
        executor.execute(
            "INSERT INTO object_data (object_store_id, key, data) VALUES (?, ?, ?)",
            &[
                SqlParam::Integer(store.id),
                SqlParam::Blob(&encoded_key),
                SqlParam::Blob(value),
            ],
        )?;
    }

    // Step 5.5. If index’s multiEntry flag is false, or if index key is not an array key then
    // store a record in index containing index key as its key and key as its value. The record is
    // stored in index’s list of records such that the list is sorted primarily on the records keys,
    // and secondarily on the records values, in ascending order.
    // Step 5.6. If index’s multiEntry flag is true and index key is an array key, then for each
    // subkey of the subkeys of index key store a record in index containing subkey as its key and
    // key as its value. The records are stored in index’s list of records such that the list is
    // sorted primarily on the records keys, and secondarily on the records values, in ascending
    // order.
    insert_index_records(executor, store, &encoded_key, &index_records)?;

    if let Some(current_number) = new_current_number {
        executor.execute(
            "UPDATE object_store SET auto_increment = ? WHERE id = ?",
            &[
                SqlParam::Integer(current_number),
                SqlParam::Integer(store.id),
            ],
        )?;
    }

    // Step 6. Return key.
    Ok(PutItemResult::Key(key))
}

/// The most records that one step of populating an index reads.
pub(crate) const POPULATION_BATCH_RECORDS: usize = 500;

/// The size of the values above which a step of populating an index reads no more records.
const POPULATION_BATCH_BYTES: usize = 1 << 20;

/// The records of `store` after the key `after`, or from the first one, in key order, for
/// populating an index: at most [`POPULATION_BATCH_RECORDS`], and no more once their values add
/// up to [`POPULATION_BATCH_BYTES`]. Returns them, and whether no records follow them.
pub(crate) fn population_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    after: Option<&IndexedDBKeyType>,
) -> Result<(Vec<IndexedDBValue>, bool), SqlError> {
    let after = after.map(encoding::serialize);
    let mut sql = "SELECT key, data FROM object_data WHERE object_store_id = ?".to_owned();
    let mut params = vec![SqlParam::Integer(store.id)];
    if let Some(after) = &after {
        sql.push_str(" AND key > ?");
        params.push(SqlParam::Blob(after));
    }
    sql.push_str(" ORDER BY key LIMIT ?");
    params.push(SqlParam::Integer(POPULATION_BATCH_RECORDS as i64));
    let mut records = Vec::new();
    let mut bytes = 0;
    let mut full = false;
    executor.query(&sql, &params, &mut |row| {
        let record = value_of_row(row)?;
        bytes += record.value.len();
        records.push(record);
        full = bytes >= POPULATION_BATCH_BYTES;
        Ok(if full {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    })?;
    let last = !full && records.len() < POPULATION_BATCH_RECORDS;
    Ok((records, last))
}

/// The most keys that one statement of [`set_index_keys`] checks.
const POPULATION_KEYS_PER_CHECK: usize = 500;

/// The most index records that one statement of [`set_index_keys`] inserts, four parameters
/// each, so that a statement stays below SQLite's oldest limit of 999 parameters.
const POPULATION_RECORDS_PER_INSERT: usize = 200;

/// Stores the records of `index`, which is being populated, for the records of `store` that
/// `records` has the index keys of. Returns false, having stored none of them, if the index is
/// unique and two of the records, or one of them and a record stored before, have the same key.
/// The statements cover many records each, since on RDB each statement costs about 0.2 ms.
pub(crate) fn set_index_keys<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    index: StoreIndex,
    records: Vec<IndexedDBRecordIndexKeys>,
) -> Result<bool, SqlError> {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for IndexedDBRecordIndexKeys { primary_key, keys } in records {
        let encoded_key = encoding::serialize(&primary_key);
        let keys: BTreeSet<Vec<u8>> = keys
            .into_iter()
            .map(|key| encoding::serialize(&key.with_primary_key(&primary_key)))
            .collect();
        rows.extend(keys.into_iter().map(|key| (key, encoded_key.clone())));
    }
    if index.unique {
        let mut keys = FxHashSet::default();
        if !rows.iter().all(|(key, _)| keys.insert(key.as_slice())) {
            return Ok(false);
        }
        for chunk in rows.chunks(POPULATION_KEYS_PER_CHECK) {
            let sql = format!(
                "SELECT 1 FROM unique_index_data WHERE index_id = ? AND value IN ({}) LIMIT 1",
                vec!["?"; chunk.len()].join(", ")
            );
            let params: Vec<_> = std::iter::once(SqlParam::Integer(index.id))
                .chain(chunk.iter().map(|(key, _)| SqlParam::Blob(key)))
                .collect();
            if query_row(executor, &sql, &params, |_| Ok(()))?.is_some() {
                return Ok(false);
            }
        }
    }
    for chunk in rows.chunks(POPULATION_RECORDS_PER_INSERT) {
        let sql = format!(
            "INSERT INTO {} (index_id, value, object_data_key, object_store_id) VALUES {}",
            index.table(),
            vec!["(?, ?, ?, ?)"; chunk.len()].join(", ")
        );
        let params: Vec<_> = chunk
            .iter()
            .flat_map(|(key, primary_key)| {
                [
                    SqlParam::Integer(index.id),
                    SqlParam::Blob(key),
                    SqlParam::Blob(primary_key),
                    SqlParam::Integer(store.id),
                ]
            })
            .collect();
        executor.execute(&sql, &params)?;
    }
    Ok(true)
}

/// <https://w3c.github.io/IndexedDB/#delete-records-from-an-object-store>
pub(crate) fn delete_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
) -> Result<(), SqlError> {
    let condition = range_condition(range);
    // Step 2. For each index which references store, remove every record from index’s list of
    // records whose value is in range, if any such records exist.
    // NOTE: Before step 1, as index records refer to the store's records.
    delete_index_records(executor, store, &condition)?;
    // Step 1. Remove all records, if any, from store’s list of records with key in range.
    let sql = format!(
        "DELETE FROM object_data WHERE object_store_id = ?{}",
        condition.on("key")
    );
    executor.execute(&sql, &condition.params(store.id))?;
    // Step 3. Return undefined.
    Ok(())
}

/// <https://w3c.github.io/IndexedDB/#clear-an-object-store>
pub(crate) fn clear<X: SqlExecutor>(executor: &mut X, store: &ObjectStore) -> Result<(), SqlError> {
    // Step 2. In all indexes which reference store, remove all records.
    // NOTE: Before step 1, as index records refer to the store's records.
    delete_index_records(executor, store, &RangeCondition::default())?;
    // Step 1. Remove all records from store.
    executor.execute(
        "DELETE FROM object_data WHERE object_store_id = ?",
        &[SqlParam::Integer(store.id)],
    )?;
    // Step 3. Return undefined.
    Ok(())
}
