/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The IndexedDB operations on the tables of `schema.rs`.

use std::collections::BTreeSet;

use storage_traits::indexeddb::{
    IndexedDBIndex, IndexedDBIndexId, IndexedDBIndexKeys, IndexedDBKeyRange, IndexedDBKeyType,
    IndexedDBObjectStore, IndexedDBObjectStoreId, IndexedDBRecord, IndexedDBValue, KeyPath,
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
    #[cfg_attr(not(test), expect(dead_code))]
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

    /// The parameters for `object_store_id = ?` followed by this condition.
    fn params(&self, store: &ObjectStore) -> Vec<SqlParam<'_>> {
        std::iter::once(SqlParam::Integer(store.id))
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

/// Selects `columns` of the records of `store` in `range`, in ascending key order or descending
/// if `reverse` is set, at most `count` of them.
fn select_records<X: SqlExecutor, T>(
    executor: &mut X,
    columns: &str,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
    reverse: bool,
    map_row: impl Fn(&dyn SqlRow) -> Result<T, SqlError>,
) -> Result<Vec<T>, SqlError> {
    let condition = range_condition(range);
    let mut sql = format!(
        "SELECT {columns} FROM object_data WHERE object_store_id = ?{} ORDER BY key {}",
        condition.on("key"),
        if reverse { "DESC" } else { "ASC" }
    );
    let mut params = condition.params(store);
    if let Some(count) = count {
        sql.push_str(" LIMIT ?");
        params.push(SqlParam::Integer(count.into()));
    }
    query_rows(executor, &sql, &params, map_row)
}

fn value_of_row(row: &dyn SqlRow) -> Result<IndexedDBValue, SqlError> {
    Ok(IndexedDBValue {
        primary_key: decode_key(&row.blob(0)?)?,
        value: row.blob(1)?,
    })
}

pub(crate) fn get_value<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
) -> Result<Option<IndexedDBValue>, SqlError> {
    Ok(select_records(
        executor,
        "key, data",
        store,
        range,
        Some(1),
        false,
        value_of_row,
    )?
    .pop())
}

pub(crate) fn get_key<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
) -> Result<Option<IndexedDBKeyType>, SqlError> {
    Ok(
        select_records(executor, "key", store, range, Some(1), false, |row| {
            decode_key(&row.blob(0)?)
        })?
        .pop(),
    )
}

pub(crate) fn get_all_keys<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
) -> Result<Vec<IndexedDBKeyType>, SqlError> {
    select_records(executor, "key", store, range, count, false, |row| {
        decode_key(&row.blob(0)?)
    })
}

pub(crate) fn get_all_values<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
) -> Result<Vec<IndexedDBValue>, SqlError> {
    select_records(
        executor,
        "key, data",
        store,
        range,
        count,
        false,
        value_of_row,
    )
}

/// The records of `store` in `range` in ascending key order: the first `count` of them, or the
/// last `count` if `reverse` is set.
pub(crate) fn get_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
    count: Option<u32>,
    reverse: bool,
) -> Result<Vec<IndexedDBRecord>, SqlError> {
    let mut records = select_records(executor, "key, data", store, range, count, reverse, |row| {
        let key = decode_key(&row.blob(0)?)?;
        Ok(IndexedDBRecord {
            key: key.clone(),
            primary_key: key,
            value: row.blob(1)?,
        })
    })?;
    if reverse {
        records.reverse();
    }
    Ok(records)
}

pub(crate) fn count<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
) -> Result<u64, SqlError> {
    let condition = range_condition(range);
    let sql = format!(
        "SELECT COUNT(*) FROM object_data WHERE object_store_id = ?{}",
        condition.on("key")
    );
    let params = condition.params(store);
    let count = query_row(executor, &sql, &params, |row| row.integer(0))?.unwrap_or_default();
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
    let params = condition.params(store);
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
    executor.execute(&sql, &condition.params(store))?;
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
