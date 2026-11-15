/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The IndexedDB operations on the tables of `schema.rs`.

use storage_traits::indexeddb::{
    CreateObjectResult, IndexedDBIndex, IndexedDBKeyRange, IndexedDBKeyType, IndexedDBRecord,
    IndexedDBValue, KeyPath, PutItemResult,
};

use super::executor::{SqlError, SqlExecutor, SqlParam, SqlRow, query_row, query_rows};
use crate::indexeddb::engines::encoding;

/// A row of `object_store`.
#[derive(Clone, Debug)]
pub(crate) struct ObjectStore {
    pub(crate) id: i64,
    /// The encoded key path, see [`encode_key_path`].
    pub(crate) key_path: Option<Vec<u8>>,
    /// The key generator's current number, or 0 for a store without a key generator.
    pub(crate) auto_increment: i64,
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

fn expect_one_changed_row(changed: u64, context: &'static str) -> Result<(), SqlError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(SqlError::MissingRow(context))
    }
}

pub(crate) fn object_store<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<Option<ObjectStore>, SqlError> {
    query_row(
        executor,
        "SELECT id, key_path, auto_increment FROM object_store WHERE name = ?",
        &[SqlParam::Text(store_name)],
        |row| {
            Ok(ObjectStore {
                id: row.integer(0)?,
                key_path: row.optional_blob(1)?,
                auto_increment: row.integer(2)?,
            })
        },
    )
}

fn required_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<ObjectStore, SqlError> {
    object_store(executor, store_name)?.ok_or(SqlError::MissingRow("object store"))
}

pub(crate) fn object_store_names<X: SqlExecutor>(
    executor: &mut X,
) -> Result<Vec<String>, SqlError> {
    query_rows(
        executor,
        "SELECT name FROM object_store ORDER BY id",
        &[],
        |row| row.text(0),
    )
}

pub(crate) fn create_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
    key_path: Option<&KeyPath>,
    auto_increment: bool,
) -> Result<CreateObjectResult, SqlError> {
    if object_store(executor, store_name)?.is_some() {
        return Ok(CreateObjectResult::AlreadyExists);
    }
    let key_path = key_path.map(encode_key_path).transpose()?;
    executor.execute(
        "INSERT INTO object_store (name, key_path, auto_increment) VALUES (?, ?, ?)",
        &[
            SqlParam::Text(store_name),
            key_path.as_deref().map_or(SqlParam::Null, SqlParam::Blob),
            SqlParam::Integer(auto_increment as i64),
        ],
    )?;
    Ok(CreateObjectResult::Created)
}

pub(crate) fn delete_object_store<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<(), SqlError> {
    // https://www.w3.org/TR/IndexedDB-3/#dom-idbdatabase-deleteobjectstore
    // Step 7. Destroy store.
    let store = required_object_store(executor, store_name)?;
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

pub(crate) fn key_path<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<Option<KeyPath>, SqlError> {
    object_store(executor, store_name)?
        .and_then(|store| store.key_path)
        .map(|key_path| decode_key_path(&key_path))
        .transpose()
}

/// <https://w3c.github.io/IndexedDB/#key-generator-current-number>, or `None` for a store without
/// a key generator.
pub(crate) fn key_generator_current_number<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<Option<i64>, SqlError> {
    Ok(object_store(executor, store_name)?
        .and_then(|store| (store.auto_increment != 0).then_some(store.auto_increment)))
}

pub(crate) fn indexes<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
) -> Result<Vec<IndexedDBIndex>, SqlError> {
    let store = required_object_store(executor, store_name)?;
    query_rows(
        executor,
        "SELECT name, key_path, unique_index, multi_entry_index FROM object_store_index \
         WHERE object_store_id = ? ORDER BY id",
        &[SqlParam::Integer(store.id)],
        |row| {
            Ok(IndexedDBIndex {
                name: row.text(0)?,
                key_path: decode_key_path(&row.blob(1)?)?,
                unique: row.integer(2)? != 0,
                multi_entry: row.integer(3)? != 0,
            })
        },
    )
}

pub(crate) fn create_index<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
    index_name: &str,
    key_path: &KeyPath,
    unique: bool,
    multi_entry: bool,
) -> Result<CreateObjectResult, SqlError> {
    let store = required_object_store(executor, store_name)?;
    let exists = query_row(
        executor,
        "SELECT 1 FROM object_store_index WHERE object_store_id = ? AND name = ?",
        &[SqlParam::Integer(store.id), SqlParam::Text(index_name)],
        |_| Ok(()),
    )?
    .is_some();
    if exists {
        return Ok(CreateObjectResult::AlreadyExists);
    }
    executor.execute(
        "INSERT INTO object_store_index \
         (object_store_id, name, key_path, unique_index, multi_entry_index) \
         VALUES (?, ?, ?, ?, ?)",
        &[
            SqlParam::Integer(store.id),
            SqlParam::Text(index_name),
            SqlParam::Blob(&encode_key_path(key_path)?),
            SqlParam::Integer(unique as i64),
            SqlParam::Integer(multi_entry as i64),
        ],
    )?;
    Ok(CreateObjectResult::Created)
}

/// Renaming an index that does not exist is not an error.
pub(crate) fn rename_index<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
    index_name: &str,
    new_name: &str,
) -> Result<(), SqlError> {
    let store = required_object_store(executor, store_name)?;
    executor.execute(
        "UPDATE object_store_index SET name = ? WHERE name = ? AND object_store_id = ?",
        &[
            SqlParam::Text(new_name),
            SqlParam::Text(index_name),
            SqlParam::Integer(store.id),
        ],
    )?;
    Ok(())
}

/// Deleting an index that does not exist is not an error.
pub(crate) fn delete_index<X: SqlExecutor>(
    executor: &mut X,
    store_name: &str,
    index_name: &str,
) -> Result<(), SqlError> {
    let store = required_object_store(executor, store_name)?;
    executor.execute(
        "DELETE FROM object_store_index WHERE name = ? AND object_store_id = ?",
        &[SqlParam::Text(index_name), SqlParam::Integer(store.id)],
    )?;
    Ok(())
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

/// The `key` conditions for a key range, to append to a `WHERE` clause, and the encoded bounds
/// to bind for their placeholders, in order.
#[derive(Default)]
struct RangeCondition {
    sql: String,
    bounds: Vec<Vec<u8>>,
}

impl RangeCondition {
    fn push(&mut self, sql: &str, bound: &IndexedDBKeyType) {
        self.sql.push_str(sql);
        self.bounds.push(encoding::serialize(bound));
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
        condition.push(" AND key = ?", singleton);
        return condition;
    }
    if let Some(lower) = range.lower.as_ref() {
        let sql = if range.lower_open {
            " AND key > ?"
        } else {
            " AND key >= ?"
        };
        condition.push(sql, lower);
    }
    if let Some(upper) = range.upper.as_ref() {
        let sql = if range.upper_open {
            " AND key < ?"
        } else {
            " AND key <= ?"
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
        condition.sql,
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
        condition.sql
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

/// <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store>: stores `value` under
/// `key`, or under a key from the store's key generator if `key` is `None`, which requires that
/// the store has one. A record that can't be stored changes nothing, not even the key generator.
pub(crate) fn put_record<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    key: Option<IndexedDBKeyType>,
    value: &[u8],
    overwrite: bool,
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
            // value.
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

    // Step 3. If a record already exists in store with its key equal to key, then remove the
    // record from store using delete records from an object store.
    // Step 4. Store a record in store containing key as its key and !
    // StructuredSerializeForStorage(value) as its value. The record is stored in the object
    // store’s list of records such that the list is sorted according to the key of the records in
    // ascending order.
    if exists {
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

pub(crate) fn delete_records<X: SqlExecutor>(
    executor: &mut X,
    store: &ObjectStore,
    range: &IndexedDBKeyRange,
) -> Result<(), SqlError> {
    let condition = range_condition(range);
    let sql = format!(
        "DELETE FROM object_data WHERE object_store_id = ?{}",
        condition.sql
    );
    let params = condition.params(store);
    executor.execute(&sql, &params)?;
    Ok(())
}

pub(crate) fn clear<X: SqlExecutor>(executor: &mut X, store: &ObjectStore) -> Result<(), SqlError> {
    executor.execute(
        "DELETE FROM object_data WHERE object_store_id = ?",
        &[SqlParam::Integer(store.id)],
    )?;
    Ok(())
}
