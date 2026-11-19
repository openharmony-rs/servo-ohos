// META: global=window,dedicatedworker,sharedworker
// META: title=IndexedDB: getAll(), getAllKeys() and getAllRecords() with IDBGetAllOptions
// META: timeout=long

// https://w3c.github.io/IndexedDB/#create-a-request-to-retrieve-multiple-items: getAll(),
// getAllKeys() and getAllRecords() of object stores and indexes take a query or an
// IDBGetAllOptions dictionary with a query, a count and a direction. These tests cover what the
// IndexedDB WPT tests leave out: every direction with counts and ranges on indexes whose keys have
// several records, records of stores with generated keys, how a dictionary is told apart from a
// query and the order of the exceptions, and IDBRecord's attributes.

'use strict';

// Opens a new database at version 1, with `upgrade` as its upgradeneeded handler.
function openDatabase(t, upgrade) {
  const name = 'get-all-options-' + Math.random();
  t.add_cleanup(() => { indexedDB.deleteDatabase(name); });
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => upgrade(request.result, request.transaction);
    request.onsuccess = () => {
      t.add_cleanup(() => { request.result.close(); });
      resolve(request.result);
    };
    request.onerror = () => reject(request.error);
  });
}

function result(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function assert_keys_equal(actual, expected, description) {
  assert_equals(actual.length, expected.length, description);
  actual.forEach((key, i) => assert_equals(indexedDB.cmp(key, expected[i]), 0, description));
}

// The [key, primaryKey, value] of the first `count` records (all for 0 or undefined) that a
// cursor over `source` with `query` and `direction` visits.
function visit(source, query, direction, count) {
  return new Promise((resolve, reject) => {
    const visited = [];
    const request = source.openCursor(query, direction);
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor || (count && visited.length == count)) {
        resolve(visited);
        return;
      }
      visited.push([cursor.key, cursor.primaryKey, cursor.value]);
      cursor.continue();
    };
    request.onerror = () => reject(request.error);
  });
}

// The records of 'items' under the keys 1 to 12: `k` is the key in the index 'k', with
// several records per key, `tags` those in the multiEntry index 'tags', and `u` the key in the
// unique index 'u'.
const ITEMS = [
  [1, 'b'], [2, 'a'], [3, 'b'], [4, 'a'], [5, 'c'], [6, 'b'],
  [7, 'd'], [8, 'c'], [9, 'a'], [10, 'e'], [11, 'b'], [12, 'd'],
];

function createItems(db) {
  const store = db.createObjectStore('items');
  store.createIndex('k', 'k');
  store.createIndex('tags', 'tags', {multiEntry: true});
  store.createIndex('u', 'u', {unique: true});
  // Put in an order that is neither the key order nor any index's.
  for (const [key, k] of [...ITEMS].sort((a, b) => (a[0] * 7) % 13 - (b[0] * 7) % 13)) {
    const tags = key % 3 == 0 ? [] : [k, key % 2 ? 'odd' : 'even', 'all'];
    store.put({key, k, tags, u: 100 - key}, key);
  }
}

const DIRECTIONS = ['next', 'nextunique', 'prev', 'prevunique'];
const COUNTS = [undefined, 0, 1, 2, 5, 100];

promise_test(async t => {
  const db = await openDatabase(t, createItems);
  const sources = {
    items: [null, 4, IDBKeyRange.bound(3, 9), IDBKeyRange.bound(3, 9, true, true),
            IDBKeyRange.lowerBound(10), IDBKeyRange.upperBound(2, true), 50],
    k: [null, 'b', IDBKeyRange.bound('a', 'c'), IDBKeyRange.bound('a', 'c', true, true),
        IDBKeyRange.lowerBound('b'), IDBKeyRange.upperBound('c', true), 'z'],
    tags: [null, 'all', 'odd', IDBKeyRange.bound('a', 'd'), IDBKeyRange.lowerBound('c', true)],
    u: [null, 95, IDBKeyRange.bound(90, 97), IDBKeyRange.upperBound(95, true)],
  };
  // Each step waits for its requests, so it reads in a new transaction.
  const sourceOf = name => {
    const store = db.transaction('items').objectStore('items');
    return name == 'items' ? store : store.index(name);
  };
  let checked = 0;
  for (const [name, queries] of Object.entries(sources)) {
    for (const direction of DIRECTIONS) {
      for (const query of queries) {
        for (const count of COUNTS) {
          const description = `${name} ${direction} ${JSON.stringify(query)} ${count}`;
          const options = {query, direction};
          if (count !== undefined) {
            options.count = count;
          }
          let source = sourceOf(name);
          const [expected, records, values, keys] = await Promise.all([
            visit(source, query, direction, count),
            result(source.getAllRecords(options)),
            result(source.getAll(options)),
            result(source.getAllKeys(options)),
          ]);
          assert_equals(records.length, expected.length, `records ${description}`);
          records.forEach((record, i) => {
            const [key, primaryKey, value] = expected[i];
            assert_true(record instanceof IDBRecord, description);
            assert_equals(indexedDB.cmp(record.key, key), 0, `key ${i} ${description}`);
            assert_equals(record.primaryKey, primaryKey, `primary key ${i} ${description}`);
            assert_equals(record.value.key, value.key, `value ${i} ${description}`);
          });
          assert_array_equals(
              values.map(value => value.key), expected.map(([, , value]) => value.key),
              `values ${description}`);
          assert_array_equals(
              keys, expected.map(([, primaryKey]) => primaryKey), `keys ${description}`);
          if (direction == 'next') {
            source = sourceOf(name);
            const [byArguments, keysByArguments] = await Promise.all([
              result(source.getAll(query, count)),
              result(source.getAllKeys(query, count)),
            ]);
            assert_array_equals(
                byArguments.map(value => value.key), values.map(value => value.key),
                `getAll(query, count) ${description}`);
            assert_array_equals(keysByArguments, keys, `getAllKeys(query, count) ${description}`);
          }
          checked++;
        }
      }
    }
  }
  assert_equals(checked, 4 * 6 * (7 + 7 + 5 + 4));
}, 'Every direction, count and query on a store and on indexes whose keys have several records ' +
   'gets the records that a cursor visits');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const flat = db.createObjectStore('flat', {keyPath: 'id', autoIncrement: true});
    flat.createIndex('name', 'name');
    flat.createIndex('id', 'id');
    flat.createIndex('pair', ['name', 'id']);
    const nested = db.createObjectStore('nested', {keyPath: 'a.b', autoIncrement: true});
    nested.createIndex('name', 'name');
    for (const name of ['x', 'y', 'x']) {
      flat.put({name});
      nested.put({name, a: {}});
    }
    flat.put({name: 'y', id: 10});
    flat.put({name: 'x'});
  });
  const tx = db.transaction(['flat', 'nested']);
  const flat = tx.objectStore('flat');
  const nested = tx.objectStore('nested');
  const [store, byName, byId, byPair, prevUnique, nestedRecords, nestedByName, values] =
      await Promise.all([
        result(flat.getAllRecords()),
        result(flat.index('name').getAllRecords()),
        result(flat.index('id').getAllRecords({direction: 'prev', count: 2})),
        result(flat.index('pair').getAllRecords({query: IDBKeyRange.bound(['x'], ['x', 3])})),
        result(flat.index('name').getAllRecords({direction: 'prevunique'})),
        result(nested.getAllRecords({direction: 'prev'})),
        result(nested.index('name').getAllRecords({query: 'x'})),
        result(flat.index('name').getAll({query: 'y', direction: 'prev'})),
      ]);
  const summary = records => records.map(
      record => [record.key, record.primaryKey, record.value.name, record.value.id]);
  assert_equals(JSON.stringify(summary(store)), JSON.stringify(
      [[1, 1, 'x', 1], [2, 2, 'y', 2], [3, 3, 'x', 3], [10, 10, 'y', 10], [11, 11, 'x', 11]]),
      'records of the store, with the generated keys in the values');
  assert_equals(JSON.stringify(summary(byName)), JSON.stringify(
      [['x', 1, 'x', 1], ['x', 3, 'x', 3], ['x', 11, 'x', 11], ['y', 2, 'y', 2],
       ['y', 10, 'y', 10]]),
      'records of an index, with the generated keys in the values');
  assert_equals(JSON.stringify(summary(byId)), JSON.stringify(
      [[11, 11, 'x', 11], [10, 10, 'y', 10]]),
      'an index on the key path of the generated keys');
  assert_equals(JSON.stringify(summary(byPair)), JSON.stringify(
      [[['x', 1], 1, 'x', 1], [['x', 3], 3, 'x', 3]]),
      'an index whose keys contain the generated keys');
  assert_equals(JSON.stringify(summary(prevUnique)), JSON.stringify(
      [['y', 2, 'y', 2], ['x', 1, 'x', 1]]),
      'prevunique takes the lowest primary key of each key');
  assert_equals(JSON.stringify(nestedRecords.map(
      record => [record.key, record.primaryKey, record.value.name, record.value.a.b])),
      JSON.stringify([[3, 3, 'x', 3], [2, 2, 'y', 2], [1, 1, 'x', 1]]),
      'records of a store with a nested key path');
  assert_equals(JSON.stringify(nestedByName.map(
      record => [record.key, record.primaryKey, record.value.a.b])),
      JSON.stringify([['x', 1, 1], ['x', 3, 3]]),
      'records of an index of a store with a nested key path');
  assert_equals(JSON.stringify(values), JSON.stringify(
      [{name: 'y', id: 10}, {name: 'y', id: 2}]), 'getAll() with options injects the keys');
}, 'Records and values of stores with generated keys have their keys');

promise_test(async t => {
  const buffer = new Uint8Array([1, 2]).buffer;
  const date = new Date(5);
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('keys');
    for (const key of [1, 2, 3, 'a', 'b', date, buffer, [1, 'a']]) {
      store.put(String(key), key);
    }
  });
  // Each read waits for its request, so it reads in a new transaction.
  const keys = () => db.transaction('keys').objectStore('keys');
  const get = (...args) => result(keys().getAll(...args));
  const getKeys = (...args) => result(keys().getAllKeys(...args));

  // A key or a key range is a query, also a Date, an ArrayBuffer, a view and an Array.
  assert_array_equals(await get(2), ['2'], 'a number');
  assert_array_equals(await get('b'), ['b'], 'a string');
  assert_array_equals(await get(new Date(5)), [String(date)], 'a Date');
  assert_array_equals(await get(new Uint8Array([1, 2]).buffer), [String(buffer)], 'an ArrayBuffer');
  assert_array_equals(await get(new Uint8Array([1, 2])), [String(buffer)], 'a view');
  assert_array_equals(await get([1, 'a']), ['1,a'], 'an Array');
  assert_array_equals(await get(IDBKeyRange.bound(2, 'a'), 2), ['2', '3'], 'a key range');
  // Undefined and null are unbounded key ranges, and the count argument counts.
  assert_array_equals(await getKeys(undefined, 2), [1, 2], 'undefined and a count');
  assert_array_equals(await getKeys(null, 2), [1, 2], 'null and a count');
  assert_equals((await getKeys()).length, 8, 'no arguments');

  // Anything else is an IDBGetAllOptions dictionary, whose count replaces the count argument.
  assert_array_equals(await getKeys({query: 'a', count: 1}), ['a'], 'a dictionary');
  const all = await getKeys();
  assert_keys_equal(await getKeys({}, 1), all, 'an empty dictionary and a count');
  assert_array_equals(await getKeys({count: 2}, 5), [1, 2], 'the count of the dictionary');
  assert_keys_equal(
      await getKeys({direction: 'prev'}, 1), [...all].reverse(),
      'a dictionary without a count gets every record');
  assert_array_equals(
      await getKeys(Object.assign(() => {}, {query: 3})), [3], 'a function is a dictionary');

  // A key type with an invalid key throws, as a query.
  const store = keys();
  assert_throws_dom('DataError', () => store.getAll(new Date(NaN)), 'an invalid Date');
  assert_throws_dom('DataError', () => store.getAll([{}]), 'an Array of non-keys');
  assert_throws_dom('DataError', () => store.getAll({query: [{}]}), 'an invalid query');
  // A value that is no key and no object can't be a dictionary.
  assert_throws_js(TypeError, () => store.getAll(true), 'a boolean');
  assert_throws_js(TypeError, () => store.getAllKeys(Symbol()), 'a symbol');
  assert_throws_js(TypeError, () => store.getAllRecords('a'), 'getAllRecords() with a string');
  // The dictionary's members convert in the order of their names.
  for (const [options, error] of [
           [{direction: 'sideways'}, TypeError], [{count: -1}, TypeError],
           [{count: 2 ** 32}, TypeError], [{count: NaN}, TypeError]]) {
    assert_throws_js(error, () => store.getAll(options), JSON.stringify(options));
    assert_throws_js(error, () => store.getAllRecords(options), JSON.stringify(options));
  }
  const read = [];
  const options = {};
  for (const member of ['query', 'direction', 'count']) {
    Object.defineProperty(options, member, {get() { read.push(member); }});
  }
  store.getAll(options);
  store.getAllRecords(options);
  assert_array_equals(read, ['count', 'direction', 'query', 'count', 'direction', 'query']);
}, 'getAll() and getAllKeys() take keys and key ranges as queries and other values as options');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('items').createIndex('k', 'k');
  });
  const store = db.transaction('items').objectStore('items');
  const index = store.index('k');
  await new Promise(resolve => t.step_timeout(resolve, 0));
  // The transaction is no longer active: getAll() throws before it reads the options.
  for (const source of [store, index]) {
    const read = [];
    const options = {get query() { read.push('query'); }};
    assert_throws_dom('TransactionInactiveError', () => source.getAll(options));
    assert_throws_dom('TransactionInactiveError', () => source.getAllKeys(options, 1));
    assert_array_equals(read, [], 'the options are not read');
    // getAllRecords() converts its dictionary first.
    assert_throws_dom('TransactionInactiveError', () => source.getAllRecords(options));
    assert_array_equals(read, ['query'], 'getAllRecords() reads its options');
    assert_throws_js(TypeError, () => source.getAllRecords({direction: 'up'}));
  }
}, 'getAll() checks its transaction before its options, getAllRecords() after');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    store.createIndex('date', 'date');
    store.put({date: new Date(7)}, [1, 'a']);
  });
  const store = db.transaction('items').objectStore('items');
  const [[fromStore], [fromIndex]] = await Promise.all([
    result(store.getAllRecords()),
    result(store.index('date').getAllRecords()),
  ]);
  assert_equals(String(fromStore), '[object IDBRecord]');
  assert_true(Array.isArray(fromStore.key), 'an Array key');
  assert_equals(fromStore.key, fromStore.key, 'key gives the same object every time');
  assert_equals(fromStore.primaryKey, fromStore.primaryKey, 'and so does primaryKey');
  assert_equals(fromStore.value, fromStore.value, 'and value');
  assert_equals(indexedDB.cmp(fromStore.key, fromStore.primaryKey), 0);
  assert_true(fromIndex.key instanceof Date, 'the index key');
  assert_equals(fromIndex.key.getTime(), 7);
  assert_array_equals(fromIndex.primaryKey, [1, 'a'], 'the primary key');
  assert_not_equals(fromIndex.value, fromStore.value, 'each record has its own value');
  assert_throws_js(TypeError, () => { fromIndex.key = 1; }, 'key is read-only');
  assert_throws_js(TypeError, () => { fromIndex.value = 1; }, 'value is read-only');
  assert_throws_js(TypeError, () => new IDBRecord(), 'IDBRecord has no constructor');
  fromStore.key.push('changed');
  fromStore.value.date = null;
  const [again] = await result(
      db.transaction('items').objectStore('items').getAllRecords());
  assert_array_equals(again.key, [1, 'a'], 'changing the key changes nothing stored');
  assert_equals(again.value.date.getTime(), 7, 'changing the value changes nothing stored');
}, 'IDBRecord keeps the key, primary key and value it was created with');
