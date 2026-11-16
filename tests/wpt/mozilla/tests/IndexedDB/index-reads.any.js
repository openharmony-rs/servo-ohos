// META: global=window,worker
// META: title=IndexedDB: reading the records of an index
// META: timeout=long

// https://w3c.github.io/IndexedDB/#index-interface: get(), getKey(), getAll(), getAllKeys() and
// count() read an index's records, which are sorted by key and then by value, the primary key of
// the referenced record (https://w3c.github.io/IndexedDB/#index-retrieval-operation). These tests
// cover what the IndexedDB WPT tests leave out: keys of every type, reads right after
// createIndex, generated keys in the values read, and writes earlier in the same transaction.

'use strict';

// Opens a new database at version 1, with `upgrade` as its upgradeneeded handler.
function openDatabase(t, upgrade) {
  const name = 'index-reads-' + Math.random();
  t.add_cleanup(() => { indexedDB.deleteDatabase(name); });
  return reopen(name, 1, upgrade);
}

// Opens the database `name` at `version`, with `upgrade` as its upgradeneeded handler.
function reopen(name, version, upgrade) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = () => upgrade(request.result, request.transaction);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function result(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function complete(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
  });
}

// More records than the backend reads in one step of a population.
const MANY = 1234;

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    for (let i = 0; i < MANY; i++) {
      store.add({n: i, parity: i % 2}, i);
    }
  });
  db.close();

  let reads;
  const upgraded = await reopen(db.name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    const index = store.createIndex('parity', 'parity');
    reads = Promise.all([
      result(index.count()),
      result(index.count(1)),
      result(index.getAllKeys(1, 3)),
      result(index.getAll(0)),
      result(index.get(1)),
      result(index.getKey(0)),
      result(store.add({n: -1, parity: 1}, -1)).then(() => result(index.getKey(1))),
    ]);
  });
  t.add_cleanup(() => { upgraded.close(); });
  const [count, odd, firstOdd, even, get, getKey, added] = await reads;
  assert_equals(count, MANY, 'every record is in the index');
  assert_equals(odd, MANY / 2);
  assert_array_equals(firstOdd, [1, 3, 5]);
  assert_equals(even.length, MANY / 2);
  assert_array_equals(even.map(value => value.n), [...Array(MANY / 2).keys()].map(i => 2 * i));
  assert_equals(get.n, 1);
  assert_equals(getKey, 0);
  assert_equals(added, -1, 'a put after createIndex is in the index');
}, 'Reads of an index right after createIndex see the records of the whole store');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items', {keyPath: 'id', autoIncrement: true});
    store.createIndex('x', 'x');
    store.createIndex('id', 'id');
    store.put({x: 'b'});
    store.put({x: 'a'});
    store.put({x: 'a', id: 10});
    store.put({x: 'a'});
    const nested = db.createObjectStore('nested', {keyPath: 'a.id', autoIncrement: true});
    nested.createIndex('x', 'x');
    nested.put({a: {}, x: 1});
    db.createObjectStore('later', {keyPath: 'id', autoIncrement: true}).put({y: 'z'});
  });
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction(['items', 'nested'], 'readonly');
  const index = tx.objectStore('items').index('x');
  const byId = tx.objectStore('items').index('id');
  const [first, all, keys, byIdValue, byIdKeys, nested] = await Promise.all([
    result(index.get('a')),
    result(index.getAll()),
    result(index.getAllKeys()),
    result(byId.get(11)),
    result(byId.getAllKeys()),
    result(tx.objectStore('nested').index('x').get(1)),
  ]);
  assert_object_equals(first, {x: 'a', id: 2}, 'the generated key is in the value');
  assert_array_equals(all.map(value => value.id), [2, 10, 11, 1]);
  assert_array_equals(keys, [2, 10, 11, 1]);
  assert_object_equals(byIdValue, {x: 'a', id: 11}, 'an index on the key path has the key');
  assert_array_equals(byIdKeys, [1, 2, 10, 11]);
  assert_object_equals(nested, {a: {id: 1}, x: 1}, 'the key is injected below the top');
  db.close();

  let later;
  const upgraded = await reopen(db.name, 2, (db, tx) => {
    const index = tx.objectStore('later').createIndex('id', 'id');
    later = Promise.all([result(index.get(1)), result(index.getAllKeys())]);
  });
  t.add_cleanup(() => { upgraded.close(); });
  const [value, laterKeys] = await later;
  assert_object_equals(value, {y: 'z', id: 1}, 'a populated index has the generated key');
  assert_array_equals(laterKeys, [1]);
}, 'Values read through an index have their generated keys');

// Index keys of every type, in key order, each of them the key of two records.
const KEYS = [
  -Infinity, -1.5, 0, Infinity,
  new Date(-1), new Date(0),
  '', 'A', '\uD800', '￿',
  new ArrayBuffer(0), new Uint8Array([0, 255]),
  [], [1, []],
];

promise_test(async t => {
  // Primary keys sort like the records' positions in the index: 'p00a', 'p00b', 'p01a', ...
  const label = (i, copy) => 'p' + String(i).padStart(2, '0') + copy;
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    store.createIndex('k', 'k');
    store.createIndex('unique', 'u', {unique: true});
    for (let i = KEYS.length - 1; i >= 0; i--) {
      store.put({k: KEYS[i], u: i, i}, label(i, 'b'));
      store.put({k: KEYS[i], i}, label(i, 'a'));
    }
  });
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction('items', 'readonly');
  const index = tx.objectStore('items').index('k');
  const unique = tx.objectStore('items').index('unique');
  const labels = (from, to) =>
      KEYS.slice(from, to).flatMap((_, i) => [label(from + i, 'a'), label(from + i, 'b')]);
  const range = IDBKeyRange.bound(new Date(-1), 'A', false, true);
  const [keys, values, inRange, inRangeCount, first, firstKey, limited, arrays, uniqueKeys] =
      await Promise.all([
        result(index.getAllKeys()),
        result(index.getAll()),
        result(index.getAllKeys(range)),
        result(index.count(range)),
        result(index.get(range)),
        result(index.getKey('\uD800')),
        result(index.getAllKeys(IDBKeyRange.lowerBound(new ArrayBuffer(0)), 3)),
        result(index.getAllKeys([])),
        result(unique.getAllKeys(IDBKeyRange.bound(1, 3))),
      ]);
  assert_array_equals(keys, labels(0, KEYS.length));
  assert_array_equals(values.map(value => value.i), KEYS.flatMap((_, i) => [i, i]));
  assert_array_equals(inRange, labels(4, 7), 'a range from dates to strings');
  assert_equals(inRangeCount, 6);
  assert_equals(first.i, 4);
  assert_equals(firstKey, label(8, 'a'), 'a lone surrogate is a key of its own');
  assert_array_equals(limited, [label(10, 'a'), label(10, 'b'), label(11, 'a')]);
  assert_array_equals(arrays, labels(12, 13));
  assert_array_equals(uniqueKeys, [label(1, 'b'), label(2, 'b'), label(3, 'b')]);
}, 'Index keys of every type are read in key order, and records with the same index key in ' +
   'primary key order');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    store.createIndex('tags', 'tags', {multiEntry: true});
    store.put({tags: ['b', 'a', 'a', ['c']]}, 2);
    store.put({tags: ['a', null]}, 1);
    store.put({tags: 'b'}, 3);
  });
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction('items', 'readonly');
  const index = tx.objectStore('items').index('tags');
  const [all, count, a, nested, firstB] = await Promise.all([
    result(index.getAllKeys()),
    result(index.count()),
    result(index.getAllKeys('a')),
    result(index.getKey(['c'])),
    result(index.get('b')),
  ]);
  assert_array_equals(all, [1, 2, 2, 3, 2], 'a, b and then the array key');
  assert_equals(count, 5);
  assert_array_equals(a, [1, 2], 'a subkey that appears twice gives one record');
  assert_equals(nested, 2);
  assert_equals(JSON.stringify(firstB.tags), '["b","a","a",["c"]]', 'the first record of b');
}, 'A multiEntry index has a record per distinct subkey');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('items').createIndex('name', 'name');
  });
  t.add_cleanup(() => { db.close(); });
  let tx = db.transaction('items', 'readwrite');
  let store = tx.objectStore('items');
  let index = store.index('name');
  store.put({name: 'ann'}, 1);
  store.put({name: 'bob'}, 2);
  store.put({name: 'ann'}, 3);
  const afterPuts = result(index.getAllKeys('ann'));
  store.put({name: 'cid'}, 1);
  const afterOverwrite = result(index.getAll());
  store.delete(3);
  const afterDelete = result(index.get('ann'));
  const deletedCount = result(index.count('ann'));
  store.clear();
  const afterClear = result(index.count());
  store.put({name: 'dan'}, 4);
  await complete(tx);
  assert_array_equals(await afterPuts, [1, 3]);
  assert_array_equals((await afterOverwrite).map(value => value.name), ['ann', 'bob', 'cid']);
  assert_equals(await afterDelete, undefined);
  assert_equals(await deletedCount, 0);
  assert_equals(await afterClear, 0);

  tx = db.transaction('items', 'readwrite');
  store = tx.objectStore('items');
  store.put({name: 'eve'}, 5);
  tx.abort();
  await new Promise(resolve => { tx.onabort = resolve; });
  tx = db.transaction('items', 'readonly');
  index = tx.objectStore('items').index('name');
  assert_array_equals(await result(index.getAllKeys()), [4], 'the aborted put is not there');
}, 'Index reads see the writes before them in their transaction, and no aborted writes');

promise_test(async t => {
  let old, reads;
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    store.put({a: 'x', b: 'y'}, 1);
    old = store.createIndex('i', 'a');
    store.deleteIndex('i');
    const index = store.createIndex('i', 'b');
    reads = Promise.all([result(index.getAllKeys('y')), result(index.count('x'))]);
  });
  t.add_cleanup(() => { db.close(); });
  const [keys, oldKeys] = await reads;
  assert_array_equals(keys, [1]);
  assert_equals(oldKeys, 0, 'the index has no records of the deleted one');
  for (const method of ['get', 'getKey', 'getAll', 'getAllKeys', 'count']) {
    assert_throws_dom('InvalidStateError', () => old[method](1), method);
  }
}, 'An index deleted and created again with its name in one upgrade starts afresh');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    store.createIndex('n', 'n');
    for (let i = 0; i < 5; i++) {
      store.put({n: i}, i);
    }
  });
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction('items', 'readonly');
  const store = tx.objectStore('items');
  const index = store.index('n');
  const counts = await Promise.all([
    result(store.getAll(undefined, 0)),
    result(store.getAllKeys(null, 0)),
    result(index.getAll(undefined, 0)),
    result(index.getAllKeys(null, 0)),
    result(index.getAllKeys(null, 2)),
  ]);
  assert_array_equals(counts.map(values => values.length), [5, 5, 5, 5, 2]);
}, 'getAll() and getAllKeys() with a count of 0 get every record');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const s = db.createObjectStore('s', {keyPath: 'id', autoIncrement: true});
    s.createIndex('pair', ['x', 'id']);
    s.createIndex('ids', 'id', {multiEntry: true});
    s.createIndex('below', 'id.sub');
    const n = db.createObjectStore('n', {keyPath: 'a.id', autoIncrement: true});
    n.createIndex('a', 'a');
    n.createIndex('pair', ['a.id', 'x']);
    n.createIndex('aid', 'a.id');
    n.createIndex('alen', 'a.length');
  });
  t.add_cleanup(() => { db.close(); });
  const writes = db.transaction(['s', 'n'], 'readwrite');
  writes.objectStore('s').put({x: 'b'});
  writes.objectStore('s').put({x: 'a'});
  writes.objectStore('s').put({x: 'a', id: 10});
  writes.objectStore('n').put({x: 1});
  writes.objectStore('n').put({a: {}, x: 2});
  writes.objectStore('n').put({a: [7], x: 3});
  await complete(writes);

  const tx = db.transaction(['s', 'n']);
  const s = tx.objectStore('s');
  const n = tx.objectStore('n');
  // An item of a sequence key path that is the store's key path gets the generated key.
  assert_array_equals(await result(s.index('pair').getAllKeys()), [2, 10, 1]);
  assert_equals(await result(s.index('pair').getKey(['a', 2])), 2);
  assert_equals(await result(s.index('pair').getKey(['b', 1])), 1);
  // A multiEntry index on the store's key path has the generated key; below it, there is none.
  assert_array_equals(await result(s.index('ids').getAllKeys()), [1, 2, 10]);
  assert_equals(await result(s.index('below').count()), 0);
  // With a nested store key path, the key is injected into the object on the path, or into one
  // created for it.
  assert_array_equals(await result(n.index('aid').getAllKeys()), [1, 2, 3]);
  assert_equals(await result(n.index('pair').getKey([1, 1])), 1);
  assert_equals(await result(n.index('pair').getKey([2, 2])), 2);
  assert_equals(await result(n.index('pair').getKey([3, 3])), 3);
  // `a` is an invalid key when it is an object, and an array key when it is the array [7].
  assert_equals(await result(n.index('a').count()), 1);
  assert_equals(await result(n.index('a').getKey([7])), 3);
  assert_array_equals(await result(n.index('alen').getAllKeys(1)), [3]);
  assert_equals(await result(n.index('alen').count()), 1);
  const values = await result(n.getAll());
  assert_object_equals(values[0], {x: 1, a: {id: 1}});
  assert_object_equals(values[1], {a: {id: 2}, x: 2});
  assert_true(Array.isArray(values[2].a));
  assert_equals(values[2].a.id, 3);
}, 'Index keys that come from a generated key, read back through the indexes');
