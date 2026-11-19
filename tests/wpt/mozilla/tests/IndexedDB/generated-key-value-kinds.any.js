// META: global=window,worker
// META: title=IndexedDB: a generated in-line key is part of the value only where storing keeps it

// https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store injects a generated key
// into the value (step 1.1.3) before StructuredSerializeForStorage (step 4), which keeps the own
// properties of ordinary objects and Arrays only. So a value of another kind, or with such an
// object where the key would attach, reads back without the key.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'generated-key-value-kinds-' + Math.random();
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => upgrade(request.result);
    request.onsuccess = () => {
      t.add_cleanup(() => {
        request.result.close();
        indexedDB.deleteDatabase(name);
      });
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

// Puts `value` into `storeName` under a generated key, and reads it back with get() and with a
// cursor.
async function putAndRead(db, storeName, value) {
  const tx = db.transaction(storeName, 'readwrite');
  const store = tx.objectStore(storeName);
  const key = await result(store.put(value));
  const got = await result(store.get(key));
  const cursor = await result(store.openCursor(key));
  return {key, got, fromCursor: cursor.value};
}

const exotic = {
  Date: [() => new Date(0), v => v instanceof Date],
  Map: [() => new Map([[1, 2]]), v => v instanceof Map],
  Set: [() => new Set([1]), v => v instanceof Set],
  Blob: [() => new Blob(['x']), v => v instanceof Blob],
  File: [() => new File(['x'], 'x.txt'), v => v instanceof File],
  ArrayBuffer: [() => new ArrayBuffer(4), v => v instanceof ArrayBuffer],
  Uint8Array: [() => new Uint8Array(2), v => v instanceof Uint8Array],
  RegExp: [() => /x/, v => v instanceof RegExp],
  'Boolean object': [() => new Boolean(true), v => v instanceof Boolean],
};

for (const [kind, [make, isKind]] of Object.entries(exotic)) {
  promise_test(async t => {
    const db = await openDatabase(t, db => {
      db.createObjectStore('store', {keyPath: 'id', autoIncrement: true});
    });
    const {got, fromCursor} = await putAndRead(db, 'store', make());
    for (const value of [got, fromCursor]) {
      assert_true(isKind(value), 'the value keeps its kind');
      assert_false(Object.prototype.hasOwnProperty.call(value, 'id'),
                   'the generated key is not part of the value');
    }
  }, `A ${kind} value doesn't get its generated key`);
}

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store', {keyPath: 'id', autoIncrement: true});
  });
  for (const make of [() => ({}), () => [1, 2]]) {
    const {key, got, fromCursor} = await putAndRead(db, 'store', make());
    assert_equals(got.id, key, 'get() returns the value with its key');
    assert_equals(fromCursor.id, key, 'a cursor returns the value with its key');
  }
}, 'Ordinary objects and Arrays get their generated key');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store', {keyPath: 'a.id', autoIncrement: true});
  });
  let {got} = await putAndRead(db, 'store', {a: new Date(0)});
  assert_true(got.a instanceof Date, 'the nested value keeps its kind');
  assert_false(Object.prototype.hasOwnProperty.call(got.a, 'id'),
               'the generated key is not part of a nested Date');

  let key;
  ({key, got} = await putAndRead(db, 'store', {a: {}}));
  assert_equals(got.a.id, key, 'a nested ordinary object gets the key');

  ({key, got} = await putAndRead(db, 'store', {}));
  assert_equals(got.a.id, key, 'the objects on the key path are created');

  ({key, got} = await putAndRead(db, 'store', {a: [1]}));
  assert_equals(got.a.id, key, 'a nested Array gets the key');
}, 'A generated key in a nested key path attaches to ordinary objects and Arrays only');
