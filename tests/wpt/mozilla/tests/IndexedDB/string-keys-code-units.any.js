// META: global=window,worker
// META: title=IndexedDB: string keys are their code units, unpaired surrogates included

// https://w3c.github.io/IndexedDB/#convert-a-value-to-a-key makes a string key of the string's
// code units, and https://w3c.github.io/IndexedDB/#compare-two-keys compares string keys by code
// unit. So unpaired surrogates survive a round trip, and keys that only differ in them are
// different keys.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'string-keys-code-units-' + Math.random();
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

const keys = ['\uDC00', '𝄞', '�', '\uD800', 'a\uDFFF', '\uDC00\uD800'];
const sortedKeys = ['a\uDFFF', '\uD800', '𝄞', '\uDC00', '\uDC00\uD800', '�'];

async function storeWithKeys(t) {
  const db = await openDatabase(t, db => db.createObjectStore('store'));
  const store = db.transaction('store', 'readwrite').objectStore('store');
  for (const [index, key] of keys.entries()) {
    assert_equals(await result(store.add(index, key)), key, 'add() returns the key');
  }
  return db;
}

promise_test(async t => {
  const db = await storeWithKeys(t);
  const store = db.transaction('store', 'readonly').objectStore('store');
  assert_array_equals(await result(store.getAllKeys()), sortedKeys);
  for (const [index, key] of keys.entries()) {
    assert_equals(await result(store.get(key)), index, `get() of key ${index}`);
  }
  assert_equals(await result(store.count('\uD800')), 1);
}, 'Keys with unpaired surrogates are distinct, read back unchanged and sorted by code unit');

promise_test(async t => {
  const db = await storeWithKeys(t);
  const store = db.transaction('store', 'readonly').objectStore('store');
  const range = IDBKeyRange.bound('\uD800', '\uDC00', true, false);
  assert_array_equals(await result(store.getAllKeys(range)), ['𝄞', '\uDC00']);
  const seen = [];
  const request = store.openCursor(null, 'prev');
  await new Promise((resolve, reject) => {
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) {
        return resolve();
      }
      seen.push(cursor.key);
      cursor.continue();
    };
    request.onerror = () => reject(request.error);
  });
  assert_array_equals(seen, sortedKeys.slice().reverse());
}, 'Key ranges and cursors order keys with unpaired surrogates by code unit');

promise_test(async t => {
  const db = await openDatabase(t, db => db.createObjectStore('store', {keyPath: 'id'}));
  const store = db.transaction('store', 'readwrite').objectStore('store');
  assert_equals(await result(store.put({id: '\uDC00\uD800'})), '\uDC00\uD800');
  assert_equals((await result(store.get('\uDC00\uD800'))).id, '\uDC00\uD800');
  assert_equals(await result(store.get('��')), undefined);
}, 'In-line keys with unpaired surrogates');

test(() => {
  assert_equals(indexedDB.cmp('\uD800', '�'), -1);
  assert_equals(indexedDB.cmp('𝄞', '�'), -1);
  assert_equals(indexedDB.cmp('\uD800', '\uDC00'), -1);
  assert_equals(indexedDB.cmp('\uDC00', '\uDC00'), 0);
  assert_equals(indexedDB.cmp(['\uDC00'], ['\uD800']), 1);
}, 'cmp() compares strings by code unit');
