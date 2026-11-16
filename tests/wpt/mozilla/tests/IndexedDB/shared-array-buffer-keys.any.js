// META: global=window,dedicatedworker,sharedworker
// META: title=IndexedDB: SharedArrayBuffers and views on them as keys

// https://w3c.github.io/IndexedDB/#convert-a-value-to-a-key: a buffer source, including a
// SharedArrayBuffer and a view on one, converts to a binary key with a copy of its bytes. The
// upstream test (sharedarraybuffer-key.https.html) also needs cross-origin isolation, which Servo
// doesn't implement: this test's metadata sets the pref js_shared_memory_enabled_for_testing,
// which gives every global a SharedArrayBuffer.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'shared-array-buffer-keys-' + Math.random();
  t.add_cleanup(() => { indexedDB.deleteDatabase(name); });
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => upgrade(request.result);
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

function complete(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
  });
}

// A SharedArrayBuffer of `bytes.length + 2` bytes with `bytes` at offset 1.
function sharedBuffer(bytes) {
  const buffer = new SharedArrayBuffer(bytes.length + 2);
  new Uint8Array(buffer).set(bytes, 1);
  return buffer;
}

function bytesOf(arrayBuffer) {
  assert_true(arrayBuffer instanceof ArrayBuffer, 'a binary key reads back as an ArrayBuffer');
  return [...new Uint8Array(arrayBuffer)];
}

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store');
  });
  const tx = db.transaction('store', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('store');

  const whole = sharedBuffer([1, 2, 3]);
  const view = new Uint8Array(sharedBuffer([4, 5, 6]), 1, 3);
  const dataView = new DataView(sharedBuffer([7, 8, 9]), 1, 3);
  const array = [sharedBuffer([10])];
  const puts = [
    store.put('whole', whole),
    store.put('view', view),
    store.put('data view', dataView),
    store.put('array', array),
  ];
  new Uint8Array(whole).fill(0);
  new Uint8Array(view.buffer).fill(0);
  new Uint8Array(dataView.buffer).fill(0);
  new Uint8Array(array[0]).fill(0);

  assert_array_equals(bytesOf(await result(puts[0])), [0, 1, 2, 3, 0]);
  assert_array_equals(bytesOf(await result(puts[1])), [4, 5, 6]);
  assert_array_equals(bytesOf(await result(puts[2])), [7, 8, 9]);
  const arrayKey = await result(puts[3]);
  assert_array_equals(bytesOf(arrayKey[0]), [0, 10, 0]);

  assert_equals(await result(store.get(new Uint8Array([0, 1, 2, 3, 0]))), 'whole');
  assert_equals(await result(store.get(new Uint8Array([4, 5, 6]).buffer)), 'view');
  assert_equals(await result(store.get(new Uint8Array([7, 8, 9]))), 'data view');
  assert_equals(await result(store.get([new Uint8Array([0, 10, 0])])), 'array');
  await done;
}, 'SharedArrayBuffers, views on them and Arrays of them are keys with a copy of the bytes');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('store');
    for (let i = 1; i <= 4; i++) {
      store.put(i, new Uint8Array([i]));
    }
  });
  const store = db.transaction('store').objectStore('store');
  const shared = n => new Uint8Array(sharedBuffer([n]), 1, 1);
  assert_equals(indexedDB.cmp(sharedBuffer([1, 2]), new Uint8Array([0, 1, 2, 0])), 0);
  assert_equals(indexedDB.cmp(shared(2), new Uint8Array([1])), 1);
  assert_array_equals(await result(store.getAll(shared(2))), [2],
                      'getAll() takes a view on a SharedArrayBuffer as a query');
  assert_array_equals(await result(store.getAll(IDBKeyRange.bound(shared(2), shared(3)))), [2, 3]);
  assert_array_equals(await result(store.getAll(IDBKeyRange.only(shared(4)))), [4]);
  assert_equals(await result(store.count(shared(1))), 1);
  const cursor = await result(store.openCursor(IDBKeyRange.lowerBound(shared(3), true)));
  assert_equals(cursor.value, 4);
}, 'SharedArrayBuffers and views on them are keys for queries and key ranges');
