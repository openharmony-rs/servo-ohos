// META: global=window,dedicatedworker,sharedworker
// META: title=IndexedDB: values holding shared memory can't be stored

// https://w3c.github.io/IndexedDB/#clone serializes values with StructuredSerializeForStorage, which
// throws a DataCloneError for a SharedArrayBuffer, also inside a view or another value
// (https://html.spec.whatwg.org/multipage/structured-data.html#structuredserializeinternal). The
// upstream tests (serialize-sharedarraybuffer-throws.https.html, html/.../serialization-via-idb)
// also need cross-origin isolation, which Servo doesn't implement: this test's metadata sets the
// pref js_shared_memory_enabled_for_testing, which gives every global a SharedArrayBuffer.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'shared-array-buffer-values-' + Math.random();
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

function sharedValues() {
  const buffer = new SharedArrayBuffer(8);
  return [
    ['a SharedArrayBuffer', buffer],
    ['a Uint8Array on a SharedArrayBuffer', new Uint8Array(buffer, 2, 4)],
    ['a DataView on a SharedArrayBuffer', new DataView(buffer)],
    ['an object holding a SharedArrayBuffer', {buffer}],
    ['an Array holding a view on a SharedArrayBuffer', [1, new Int32Array(buffer)]],
    ['a Map holding a SharedArrayBuffer', new Map([['buffer', buffer]])],
  ];
}

test(() => {
  assert_equals(typeof SharedArrayBuffer, 'function', 'the pref gives the global SharedArrayBuffer');
}, 'SharedArrayBuffer is exposed with js_shared_memory_enabled_for_testing');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('out-of-line');
    db.createObjectStore('in-line', {keyPath: 'id'});
  });
  const tx = db.transaction(['out-of-line', 'in-line'], 'readwrite');
  const done = complete(tx);
  const outOfLine = tx.objectStore('out-of-line');
  const inLine = tx.objectStore('in-line');
  for (const [description, value] of sharedValues()) {
    assert_throws_dom('DataCloneError', () => outOfLine.put(value, 1), `put() of ${description}`);
    assert_throws_dom('DataCloneError', () => outOfLine.add(value, 1), `add() of ${description}`);
    assert_throws_dom('DataCloneError', () => inLine.put({id: 1, value}),
                      `put() of ${description} with an in-line key`);
  }
  assert_throws_dom('DataCloneError', () => inLine.put({value: new SharedArrayBuffer(1)}),
                    'cloning comes before extracting the key, which would throw a DataError');
  assert_equals(await result(outOfLine.put(new ArrayBuffer(8), 1)), 1,
                'the transaction goes on after the exceptions');
  assert_equals(await result(outOfLine.count()), 1);
  assert_equals(await result(inLine.count()), 0);
  await done;
}, 'put() and add() throw a DataCloneError for values holding shared memory');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store');
  });
  const setup = db.transaction('store', 'readwrite');
  setup.objectStore('store').put('old', 1);
  await complete(setup);

  const tx = db.transaction('store', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('store');
  const cursor = await result(store.openCursor());
  for (const [description, value] of sharedValues()) {
    assert_throws_dom('DataCloneError', () => cursor.update(value), `update() with ${description}`);
  }
  assert_equals(await result(store.get(1)), 'old');
  await done;
}, 'update() throws a DataCloneError for values holding shared memory');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store');
  });
  const tx = db.transaction('store', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('store');
  let before = false;
  let after = false;
  assert_throws_dom('DataCloneError', () => {
    store.put([
      {get x() { before = true; return 1; }},
      new SharedArrayBuffer(4),
      {get x() { after = true; return 2; }},
    ], 1);
  });
  assert_true(before, 'the getter before the SharedArrayBuffer is called');
  assert_false(after, 'the getter after the SharedArrayBuffer is not called');
  assert_equals(await result(store.count()), 0);
  await done;
}, 'Serializing stops at the SharedArrayBuffer');
