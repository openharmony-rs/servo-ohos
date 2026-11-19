// META: global=window,worker
// META: title=IndexedDB: computing keys from a clone runs no page code

// https://w3c.github.io/IndexedDB/#add-or-put and https://w3c.github.io/IndexedDB/#dom-idbcursor-update
// clone the value into a user-agent defined realm and extract the keys from that clone, so no page
// code runs while the keys are computed, and properties that the page defines on prototypes don't
// reach the keys. Only serializing the value, while the transaction is inactive, runs page code.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'index-keys-page-code-' + Math.random();
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

function complete(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
  });
}

// Defines `name` on `prototype` as a getter that counts its calls and returns `value`, until
// `remove()` (or the end of the test). Assignments create an own property of the target. Returns
// `{calls, remove}`, where `calls.count` is the number of calls.
function prototypeGetter(t, prototype, name, value) {
  const calls = {count: 0};
  const previous = Object.getOwnPropertyDescriptor(prototype, name);
  Object.defineProperty(prototype, name, {
    configurable: true,
    get() {
      calls.count++;
      return value;
    },
    set(v) {
      Object.defineProperty(this, name, {value: v, writable: true, enumerable: true, configurable: true});
    },
  });
  const remove = () => {
    if (previous) {
      Object.defineProperty(prototype, name, previous);
    } else {
      delete prototype[name];
    }
  };
  t.add_cleanup(remove);
  return {calls, remove};
}

// Defines the getters of `prototypeGetter` for `[prototype, name]` pairs, runs `f` and removes
// them; returns `f`'s result and the call counts.
function withPrototypeGetters(t, getters, f) {
  const defined = getters.map(([prototype, name]) =>
    prototypeGetter(t, prototype, name, 'from-prototype'));
  let returned;
  try {
    returned = f();
  } finally {
    defined.forEach(getter => getter.remove());
  }
  return [returned, defined.map(getter => getter.calls.count)];
}

// A store `items` with in-line keys from `id` and a key generator, a multiEntry index on `tags` and
// an index on `missing`, which no value has.
function createItemsStore(db) {
  const store = db.createObjectStore('items', {keyPath: 'id', autoIncrement: true});
  store.createIndex('tags', 'tags', {multiEntry: true});
  store.createIndex('missing', 'missing');
}

async function indexKeys(store) {
  return {
    tags: await result(store.index('tags').getAllKeys()),
    missing: await result(store.index('missing').count()),
  };
}

const PROTOTYPE_GETTERS = [[Array.prototype, 1], [Object.prototype, 'id'], [Object.prototype, 'missing']];

promise_test(async t => {
  const db = await openDatabase(t, createItemsStore);
  const tx = db.transaction('items', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('items');
  const [put, calls] =
    withPrototypeGetters(t, PROTOTYPE_GETTERS, () => store.put({tags: ['a', , 'b']}));
  assert_array_equals(calls, [0, 0, 0], 'no getter runs while put() computes the keys');
  assert_equals(await result(put), 1, 'the key is generated, not read from Object.prototype');
  const keys = await indexKeys(store);
  assert_array_equals(keys.tags, [1, 1], 'the hole gives no index key');
  assert_equals(keys.missing, 0, 'Object.prototype gives no index key');
  await done;
}, 'put() reads neither the prototypes of Arrays nor those of Objects for keys');

promise_test(async t => {
  const db = await openDatabase(t, createItemsStore);
  const setup = db.transaction('items', 'readwrite');
  setup.objectStore('items').put({tags: ['x']});
  await complete(setup);

  const tx = db.transaction('items', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('items');
  const cursor = await result(store.openCursor());
  const [update, calls] =
    withPrototypeGetters(t, PROTOTYPE_GETTERS, () => cursor.update({id: 1, tags: ['c', , 'd']}));
  assert_array_equals(calls, [0, 0, 0], 'no getter runs while update() computes the keys');
  assert_equals(await result(update), 1);
  const keys = await indexKeys(store);
  assert_array_equals(keys.tags, [1, 1], 'the hole gives no index key');
  assert_equals(keys.missing, 0, 'Object.prototype gives no index key');
  await done;
}, 'update() reads neither the prototypes of Arrays nor those of Objects for keys');

for (const method of ['put', 'update']) {
  promise_test(async t => {
    const db = await openDatabase(t, db => {
      db.createObjectStore('errors');
    });
    if (method === 'update') {
      const setup = db.transaction('errors', 'readwrite');
      setup.objectStore('errors').put('old', 'record');
      await complete(setup);
    }
    const tx = db.transaction('errors', 'readwrite');
    const done = complete(tx);
    const store = tx.objectStore('errors');
    const cursor = method === 'update' ? await result(store.openCursor()) : null;
    const names = [];
    const previous = Object.getOwnPropertyDescriptor(Error.prototype, 'name');
    Object.defineProperty(Error.prototype, 'name', {
      configurable: true,
      get() {
        try {
          store.put('inner', 'inner');
          names.push('none');
        } catch (e) {
          names.push(e.name);
        }
        return 'Error';
      },
    });
    t.add_cleanup(() => { Object.defineProperty(Error.prototype, 'name', previous); });
    const value = {error: new Error('message')};
    const request = method === 'put' ? store.put(value, 'record') : cursor.update(value);
    Object.defineProperty(Error.prototype, 'name', previous);
    assert_array_equals(names, ['TransactionInactiveError'],
                        'the getter runs once, while the value is serialized');
    assert_equals(await result(request), 'record');
    const stored = await result(store.get('record'));
    assert_equals(stored.error.constructor, Error);
    assert_equals(stored.error.message, 'message');
    assert_array_equals(await result(store.getAllKeys()), ['record']);
    await done;
  }, `${method}() serializes the value once, with the transaction inactive`);
}
