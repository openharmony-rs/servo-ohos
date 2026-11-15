// META: global=window,worker
// META: title=IndexedDB: page code reached while computing index keys can't make requests

// https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store step 5 extracts the index
// keys from a clone in a user-agent defined realm, where no page code runs. Servo computes them in
// the page's realm when put(), add() or update() is called, where a getter on Array.prototype that
// a hole in a multiEntry array reaches does run. The transaction is inactive meanwhile, as it is
// while the value is cloned, so requests from such a getter throw a TransactionInactiveError
// instead of overtaking the request being made.

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

// An array with a hole at index 1 that the getter below acts on; the mark survives cloning.
function tagsWithHole(first, last) {
  const tags = [first, , last];
  tags.hole = true;
  return tags;
}

// Defines a getter of Array.prototype[1] that, for an array of `tagsWithHole`, tries the requests
// of `requests` against the transaction and returns the names of their exceptions, joined, through
// `errors()`, until the returned function removes it. Assignments to index 1 of other arrays still
// create their own property.
function arrayGetterMakingRequests(t, requests) {
  let names = '';
  Object.defineProperty(Array.prototype, 1, {
    configurable: true,
    get() {
      if (this.hole !== true) {
        return undefined;
      }
      for (const request of requests) {
        try {
          request();
          names += ' none';
        } catch (e) {
          names += ' ' + e.name;
        }
      }
      return undefined;
    },
    set(value) {
      Object.defineProperty(this, 1, {value, writable: true, enumerable: true, configurable: true});
    },
  });
  const remove = () => { delete Array.prototype[1]; };
  t.add_cleanup(remove);
  return [remove, () => names.trim()];
}

function createTagsStore(db) {
  db.createObjectStore('items').createIndex('tags', 'tags', {multiEntry: true});
}

promise_test(async t => {
  const db = await openDatabase(t, createTagsStore);
  const tx = db.transaction('items', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('items');
  const [remove, errors] = arrayGetterMakingRequests(t, [
    () => store.put({tags: ['inner']}, 'inner'),
    () => store.get('outer'),
    () => store.count(),
  ]);
  const put = store.put({tags: tagsWithHole('a', 'b')}, 'outer');
  remove();
  assert_equals(errors(), 'TransactionInactiveError TransactionInactiveError TransactionInactiveError');
  assert_equals(await result(put), 'outer');
  assert_array_equals(await result(store.getAllKeys()), ['outer']);
  await done;
}, 'Requests from a getter reached while put() computes index keys throw');

promise_test(async t => {
  const db = await openDatabase(t, createTagsStore);
  const setup = db.transaction('items', 'readwrite');
  setup.objectStore('items').put({tags: ['x']}, 'record');
  await complete(setup);

  const tx = db.transaction('items', 'readwrite');
  const done = complete(tx);
  const store = tx.objectStore('items');
  const cursor = await result(store.openCursor());
  const [remove, errors] = arrayGetterMakingRequests(t, [
    () => store.put({tags: ['inner']}, 'inner'),
    () => cursor.continue(),
  ]);
  const update = cursor.update({tags: tagsWithHole('c', 'd')});
  remove();
  assert_equals(errors(), 'TransactionInactiveError TransactionInactiveError');
  assert_equals(await result(update), 'record');
  assert_array_equals(await result(store.getAllKeys()), ['record']);
  await done;
}, 'Requests from a getter reached while update() computes index keys throw');
