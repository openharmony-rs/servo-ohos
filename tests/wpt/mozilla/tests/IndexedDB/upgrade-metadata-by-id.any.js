// META: global=window,worker
// META: title=IndexedDB: object stores and indexes keep their identity through renames and deletes

// The backend addresses object stores and indexes by id. A store or index that is deleted and
// created again under its name is a different one, and a rename moves the name, not the records.

'use strict';

function result(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

// Opens the database `name` at `version`, runs `upgrade(db, transaction)` in its upgrade
// transaction and resolves with the connection, or with the open request's error if the upgrade
// aborts.
function open(t, name, version, upgrade) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = t.step_func(() => upgrade(request.result, request.transaction));
    request.onsuccess = () => {
      t.add_cleanup(() => {
        request.result.close();
      });
      resolve(request.result);
    };
    request.onerror = event => {
      event.preventDefault();
      resolve(request.error);
    };
  });
}

function newDatabaseName(t) {
  const name = 'upgrade-metadata-by-id-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  return name;
}

promise_test(async t => {
  const name = newDatabaseName(t);
  let db = await open(t, name, 1, db => {
    db.createObjectStore('store').put('old', 1);
  });
  db.close();

  db = await open(t, name, 2, (db, transaction) => {
    const old = transaction.objectStore('store');
    db.deleteObjectStore('store');
    const fresh = db.createObjectStore('store');
    assert_throws_dom('InvalidStateError', () => old.put('stale', 2),
                      'The handle of the deleted store stays deleted');
    assert_equals(transaction.objectStore('store'), fresh,
                  'The name finds the new store');
    fresh.put('new', 2);
  });
  const store = db.transaction('store', 'readonly').objectStore('store');
  assert_array_equals(await result(store.getAll()), ['new']);
}, 'A store deleted and created again in one upgrade is a new, empty store');

promise_test(async t => {
  const name = newDatabaseName(t);
  let db = await open(t, name, 1, db => {
    db.createObjectStore('a').put('in a', 1);
    db.createObjectStore('b').put('in b', 1);
  });
  db.close();

  db = await open(t, name, 2, (db, transaction) => {
    const a = transaction.objectStore('a');
    const b = transaction.objectStore('b');
    a.name = 'tmp';
    b.name = 'a';
    a.name = 'b';
    assert_equals(transaction.objectStore('a'), b);
    assert_equals(transaction.objectStore('b'), a);
  });
  db.close();

  db = await open(t, name, 2, () => assert_unreached('No upgrade'));
  const transaction = db.transaction(['a', 'b'], 'readonly');
  assert_equals(await result(transaction.objectStore('a').get(1)), 'in b');
  assert_equals(await result(transaction.objectStore('b').get(1)), 'in a');
}, 'Swapped store names stay swapped and the records stay with their stores');

promise_test(async t => {
  const name = newDatabaseName(t);
  let db = await open(t, name, 1, db => {
    const store = db.createObjectStore('books');
    store.createIndex('by_title', 'title');
    store.put({title: 'Moby Dick'}, 1);
  });
  db.close();

  let store, index;
  const error = await open(t, name, 2, (db, transaction) => {
    store = transaction.objectStore('books');
    index = store.index('by_title');
    store.name = 'renamed';
    index.name = 'renamed_index';
    transaction.abort();
    assert_equals(store.name, 'books', 'The store name reverts at once');
    assert_equals(index.name, 'by_title', 'The index name reverts at once');
    assert_array_equals(store.indexNames, ['by_title']);
    assert_array_equals(db.objectStoreNames, ['books']);
  });
  assert_equals(error.name, 'AbortError');

  db = await open(t, name, 1, () => assert_unreached('No upgrade'));
  const reopened = db.transaction('books', 'readonly').objectStore('books');
  assert_array_equals(reopened.indexNames, ['by_title']);
  assert_equals((await result(reopened.get(1))).title, 'Moby Dick');
}, 'An aborted upgrade reverts store and index renames, and the records stay');

promise_test(async t => {
  const name = newDatabaseName(t);
  const db = await open(t, name, 1, db => {
    const store = db.createObjectStore('store');
    const old = store.createIndex('index', 'a');
    store.deleteIndex('index');
    const fresh = store.createIndex('index', 'b');
    assert_throws_dom('InvalidStateError', () => { old.name = 'other'; },
                      'The handle of the deleted index stays deleted');
    assert_equals(store.index('index'), fresh, 'The name finds the new index');
    assert_equals(fresh.keyPath, 'b');
  });
  const store = db.transaction('store', 'readonly').objectStore('store');
  assert_array_equals(store.indexNames, ['index']);
  assert_equals(store.index('index').keyPath, 'b');
}, 'An index deleted and created again in one upgrade is a new index');
