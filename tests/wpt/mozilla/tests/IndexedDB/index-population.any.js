// META: global=window,worker
// META: title=IndexedDB: an index created on an object store with records is populated from them
// META: timeout=long

// https://w3c.github.io/IndexedDB/#dom-idbobjectstore-createindex: creating an index is "processed
// as an asynchronous request within the upgrade transaction", which gives the index the records
// of the store's records; if a unique index would get a key twice, the upgrade transaction aborts
// with a ConstraintError. Requests made after createIndex see the whole index. These tests observe
// the index records through unique conflicts; index-reads.any.js reads them.

'use strict';

// Opens a new database at version 1, with `upgrade` as its upgradeneeded handler.
function openDatabase(t, upgrade) {
  const name = 'index-population-' + Math.random();
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

// Opens the database `name` at `version` and resolves with the names of the events of the upgrade
// transaction's requests and of the transaction, and of the open request's error, once it fails.
function failedUpgrade(name, version, upgrade) {
  return new Promise((resolve, reject) => {
    const events = [];
    const log = label => event => {
      const error = event.target.error;
      events.push(label + (error ? ' ' + error.name : ''));
    };
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = () => {
      request.transaction.onabort = log('abort');
      request.transaction.oncomplete = log('complete');
      upgrade(request.result, request.transaction, log);
    };
    request.onsuccess = () => {
      request.result.close();
      reject(new Error('The upgrade should fail'));
    };
    request.onerror = event => {
      event.preventDefault();
      log('open')(event);
      resolve(events);
    };
  });
}

function result(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

// The name of the request's error, which is prevented, so that the transaction goes on.
function failure(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => reject(new Error('The request should fail'));
    request.onerror = event => {
      event.preventDefault();
      resolve(request.error.name);
    };
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

// A database at version 1 with the store `items` holding `count` records `{k: 'k<i>', n: i}`
// under the keys 0 to count - 1.
async function databaseWithRecords(t, count) {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    for (let i = 0; i < count; i++) {
      store.add({k: 'k' + i, n: i}, i);
    }
  });
  db.close();
  return db.name;
}

promise_test(async t => {
  const name = await databaseWithRecords(t, MANY);
  let conflict, added;
  const db = await reopen(name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.createIndex('k', 'k', {unique: true});
    conflict = failure(store.add({k: 'k' + (MANY - 1)}, 'last'));
    added = result(store.add({k: 'new'}, 'new'));
  });
  t.add_cleanup(() => { db.close(); });
  assert_equals(await conflict, 'ConstraintError', 'the last record is in the index');
  assert_equals(await added, 'new');

  const tx = db.transaction('items', 'readwrite');
  const store = tx.objectStore('items');
  assert_equals(await failure(store.put({k: 'k0'}, 'first')), 'ConstraintError');
  assert_equals(await failure(store.put({k: 'new'}, 'other')), 'ConstraintError');
  assert_equals(await result(store.put({k: 'k0'}, 0)), 0, 'a record keeps its own key');
  await complete(tx);
}, 'An index created on a store with records has their keys for the requests that follow');

promise_test(async t => {
  let conflict;
  const db = await openDatabase(t, (db, tx) => {
    const store = db.createObjectStore('items');
    store.add({k: 'a'}, 1);
    store.createIndex('k', 'k', {unique: true});
    conflict = failure(store.add({k: 'a'}, 2));
  });
  db.close();
  assert_equals(await conflict, 'ConstraintError');
}, 'An index created on a store that the upgrade created and added records to has their keys');

promise_test(async t => {
  const name = await databaseWithRecords(t, MANY);
  const events = await failedUpgrade(name, 2, (db, tx, log) => {
    const store = tx.objectStore('items');
    store.put({k: 'k700'}, 3).onsuccess = log('put');
    store.createIndex('k', 'k', {unique: true});
    const after = store.add({k: 'new'}, 'new');
    after.onsuccess = log('add');
    after.onerror = log('add');
  });
  assert_array_equals(
      events, ['put', 'add AbortError', 'abort ConstraintError', 'open AbortError']);

  const db = await reopen(name, 1, () => assert_unreached('no upgrade'));
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction('items', 'readonly');
  const store = tx.objectStore('items');
  assert_array_equals([...store.indexNames], []);
  assert_equals(await result(store.count()), MANY);
  assert_equals((await result(store.get(3))).k, 'k3', 'the put is rolled back');
}, 'Records that would give a new unique index a key twice abort the upgrade with ' +
   'ConstraintError, after the events of earlier requests');

promise_test(async t => {
  let conflict, nested, plain, invalid;
  const db = await openDatabase(t, (db, tx) => {
    const store = db.createObjectStore('items');
    store.add({tags: ['a', 'a', 'b']}, 1);
    store.add({tags: ['c', null, {}, ['nested']]}, 2);
    store.add({tags: 'd'}, 3);
    store.add({tags: []}, 4);
    store.add({}, 5);
    store.createIndex('tags', 'tags', {unique: true, multiEntry: true});
    conflict = failure(store.add({tags: ['x', 'c']}, 6));
    nested = failure(store.add({tags: [['nested']]}, 7));
    plain = failure(store.add({tags: 'd'}, 8));
    invalid = result(store.add({tags: [null, 'e', {}]}, 9));
  });
  db.close();
  assert_equals(await conflict, 'ConstraintError', 'a valid subkey of an array is indexed');
  assert_equals(await nested, 'ConstraintError', 'an array in the array is a subkey');
  assert_equals(await plain, 'ConstraintError', 'a key that is no array is indexed');
  assert_equals(await invalid, 9, 'invalid subkeys are not indexed');

  const events = await failedUpgrade(db.name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.add({tags: ['y', 'b']}, 10);
    store.createIndex('more', 'tags', {unique: true, multiEntry: true});
  });
  assert_array_equals(events, ['abort ConstraintError', 'open AbortError']);
}, 'A multiEntry index gets each valid subkey of its records once, and a unique one aborts ' +
   'the upgrade if two records share one');

promise_test(async t => {
  const name = await databaseWithRecords(t, 3);
  let first, second;
  const db = await reopen(name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.createIndex('k', 'k', {unique: true});
    first = failure(store.add({k: 'k1'}, 'first'));
    store.deleteIndex('k');
    store.createIndex('k', 'n', {unique: true});
    second = failure(store.add({k: 'k1', n: 2}, 'second'));
    store.deleteIndex('k');
    store.add({k: 'k1', n: 2}, 'third');
  });
  t.add_cleanup(() => { db.close(); });
  assert_equals(await first, 'ConstraintError');
  assert_equals(await second, 'ConstraintError');
  const tx = db.transaction('items', 'readonly');
  assert_array_equals([...tx.objectStore('items').indexNames], []);
  assert_equals(await result(tx.objectStore('items').count()), 4);
}, 'Requests between the creation and the deletion of an index see the whole index');

promise_test(async t => {
  const name = await databaseWithRecords(t, MANY);
  let conflict;
  const db = await reopen(name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.createIndex('k', 'k', {unique: true});
    conflict = failure(store.add({k: 'k5'}, 'x'));
    db.deleteObjectStore('items');
    db.createObjectStore('items').createIndex('k', 'k', {unique: true});
  });
  t.add_cleanup(() => { db.close(); });
  assert_equals(await conflict, 'ConstraintError');
  const tx = db.transaction('items', 'readonly');
  assert_equals(await result(tx.objectStore('items').count()), 0);
}, 'Requests between the creation of an index and the deletion of its store see the whole index');

promise_test(async t => {
  const name = await databaseWithRecords(t, 3);
  let conflict;
  const db = await reopen(name, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.createIndex('k', 'k', {unique: true});
    store.add({k: 'k3', n: 7}, 3);
    store.createIndex('n', 'n', {unique: true});
    conflict = failure(store.add({k: 'k4', n: 7}, 4));
  });
  db.close();
  assert_equals(await conflict, 'ConstraintError', 'the second index has the added record');

  const other = await databaseWithRecords(t, 3);
  const events = await failedUpgrade(other, 2, (db, tx) => {
    const store = tx.objectStore('items');
    store.createIndex('k', 'k', {unique: true});
    store.add({k: 'k9', n: 1}, 9);
    store.createIndex('n', 'n', {unique: true});
  });
  assert_array_equals(events, ['abort ConstraintError', 'open AbortError'],
                      'the second index gets the record added before its creation');
}, 'Indexes created one after another are populated in turn, each after the requests before it');

promise_test(async t => {
  const name = await databaseWithRecords(t, MANY);
  const events = await failedUpgrade(name, 2, (db, tx, log) => {
    const store = tx.objectStore('items');
    store.put({k: 'changed'}, 0).onsuccess = event => {
      log('put')(event);
      tx.abort();
    };
    store.createIndex('k', 'k', {unique: true});
    const after = store.add({k: 'new'}, 'new');
    after.onerror = log('add');
  });
  assert_array_equals(events, ['put', 'add AbortError', 'abort AbortError', 'open AbortError']);

  const db = await reopen(name, 1, () => assert_unreached('no upgrade'));
  t.add_cleanup(() => { db.close(); });
  const tx = db.transaction('items', 'readwrite');
  const store = tx.objectStore('items');
  assert_array_equals([...store.indexNames], []);
  assert_equals((await result(store.get(0))).k, 'k0');
  assert_equals(await result(store.count()), MANY);
  await result(store.put({k: 'k1'}, 'again'));
  await complete(tx);
}, 'Aborting the upgrade while an index is populated leaves the database as it was');

promise_test(async t => {
  let conflict, generated;
  const db = await openDatabase(t, (db, tx) => {
    const store = db.createObjectStore('items', {keyPath: 'id', autoIncrement: true});
    store.add({x: 1});
    store.add({x: 2});
    store.add({id: 'explicit', x: 3});
    store.createIndex('id', 'id', {unique: true});
    store.createIndex('pair', ['x', 'id'], {unique: true});
    store.createIndex('x', 'x', {unique: true});
    conflict = failure(store.add({id: 'other', x: 2}));
    generated = result(store.add({x: 4}));
  });
  db.close();
  assert_equals(await conflict, 'ConstraintError');
  assert_equals(await generated, 3);
}, 'Records stored under generated keys are indexed with their values as they read back');

// More records than one step of a population reads, whose `tags` arrays have a hole at index 1,
// through which the multiEntry key conversion reaches Array.prototype[1].
const WITH_HOLES = 600;

// A database at version 1 with the store `items` holding `WITH_HOLES` records
// `{tags: ['a<i>', , 'b<i>']}` under the keys 0 to WITH_HOLES - 1, whose arrays are marked with a
// `hole` property, which cloning keeps.
async function databaseWithHoles(t) {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    for (let i = 0; i < WITH_HOLES; i++) {
      const tags = ['a' + i, , 'b' + i];
      tags.hole = true;
      store.add({tags}, i);
    }
  });
  db.close();
  return db.name;
}

// Defines `get` as the getter of Array.prototype[1] for the marked arrays until the returned
// function removes it. Other arrays read undefined there, and assignments to index 1 create their
// own property.
function arrayGetter(t, get) {
  Object.defineProperty(Array.prototype, 1, {
    configurable: true,
    get() {
      return this.hole === true ? get() : undefined;
    },
    set(value) {
      Object.defineProperty(this, 1, {value, writable: true, enumerable: true, configurable: true});
    },
  });
  const remove = () => { delete Array.prototype[1]; };
  t.add_cleanup(remove);
  return remove;
}

promise_test(async t => {
  const name = await databaseWithHoles(t);
  let tx = null;
  const errors = new Set();
  const remove = arrayGetter(t, () => {
    try {
      tx.objectStore('items').put({tags: ['inner']}, 'inner');
      errors.add('none');
    } catch (e) {
      errors.add(e.name);
    }
    throw new Error('getter');
  });
  let conflict, added;
  const db = await reopen(name, 2, (db, transaction) => {
    tx = transaction;
    const store = transaction.objectStore('items');
    store.createIndex('tags', 'tags', {multiEntry: true, unique: true});
    conflict = failure(store.add({tags: ['b' + (WITH_HOLES - 1)]}, 'conflict'));
    added = result(store.add({tags: ['new']}, 'new'));
  });
  remove();
  t.add_cleanup(() => { db.close(); });
  assert_array_equals([...errors], ['TransactionInactiveError']);
  assert_equals(await conflict, 'ConstraintError', 'the subkeys around the hole are indexed');
  assert_equals(await added, 'new');
  const store = db.transaction('items').objectStore('items');
  assert_equals(await result(store.count()), WITH_HOLES + 1);
  assert_equals(await result(store.get('inner')), undefined);
}, 'A getter that a population reaches and that throws skips its subkey, and its requests throw');

promise_test(async t => {
  const name = await databaseWithHoles(t);
  let tx = null;
  let calls = 0;
  const remove = arrayGetter(t, () => {
    calls++;
    if (tx) {
      const transaction = tx;
      tx = null;
      transaction.abort();
    }
    return undefined;
  });
  const events = await failedUpgrade(name, 2, (db, transaction) => {
    tx = transaction;
    transaction.objectStore('items').createIndex('tags', 'tags', {multiEntry: true});
  });
  remove();
  assert_array_equals(events, ['abort AbortError', 'open AbortError']);
  assert_less_than_equal(calls, 1, 'no more keys are computed after the abort');

  const db = await reopen(name, 1, () => assert_unreached('no upgrade'));
  t.add_cleanup(() => { db.close(); });
  const tx2 = db.transaction('items', 'readwrite');
  const store = tx2.objectStore('items');
  assert_array_equals([...store.indexNames], []);
  await result(store.put({tags: ['z']}, 'z'));
  await complete(tx2);
}, 'A getter that a population reaches and that aborts the upgrade leaves the database as it was');
