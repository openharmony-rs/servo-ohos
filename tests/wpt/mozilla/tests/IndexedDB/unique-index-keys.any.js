// META: global=window,worker
// META: title=IndexedDB: the keys a put gives unique indexes, and what a failed put leaves

// https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store step 5: each index gets the
// keys extracted from the value, none if extracting fails or gives an invalid key, and each valid
// subkey of an array for a multiEntry index. A key that a unique index has for another record
// fails the put with a ConstraintError, and a failed request reverts all its changes
// (https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request). Index reads aren't
// implemented yet, so these tests observe the index records through unique conflicts.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'unique-index-keys-' + Math.random();
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => upgrade(request.result, request.transaction);
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

function usersStore(db, options) {
  const store = db.createObjectStore('users', options);
  store.createIndex('email', 'email', {unique: true});
  store.createIndex('nick', 'nick', {unique: true});
  store.createIndex('name', 'name');
  return store;
}

promise_test(async t => {
  const db = await openDatabase(t, db => usersStore(db, {autoIncrement: true}));
  const tx = db.transaction('users', 'readwrite');
  const store = tx.objectStore('users');
  assert_equals(await result(store.add({email: 'a', nick: 'x'})), 1);
  assert_equals(await failure(store.add({email: 'a', nick: 'y'})), 'ConstraintError');
  assert_equals(await failure(store.put({email: 'a', nick: 'y'}, 7)), 'ConstraintError');
  // Neither the record nor the other unique index's key nor the key generator changed.
  assert_equals(await result(store.count()), 1);
  assert_equals(await result(store.add({email: 'b', nick: 'y'})), 2);
  await complete(tx);
}, 'A put that a unique index rejects stores nothing, also not in other indexes or the key ' +
   'generator');

promise_test(async t => {
  const db = await openDatabase(t, db => usersStore(db, {keyPath: 'id'}));
  const tx = db.transaction('users', 'readwrite');
  const store = tx.objectStore('users');
  await result(store.put({id: 1, email: 'a', nick: 'x'}));
  await result(store.put({id: 2, email: 'b', nick: 'y'}));
  assert_equals(await failure(store.put({id: 1, email: 'b', nick: 'z'})), 'ConstraintError');
  assert_equals((await result(store.get(1))).email, 'a', 'the record is unchanged');
  // Record 1 still has its keys, and replacing it with its own keys is no conflict.
  assert_equals(await failure(store.put({id: 3, email: 'c', nick: 'x'})), 'ConstraintError');
  await result(store.put({id: 1, email: 'a', nick: 'x', name: 'ann'}));
  // Record 1 gives up 'x', which is free for another record then.
  await result(store.put({id: 1, email: 'a', nick: 'w'}));
  await result(store.put({id: 3, email: 'c', nick: 'x'}));
  await complete(tx);
}, 'A record that a put replaces keeps its index keys if the put fails, and gives them up if ' +
   'it succeeds');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store', {autoIncrement: true})
        .createIndex('tags', 'tags', {unique: true, multiEntry: true});
  });
  const tx = db.transaction('store', 'readwrite');
  const store = tx.objectStore('store');
  await result(store.put({tags: [1, 2]}));
  assert_equals(await failure(store.put({tags: [3, 2]})), 'ConstraintError');
  // The failed put stored no key 3, and a key repeated in one value is no conflict.
  await result(store.put({tags: [3, 3]}));
  // Subkeys that aren't valid keys are skipped, the others are kept.
  await result(store.put({tags: [4, {}, null, undefined, [5, {}], 4]}));
  assert_equals(await failure(store.put({tags: [4]})), 'ConstraintError');
  await result(store.put({tags: [[5]]}));
  // An empty array gives no records, and a non-array value gives one.
  await result(store.put({tags: []}));
  await result(store.put({tags: []}));
  await result(store.put({tags: 6}));
  assert_equals(await failure(store.put({tags: [6]})), 'ConstraintError');
  await complete(tx);
}, 'Each key of a multiEntry unique index must be unique, and invalid subkeys are skipped');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    db.createObjectStore('store', {autoIncrement: true})
        .createIndex('k', 'k', {unique: true});
  });
  const tx = db.transaction('store', 'readwrite');
  const store = tx.objectStore('store');
  for (const value of [{}, {k: {}}, {k: [1, {}]}, {k: NaN}, {k: null}, {k: undefined},
                       {k: true}]) {
    await result(store.put(value));
    await result(store.put(value));
  }
  assert_equals(await result(store.count()), 14);
  await complete(tx);
}, 'A value without a valid key at the key path gives a unique index no record');

promise_test(async t => {
  const db = await openDatabase(t, db => usersStore(db, {keyPath: 'id'}));
  let tx = db.transaction('users', 'readwrite');
  let store = tx.objectStore('users');
  for (let id = 1; id <= 6; id++) {
    await result(store.put({id, email: 'e' + id, nick: 'n' + id}));
  }
  await complete(tx);

  tx = db.transaction('users', 'readwrite');
  store = tx.objectStore('users');
  await result(store.put({id: 7, email: 'e7'}));
  const aborted = new Promise(resolve => tx.onabort = resolve);
  tx.abort();
  await aborted;

  tx = db.transaction('users', 'readwrite');
  store = tx.objectStore('users');
  // The aborted put's key is free.
  await result(store.put({id: 8, email: 'e7'}));
  await result(store.delete(1));
  await result(store.delete(IDBKeyRange.bound(2, 4, true, false)));
  // Deleting a record frees its keys.
  for (const id of [1, 3, 4]) {
    await result(store.put({id: 10 + id, email: 'e' + id}));
  }
  assert_equals(await failure(store.put({id: 12, email: 'e2'})), 'ConstraintError');
  const cursor = await result(store.openCursor(5));
  await result(cursor.update({id: 5, email: 'z', nick: 'n5'}));
  await result(store.put({id: 15, email: 'e5'}));
  assert_equals(await failure(store.put({id: 16, email: 'z'})), 'ConstraintError');
  const cursorToDelete = await result(store.openCursor(6));
  await result(cursorToDelete.delete());
  await result(store.put({id: 16, email: 'e6', nick: 'n6'}));
  await result(store.clear());
  await result(store.put({id: 17, email: 'z', nick: 'n5'}));
  await complete(tx);
}, 'Aborting, deleting, clearing and cursor updates and deletes free unique keys');

promise_test(async t => {
  const name = 'unique-index-keys-reused-' + Math.random();
  let db;
  t.add_cleanup(() => {
    db.close();
    indexedDB.deleteDatabase(name);
  });
  const open = (version, upgrade) => new Promise((resolve, reject) => {
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = () => upgrade(request.result, request.transaction);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  db = await open(1, db => {
    const store = db.createObjectStore('store', {keyPath: 'id'});
    store.createIndex('a', 'a', {unique: true});
    store.put({id: 1, a: 'x'});
    const other = db.createObjectStore('other', {keyPath: 'id'});
    other.createIndex('b', 'b', {unique: true});
    other.put({id: 1, b: 'x'});
  });
  db.close();
  db = await open(2, (db, tx) => {
    tx.objectStore('store').deleteIndex('a');
    db.deleteObjectStore('other');
  });
  db.close();
  // The new indexes get the ids of the deleted ones, but none of their records.
  db = await open(3, (db, tx) => {
    tx.objectStore('store').createIndex('c', 'c', {unique: true});
    db.createObjectStore('other', {keyPath: 'id'}).createIndex('d', 'd', {unique: true});
  });
  const tx = db.transaction(['store', 'other'], 'readwrite');
  await result(tx.objectStore('store').put({id: 2, c: 'x'}));
  await result(tx.objectStore('other').put({id: 2, d: 'x'}));
  await complete(tx);
}, 'Indexes created after others were deleted start without records');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('store', {keyPath: 'id', autoIncrement: true});
    store.createIndex('id', 'id', {unique: true});
    store.createIndex('ids', 'id', {unique: true, multiEntry: true});
    store.createIndex('pair', ['x', 'id'], {unique: true});
    store.createIndex('below', 'id.sub', {unique: true});
    store.createIndex('x', 'x', {unique: true});
  });
  const tx = db.transaction('store', 'readwrite');
  const store = tx.objectStore('store');
  assert_equals(await result(store.put({x: 1})), 1);
  assert_equals(await result(store.put({x: 2})), 2);
  assert_equals(await failure(store.put({x: 2})), 'ConstraintError');
  assert_equals(await result(store.put({x: 3})), 3, 'the failed put used no key');
  // A record replaced with its own keys doesn't conflict with them.
  assert_equals(await result(store.put({id: 1, x: 1})), 1);
  assert_equals(await result(store.put({id: 2, x: 2, sub: 1})), 2);
  await complete(tx);
}, 'Index keys that depend on a generated key cause no conflicts among records');
