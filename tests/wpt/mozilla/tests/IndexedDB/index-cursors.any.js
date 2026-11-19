// META: global=window,worker
// META: title=IndexedDB: cursors over the records of an index

// https://w3c.github.io/IndexedDB/#iterate-a-cursor: a cursor over an index moves through its
// records sorted by key and then by value, the primary key of the referenced record, from its
// position and object store position, and each step sees the records as they are when it runs.
// These tests cover what the IndexedDB WPT tests leave out: the record the unique directions
// visit for a key with several records, continuePrimaryKey() and advance() over keys with several
// records, and records that change between the steps of index cursors.

'use strict';

// Opens a new database with `upgrade` as its upgradeneeded handler.
function openDatabase(t, upgrade) {
  const name = 'index-cursors-' + Math.random();
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

// The records of 'items', under the keys 1 to 6, and their keys in the index 'k'.
const KEYS = {1: 'b', 2: 'a', 3: 'b', 4: 'a', 5: 'c', 6: 'b'};

function createItems(db) {
  const store = db.createObjectStore('items');
  store.createIndex('k', 'k');
  store.createIndex('tags', 'tags', {multiEntry: true});
  store.createIndex('u', 'u', {unique: true});
  for (const [key, k] of Object.entries(KEYS)) {
    store.put({k, tags: [k, 'all'], u: -Number(key)}, Number(key));
  }
}

// Runs `onCursor` on each record that `request`, a cursor request, visits, then resolves with
// [key, primaryKey] of each. `onCursor` may move the cursor itself by returning true.
function visit(request, onCursor = () => false) {
  return new Promise((resolve, reject) => {
    const visited = [];
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) {
        resolve(visited);
        return;
      }
      visited.push([cursor.key, cursor.primaryKey]);
      if (!onCursor(cursor, visited.length - 1)) {
        cursor.continue();
      }
    };
    request.onerror = () => reject(request.error);
  });
}

// The index 'k' of 'items', in a new transaction.
function indexK(db, mode = 'readonly') {
  return db.transaction('items', mode).objectStore('items').index('k');
}

promise_test(async t => {
  const db = await openDatabase(t, createItems);
  const expected = {
    next: [['a', 2], ['a', 4], ['b', 1], ['b', 3], ['b', 6], ['c', 5]],
    prev: [['c', 5], ['b', 6], ['b', 3], ['b', 1], ['a', 4], ['a', 2]],
    nextunique: [['a', 2], ['b', 1], ['c', 5]],
    prevunique: [['c', 5], ['b', 1], ['a', 2]],
  };
  for (const [direction, records] of Object.entries(expected)) {
    const values = [];
    const visited = await visit(indexK(db).openCursor(null, direction), cursor => {
      values.push(cursor.value);
    });
    assert_array_equals(visited.flat(), records.flat(), `openCursor ${direction}`);
    assert_array_equals(values.map(value => value.k), records.map(([k]) => k));
    const keys = await visit(indexK(db).openKeyCursor(null, direction), cursor => {
      assert_false('value' in cursor, 'a key cursor has no value');
    });
    assert_array_equals(keys.flat(), records.flat(), `openKeyCursor ${direction}`);
  }
  const range = IDBKeyRange.bound('a', 'b', true, false);
  const inRange = await visit(indexK(db).openCursor(range, 'prevunique'));
  assert_array_equals(inRange.flat(), ['b', 1], 'prevunique in a range');
}, 'Index cursors visit the records by key and then primary key, and the unique directions ' +
   'visit the record with the lowest primary key of each key');

promise_test(async t => {
  const db = await openDatabase(t, createItems);
  const tags = db.transaction('items').objectStore('items').index('tags');
  const visited = await visit(tags.openCursor(null, 'prevunique'));
  assert_array_equals(visited.flat(), ['c', 5, 'b', 1, 'all', 1, 'a', 2]);
  const unique = db.transaction('items').objectStore('items').index('u');
  const values = [];
  const all = await visit(unique.openCursor(IDBKeyRange.upperBound(-3), 'next'), cursor => {
    values.push(cursor.value.k);
  });
  assert_array_equals(all.flat(), [-6, 6, -5, 5, -4, 4, -3, 3]);
  assert_array_equals(values, ['b', 'c', 'a', 'b']);
}, 'Cursors over a multiEntry and a unique index');

promise_test(async t => {
  const db = await openDatabase(t, createItems);
  const next = await visit(indexK(db).openCursor(), (cursor, i) => {
    switch (i) {
      case 0:
        // At ('a', 2).
        cursor.continuePrimaryKey('b', 2);
        return true;
      case 1:
        // At ('b', 3): the position's primary key is not beyond it, a larger one is.
        assert_throws_dom('DataError', () => cursor.continuePrimaryKey('b', 3));
        cursor.continuePrimaryKey('b', 4);
        return true;
      case 2:
        // At ('b', 6): the second record on is past the end.
        cursor.advance(2);
        return true;
    }
    return false;
  });
  assert_array_equals(next.flat(), ['a', 2, 'b', 3, 'b', 6]);

  const prev = await visit(indexK(db).openCursor(null, 'prev'), (cursor, i) => {
    if (i === 0) {
      cursor.continuePrimaryKey('b', 5);
      return true;
    }
    if (i === 1) {
      cursor.continuePrimaryKey('a', 10);
      return true;
    }
    return false;
  });
  assert_array_equals(prev.flat(), ['c', 5, 'b', 3, 'a', 4, 'a', 2]);

  const nextUnique = await visit(indexK(db).openCursor(null, 'nextunique'), (cursor, i) => {
    if (i === 0) {
      cursor.advance(2);
      return true;
    }
    return false;
  });
  assert_array_equals(nextUnique.flat(), ['a', 2, 'c', 5]);

  const prevUnique = await visit(indexK(db).openCursor(null, 'prevunique'), (cursor, i) => {
    if (i === 0) {
      cursor.continue('a');
      return true;
    }
    return false;
  });
  assert_array_equals(prevUnique.flat(), ['c', 5, 'a', 2]);
}, 'continuePrimaryKey(), continue() and advance() on an index with several records per key');

promise_test(async t => {
  const db = await openDatabase(t, createItems);
  const index = indexK(db, 'readwrite');
  const store = index.objectStore;
  const visited = await visit(index.openCursor(), (cursor, i) => {
    if (i === 0) {
      // At ('a', 2): a record ahead goes, one comes ahead and one behind.
      store.delete(4);
      store.put({k: 'a'}, 7);
      store.put({k: 'a'}, 0);
    } else if (i === 1) {
      // At ('a', 7): a record moves ahead and one moves behind.
      store.put({k: 'z'}, 1);
      store.put({k: '0'}, 3);
    } else if (i === 3) {
      // At ('c', 5): the record moves behind the cursor.
      cursor.update({k: 'a'});
    }
    return false;
  });
  assert_array_equals(visited.flat(), ['a', 2, 'a', 7, 'b', 6, 'c', 5, 'z', 1]);
}, 'Each step of an index cursor sees the records put, replaced and deleted before it');

for (const [direction, expected] of [
  ['next', ['a', 'moved', 'b', 'a', 'b']],
  ['nextunique', ['a', 'b', 'a', 'b']],
]) {
  promise_test(async t => {
    const db = await openDatabase(t, createItems);
    const store = db.transaction('items', 'readwrite').objectStore('items');
    const values = [];
    await visit(store.index('u').openCursor(IDBKeyRange.lowerBound(-4), direction), cursor => {
      values.push(cursor.value.k);
      if (values.length === 1) {
        // The record at the cursor's position goes, and another one takes its key.
        store.delete(4);
        store.put({k: 'moved', u: -4}, 6);
      }
    });
    assert_array_equals(values, expected);
  }, `A unique index cursor in "${direction}" after another record took the key at its position`);
}

promise_test(async t => {
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items', {keyPath: 'id', autoIncrement: true});
    store.createIndex('x', 'x');
    store.put({x: 'b'});
    store.put({x: 'a'});
  });
  const tx = db.transaction('items', 'readwrite');
  const committed = new Promise((resolve, reject) => {
    tx.oncomplete = resolve;
    tx.onabort = () => reject(tx.error);
  });
  const values = [];
  await visit(tx.objectStore('items').index('x').openCursor(), cursor => {
    values.push(cursor.value);
    if (cursor.value.x === 'a') {
      // The record moves ahead of the cursor, which reaches it again.
      cursor.update({x: 'c', id: cursor.primaryKey});
    } else if (cursor.value.x === 'b') {
      cursor.delete();
    }
  });
  assert_object_equals(values, [{x: 'a', id: 2}, {x: 'b', id: 1}, {x: 'c', id: 2}]);
  await committed;
  const index = db.transaction('items').objectStore('items').index('x');
  const [all, count] = await Promise.all([index.getAll(), index.count()].map(request =>
    new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    })));
  assert_object_equals(all, [{x: 'c', id: 2}]);
  assert_equals(count, 1);
}, 'Values of index cursors carry their generated keys, and update() and delete() change the ' +
   'record and its index records');
