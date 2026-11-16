// META: global=window,worker
// META: title=IndexedDB: cursor steps see the records as they are when they run
// META: timeout=long

// https://w3c.github.io/IndexedDB/#iterate-a-cursor: each step of a cursor finds its record
// among the records as they are when the step runs, after every request made before it. Servo's
// cursors take the records of later steps from what the backend returned with an earlier one,
// as long as no request in the transaction has written since. These tests compare every step of
// cursors over a store and an index, in every direction, with continue(), continue(key),
// continuePrimaryKey() and advance(), and writes between and around the steps, with a model of
// the spec's steps.

'use strict';

function openDatabase(t, upgrade) {
  const name = 'cursor-prefetch-' + Math.random();
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

// The store 'items' with the records 1 to `count`, whose values {n, k} have the key in the index
// 'k' as k.
function createItems(count) {
  return db => {
    const store = db.createObjectStore('items');
    store.createIndex('k', 'k');
    for (let n = 1; n <= count; n++) {
      store.put({n, k: n % 7}, n);
    }
  };
}

function seededRandom(seed) {
  let state = seed;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 4294967296;
  };
}

const cmp = (a, b) => indexedDB.cmp(a, b);

// The records of 'items' (`values`, primary key → value) and of its index 'k', as the requests
// made so far leave them.
class Model {
  constructor(count) {
    this.values = new Map();
    for (let n = 1; n <= count; n++) {
      this.values.set(n, {n, k: n % 7});
    }
    this.sorted = {};
  }

  put(key, value) {
    this.values.set(key, value);
    this.sorted = {};
  }

  delete(key) {
    this.values.delete(key);
    this.sorted = {};
  }

  // The records of `source`, 'store' or 'k', as [key, primary key], in the source's order.
  records(source) {
    if (!this.sorted[source]) {
      const records = [];
      for (const [key, value] of this.values) {
        records.push(source == 'store' ? [key, key] : [value.k, key]);
      }
      records.sort((a, b) => cmp(a[0], b[0]) || cmp(a[1], b[1]));
      this.sorted[source] = records;
    }
    return this.sorted[source];
  }
}

// Step 9 of "iterate a cursor", for the cursor `state` ({source, direction, range, position,
// objectStorePosition}): the record found, [key, primary key], or undefined, and the cursor
// moved to it.
function iterate(model, state, key, primaryKey, count) {
  const records = model.records(state.source);
  const index = state.source != 'store';
  const inRange = record => state.range.includes(record[0]);
  let found;
  for (; count > 0; count--) {
    const {position, objectStorePosition} = state;
    const beyond = (record, sign) => {
      if (position === undefined) return true;
      const c = cmp(record[0], position) * sign;
      if (!index || state.direction.endsWith('unique')) return c > 0;
      return c > 0 || (c == 0 && cmp(record[1], objectStorePosition) * sign > 0);
    };
    const keyOk = (record, sign) => {
      if (key === undefined) return true;
      const c = cmp(record[0], key) * sign;
      if (primaryKey === undefined) return c >= 0;
      return c > 0 || (c == 0 && cmp(record[1], primaryKey) * sign >= 0);
    };
    switch (state.direction) {
      case 'next':
      case 'nextunique':
        found = records.find(r => keyOk(r, 1) && beyond(r, 1) && inRange(r));
        break;
      case 'prev':
        found = records.findLast(r => keyOk(r, -1) && beyond(r, -1) && inRange(r));
        break;
      case 'prevunique': {
        const temp = records.findLast(r => keyOk(r, -1) && beyond(r, -1) && inRange(r));
        found = temp && records.find(r => cmp(r[0], temp[0]) == 0);
        break;
      }
    }
    if (!found) return undefined;
    state.position = found[0];
    state.objectStorePosition = found[1];
    key = primaryKey = undefined;
  }
  return found;
}

// Runs the cursors `specs` ({source, direction, range, keyOnly}) over 'items' in one transaction,
// taking turns as their results arrive, for at most `steps` steps each. Before each step, a
// random choice of continue(), continue(key), continuePrimaryKey() and advance() is made, and
// with a probability of 1 / `writes`, a write through the store or the cursor before the step
// and one through the store after it. Every record a cursor visits is checked against the model.
function runCursors(t, db, {seed, count, specs, writes, steps}) {
  const random = seededRandom(seed);
  const pick = n => Math.floor(random() * n);
  const model = new Model(count);
  const transaction = db.transaction('items', writes ? 'readwrite' : 'readonly');
  const store = transaction.objectStore('items');
  let visits = 0;
  const states = [];

  // Writes a record just ahead of a random cursor, where it may have taken records already.
  const write = (cursor, state) => {
    const choice = pick(6);
    const near = states[pick(states.length)];
    const sign = near.direction.startsWith('prev') ? -1 : 1;
    let key = 1 + pick(count + 40);
    let k = pick(9);
    if (near.position !== undefined && near.source == 'store') {
      key = near.position + sign * (1 + pick(6)) + (pick(2) ? 0.5 : 0);
    } else if (near.position !== undefined) {
      k = near.position + sign * pick(2);
    }
    const value = {n: -key, k};
    if (choice == 0) {
      store.put(value, key);
      model.put(key, value);
    } else if (choice == 1) {
      store.delete(key);
      model.delete(key);
    } else if (cursor && !state.keyOnly && choice == 2) {
      const value = {n: -state.objectStorePosition, k};
      cursor.update(value);
      model.put(state.objectStorePosition, value);
    } else if (cursor && !state.keyOnly && choice == 3) {
      cursor.delete();
      model.delete(state.objectStorePosition);
    }
  };

  // Moves the cursor of `state` with a random step and returns the expected record, with the
  // value it has when the step runs.
  const move = (cursor, state) => {
    const forward = !state.direction.startsWith('prev');
    const ahead = (key, by) => (forward ? key + by : key - by);
    const choice = pick(20);
    let found;
    if (choice < 14) {
      cursor.continue();
      found = iterate(model, state, undefined, undefined, 1);
    } else if (choice < 16) {
      const key = ahead(state.position, state.source == 'store' ? 1 + pick(8) : 1);
      cursor.continue(key);
      found = iterate(model, state, key, undefined, 1);
    } else if (choice < 18 && state.source != 'store' && !state.direction.endsWith('unique')) {
      const [key, primaryKey] = pick(4) ?
          [state.position, ahead(state.objectStorePosition, 1 + pick(30))] :
          [ahead(state.position, 1), 1 + pick(count)];
      cursor.continuePrimaryKey(key, primaryKey);
      found = iterate(model, state, key, primaryKey, 1);
    } else {
      const n = 1 + pick(choice < 19 ? 3 : 12);
      cursor.advance(n);
      found = iterate(model, state, undefined, undefined, n);
    }
    return found && [...found, {...model.values.get(found[1])}];
  };

  return new Promise((resolve, reject) => {
    let running = specs.length;
    transaction.onabort = () => reject(transaction.error);
    transaction.oncomplete = () => resolve(visits);
    for (const spec of specs) {
      const state = {...spec, range: spec.range || IDBKeyRange.lowerBound(-Infinity)};
      states.push(state);
      const source = spec.source == 'store' ? store : store.index(spec.source);
      const request = spec.keyOnly ?
          source.openKeyCursor(state.range, spec.direction) :
          source.openCursor(state.range, spec.direction);
      let expected = iterate(model, state, undefined, undefined, 1);
      expected = expected && [...expected, {...model.values.get(expected[1])}];
      let taken = 0;
      request.onerror = () => reject(request.error);
      request.onsuccess = t.step_func(() => {
        const cursor = request.result;
        const label = `seed ${seed}, ${spec.source} ${spec.direction}, step ${taken}`;
        if (!expected) {
          assert_equals(cursor, null, `${label}: past the end`);
          running--;
          return;
        }
        assert_not_equals(cursor, null, `${label}: a record`);
        assert_equals(cursor.key, expected[0], `${label}: key`);
        assert_equals(cursor.primaryKey, expected[1], `${label}: primary key`);
        if (!spec.keyOnly) {
          assert_equals(cursor.value.n, expected[2].n, `${label}: value`);
          assert_equals(cursor.value.k, expected[2].k, `${label}: value`);
        }
        visits++;
        if (++taken == steps) {
          running--;
          return;
        }
        if (writes && pick(writes) == 0) write(cursor, state);
        expected = move(cursor, state);
        if (writes && pick(writes) == 0) write(null, state);
      });
    }
  });
}

const CURSORS = [
  {source: 'store', direction: 'next'},
  {source: 'k', direction: 'prev'},
  {source: 'k', direction: 'nextunique', range: IDBKeyRange.bound(1, 5)},
  {source: 'store', direction: 'prev', range: IDBKeyRange.bound(10, 70, true, false)},
  {source: 'k', direction: 'next', keyOnly: true},
  {source: 'k', direction: 'prevunique'},
];

for (const seed of [1, 2]) {
  promise_test(async t => {
    const db = await openDatabase(t, createItems(80));
    const visits = await runCursors(t, db, {seed, count: 80, specs: CURSORS, writes: 0,
                                            steps: 200});
    assert_greater_than(visits, 50);
  }, `Cursors taking turns over the same records, without writes (seed ${seed})`);
}

// Writes are rare enough that cursors take records of later steps from earlier ones in between.
for (const seed of [3, 4, 5, 6, 7, 8, 10, 11]) {
  promise_test(async t => {
    const db = await openDatabase(t, createItems(200));
    const specs = [CURSORS[seed % 6], CURSORS[(seed + 1) % 6], CURSORS[(seed + 3) % 6]];
    const visits = await runCursors(t, db, {seed, count: 200, specs, writes: 40, steps: 250});
    assert_greater_than(visits, 20);
  }, `Cursors taking turns, with writes now and then (seed ${seed})`);
}

promise_test(async t => {
  const db = await openDatabase(t, createItems(80));
  const visits = await runCursors(t, db, {seed: 9, count: 80, specs: CURSORS, writes: 3,
                                          steps: 150});
  assert_greater_than(visits, 50);
}, 'Cursors taking turns, with writes through the store and the cursors at most steps');

// Runs `onCursor` on each record that the cursor of `request` visits and continues the cursor,
// unless `onCursor` returns 'moved' (it moved the cursor) or 'stop', until then or the end, and
// resolves with the primary keys visited.
function visit(request, onCursor) {
  return new Promise((resolve, reject) => {
    const visited = [];
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) {
        resolve(visited);
        return;
      }
      visited.push(cursor.primaryKey);
      try {
        const next = onCursor(cursor, visited.length);
        if (next == 'stop') {
          resolve(visited);
        } else if (next != 'moved') {
          cursor.continue();
        }
      } catch (error) {
        reject(error);
      }
    };
    request.onerror = () => reject(request.error);
  });
}

promise_test(async t => {
  const db = await openDatabase(t, createItems(30));
  const store = db.transaction('items', 'readwrite').objectStore('items');
  const visited = await visit(store.openCursor(), (cursor, steps) => {
    if (cursor.key == 4) {
      store.put({n: 5.5, k: 0}, 5.5);
      store.delete(6);
      store.put({n: 70, k: 0}, 7);
    }
    if (cursor.key == 7) {
      assert_equals(cursor.value.n, 70, 'the changed value');
      // The step runs before the put, and the next one after it.
      cursor.continue();
      store.put({n: 8.5, k: 0}, 8.5);
      return 'moved';
    }
  });
  const expected = [1, 2, 3, 4, 5, 5.5, 7, 8, 8.5];
  for (let n = 9; n <= 30; n++) expected.push(n);
  assert_array_equals(visited, expected);
}, 'Writes made before a step, and right after it, are seen by the steps after them');

promise_test(async t => {
  const db = await openDatabase(t, createItems(30));
  const store = db.transaction('items', 'readwrite').objectStore('items');
  const visited = await visit(store.index('k').openCursor(), cursor => {
    if (cursor.primaryKey == 22) {
      cursor.update({n: 22, k: 2});
    }
  });
  // The keys are n % 7; the update moves 22 from key 1 to key 2, which the cursor visits next.
  assert_array_equals(visited, [7, 14, 21, 28, 1, 8, 15, 22, 29, 2, 9, 16, 22, 23, 30, 3, 10, 17,
                                24, 4, 11, 18, 25, 5, 12, 19, 26, 6, 13, 20, 27]);
}, 'cursor.update() that moves the record ahead in the index is seen later');

promise_test(async t => {
  const db = await openDatabase(t, createItems(20));
  const store = db.transaction('items', 'readwrite').objectStore('items');
  const behind = store.openCursor();
  const ahead = store.openCursor(IDBKeyRange.lowerBound(10));
  const visited = {behind: [], ahead: []};
  await new Promise((resolve, reject) => {
    let ended = 0;
    const end = () => ++ended == 2 && resolve();
    behind.onsuccess = t.step_func(() => {
      const cursor = behind.result;
      if (!cursor) return end();
      visited.behind.push(cursor.key);
      if (cursor.key != 8) {
        cursor.continue();
        return;
      }
      // The cursor ahead deletes the records it visits, after this one took records ahead.
      ahead.result.delete();
      ahead.result.continue();
    });
    ahead.onsuccess = t.step_func(() => {
      const cursor = ahead.result;
      if (!cursor) {
        behind.result.continue();
        return end();
      }
      visited.ahead.push(cursor.key);
      if (cursor.key == 10) return;
      cursor.delete();
      cursor.continue();
    });
    behind.onerror = ahead.onerror = () => reject(behind.error || ahead.error);
  });
  assert_array_equals(visited.ahead, [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
  assert_array_equals(visited.behind, [1, 2, 3, 4, 5, 6, 7, 8, 9],
                      'records deleted by the other cursor');
}, 'cursor.delete() of one cursor is seen by another cursor over the same records');

promise_test(async t => {
  const db = await openDatabase(t, createItems(40));
  const index = db.transaction('items').objectStore('items').index('k');
  const moves = [
    // Keys 0: 7, 14, 21, 28, 35; 1: 1, 8, 15, 22, 29, 36; 2: 2, 9, ...
    [c => c.continue(), 14],
    [c => c.continue(), 21],
    [c => c.continue(), 28],
    [c => c.continue(), 35],
    [c => c.continue(), 1],
    [c => c.continue(), 8],
    // Within the records taken so far, and beyond them.
    [c => c.continuePrimaryKey(1, 20), 22],
    [c => c.continue(2), 2],
    [c => c.advance(2), 16],
    [c => c.continuePrimaryKey(5, 12), 12],
    [c => c.continue(6), 6],
    [c => c.advance(4), 34],
    [c => c.advance(10), null],
  ];
  const request = index.openCursor();
  await new Promise((resolve, reject) => {
    let step = 0;
    request.onsuccess = t.step_func(() => {
      const cursor = request.result;
      if (step == 0) {
        assert_equals(cursor.primaryKey, 7);
      } else {
        const expected = moves[step - 1][1];
        assert_equals(cursor && cursor.primaryKey, expected, `move ${step}`);
      }
      if (step == moves.length) return resolve();
      moves[step++][0](cursor);
      assert_equals(request.readyState, 'pending');
    });
    request.onerror = () => reject(request.error);
  });
}, 'continue(key), continuePrimaryKey() and advance() within and beyond the records taken');

promise_test(async t => {
  const db = await openDatabase(t, createItems(20));
  const store = db.transaction('items').objectStore('items');
  const order = [];
  await visit(store.openCursor(), (cursor, steps) => {
    order.push('cursor ' + cursor.key);
    if (steps == 6) {
      store.get(1).onsuccess = () => order.push('get');
    }
    if (steps == 7) {
      cursor.continue();
      store.count().onsuccess = () => order.push('count');
      return 'moved';
    }
    if (steps == 9) return 'stop';
  });
  assert_array_equals(order, ['cursor 1', 'cursor 2', 'cursor 3', 'cursor 4', 'cursor 5',
                              'cursor 6', 'get', 'cursor 7', 'cursor 8', 'count', 'cursor 9']);
}, 'Steps taken from the records of an earlier step complete in request order');

promise_test(async t => {
  const db = await openDatabase(t, createItems(20));
  const transaction = db.transaction('items', 'readwrite');
  const store = transaction.objectStore('items');
  let cursor;
  await visit(store.openCursor(), (c, steps) => {
    cursor = c;
    if (steps == 6) return 'stop';
  });
  await new Promise(resolve => setTimeout(resolve, 0));
  assert_throws_dom('TransactionInactiveError', () => cursor.continue(), 'in a later task');
  assert_throws_dom('TransactionInactiveError', () => cursor.advance(1), 'in a later task');

  const aborted = db.transaction('items', 'readwrite');
  await visit(aborted.objectStore('items').openCursor(), (cursor, steps) => {
    if (steps == 6) {
      aborted.abort();
      assert_throws_dom('TransactionInactiveError', () => cursor.continue(), 'after abort()');
      return 'stop';
    }
  });
}, 'Records taken from an earlier step do not make an inactive transaction usable');

promise_test(async t => {
  const db = await openDatabase(t, createItems(20));
  const transaction = db.transaction('items', 'readwrite');
  const store = transaction.objectStore('items');
  const visited = await visit(store.openCursor(), cursor => {
    if (cursor.key == 6) store.put({n: 6.5, k: 0}, 6.5);
    if (cursor.key == 8) {
      transaction.abort();
      return 'stop';
    }
  });
  assert_array_equals(visited, [1, 2, 3, 4, 5, 6, 6.5, 7, 8]);
  await new Promise(resolve => transaction.onabort = resolve);
  const after = await visit(db.transaction('items').objectStore('items').openCursor(), () => {});
  assert_equals(after.length, 20, 'the aborted put is gone');
  assert_false(after.includes(6.5));
}, 'An aborted transaction leaves nothing that a later cursor sees');

promise_test(async t => {
  const size = 300 * 1024;
  const db = await openDatabase(t, db => {
    const store = db.createObjectStore('items');
    for (let n = 1; n <= 24; n++) {
      store.put(new Uint8Array(size).fill(n), n);
    }
  });
  const store = db.transaction('items').objectStore('items');
  const visited = await visit(store.openCursor(), cursor => {
    assert_equals(cursor.value.length, size);
    assert_equals(cursor.value[size - 1], cursor.key);
  });
  assert_array_equals(visited, Array.from({length: 24}, (_, i) => i + 1));
}, 'A cursor over large values visits every record');

promise_test(async t => {
  const db = await openDatabase(t, db => {
    createItems(60)(db);
    db.createObjectStore('copies');
  });
  const transaction = db.transaction(['items', 'copies'], 'readwrite');
  const items = transaction.objectStore('items');
  const copies = transaction.objectStore('copies');
  const visited = await visit(items.openCursor(), cursor => {
    copies.put(cursor.value, cursor.key);
    if (cursor.key == 20) {
      items.put({n: 20.5, k: 0}, 20.5);
      items.delete(22);
    }
    if (cursor.key == 40) copies.clear();
  });
  const expected = [];
  for (let n = 1; n <= 60; n++) {
    if (n != 22) expected.push(n);
    if (n == 20) expected.push(20.5);
  }
  assert_array_equals(visited, expected);
  const byIndex = await visit(items.index('k').openCursor(IDBKeyRange.only(1)), cursor => {
    copies.put(cursor.value, -cursor.primaryKey);
    if (cursor.primaryKey == 15) items.put({n: 16, k: 1}, 16);
  });
  assert_array_equals(byIndex, [1, 8, 15, 16, 29, 36, 43, 50, 57]);
  const copied = await new Promise((resolve, reject) => {
    const request = copies.getAllKeys();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  assert_equals(copied.length, 20 + byIndex.length, 'the copies made after clear()');
}, 'Writes to another store between the steps of a cursor, and to its own store');
