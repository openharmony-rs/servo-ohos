// META: global=window,worker
// META: title=IndexedDB: request results are delivered in request order
// META: timeout=long

// https://w3c.github.io/IndexedDB/#transaction-lifetime: "results must be returned in the order
// the requests were placed against a specific transaction". A large result must not let the
// small results of later requests overtake it, and results that are on their way when the
// transaction aborts are discarded.

'use strict';

const LARGE_VALUE_SIZE = 256 * 1024;
const LARGE_VALUE_COUNT = 8;

function largeValue(seed) {
  const value = new Uint8Array(LARGE_VALUE_SIZE);
  for (let i = 0; i < value.length; i += 4096) {
    value[i] = (seed + i) % 256;
  }
  return value;
}

// Opens a new database with a store `large` holding LARGE_VALUE_COUNT large values under the keys
// 0.. and a store `small` holding 'small' under the key 1.
function openDatabase(t) {
  const name = 'request-result-order-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = t.step_func(() => {
      const db = request.result;
      const large = db.createObjectStore('large');
      for (let i = 0; i < LARGE_VALUE_COUNT; i++) {
        large.put(largeValue(i), i);
      }
      db.createObjectStore('small').put('small', 1);
    });
    request.onsuccess = () => {
      t.add_cleanup(() => {
        request.result.close();
      });
      resolve(request.result);
    };
    request.onerror = () => reject(request.error);
  });
}

// Records the events that `request` fires as `${label} ${type}` in `events`.
function record(request, label, events) {
  request.addEventListener('success', () => events.push(`${label} success`));
  request.addEventListener('error', event => {
    event.preventDefault();
    events.push(`${label} error ${request.error.name}`);
  });
}

function transactionDone(transaction, events) {
  return new Promise(resolve => {
    transaction.oncomplete = () => {
      events.push('complete');
      resolve();
    };
    transaction.onabort = () => {
      events.push('abort');
      resolve();
    };
  });
}

promise_test(async t => {
  const db = await openDatabase(t);
  const transaction = db.transaction(['large', 'small'], 'readonly');
  const large = transaction.objectStore('large');
  const small = transaction.objectStore('small');
  const events = [];
  const getAll = large.getAll();
  record(getAll, 'getAll large', events);
  record(small.get(1), 'get small', events);
  record(small.count(), 'count small', events);
  record(large.get(LARGE_VALUE_COUNT - 1), 'get large', events);
  record(small.getKey(1), 'getKey small', events);
  await transactionDone(transaction, events);

  assert_array_equals(events, [
    'getAll large success',
    'get small success',
    'count small success',
    'get large success',
    'getKey small success',
    'complete',
  ]);
  assert_equals(getAll.result.length, LARGE_VALUE_COUNT);
  assert_equals(getAll.result[3].length, LARGE_VALUE_SIZE);
  assert_equals(getAll.result[3][4096], (3 + 4096) % 256);
}, 'A large getAll completes before the small requests made after it');

promise_test(async t => {
  const db = await openDatabase(t);
  const transaction = db.transaction(['large', 'small'], 'readwrite');
  const large = transaction.objectStore('large');
  const small = transaction.objectStore('small');
  const events = [];
  record(large.put(largeValue(100), 100), 'put large', events);
  record(small.get(1), 'get small', events);
  record(small.add('again', 1), 'add existing', events);
  record(large.getAll(), 'getAll large', events);
  record(small.put('second', 2), 'put small', events);
  record(small.count(), 'count small', events);
  await transactionDone(transaction, events);

  assert_array_equals(events, [
    'put large success',
    'get small success',
    'add existing error ConstraintError',
    'getAll large success',
    'put small success',
    'count small success',
    'complete',
  ]);
}, 'Reads, writes and a failing request of a readwrite transaction complete in request order');

promise_test(async t => {
  const db = await openDatabase(t);
  const transaction = db.transaction(['large', 'small'], 'readonly');
  const large = transaction.objectStore('large');
  const small = transaction.objectStore('small');
  const events = [];
  const first = small.get(1);
  const later = [
    ['getAll large', large.getAll()],
    ['get small', small.get(1)],
    ['count small', small.count()],
    ['get large', large.get(0)],
  ];
  first.onsuccess = () => {
    events.push('first success');
    transaction.abort();
  };
  for (const [label, request] of later) {
    record(request, label, events);
  }
  await transactionDone(transaction, events);

  assert_array_equals(events, [
    'first success',
    'getAll large error AbortError',
    'get small error AbortError',
    'count small error AbortError',
    'get large error AbortError',
    'abort',
  ]);
  for (const [label, request] of later) {
    assert_equals(request.readyState, 'done', `${label} is done`);
    assert_equals(request.result, undefined, `${label} has no result`);
  }
}, 'An abort while results are on their way fails the pending requests in order, then aborts');

promise_test(async t => {
  const db = await openDatabase(t);
  const transaction = db.transaction(['large', 'small'], 'readwrite');
  const large = transaction.objectStore('large');
  const small = transaction.objectStore('small');
  const events = [];
  record(large.getAll(), 'getAll large', events);
  record(small.put('committed', 3), 'put small', events);
  record(small.count(), 'count small', events);
  transaction.commit();
  await transactionDone(transaction, events);

  assert_array_equals(events, [
    'getAll large success',
    'put small success',
    'count small success',
    'complete',
  ]);

  const check = db.transaction('small', 'readonly');
  const value = check.objectStore('small').get(3);
  await transactionDone(check, []);
  assert_equals(value.result, 'committed');
}, 'commit() lets the pending requests complete in order before the transaction completes');

promise_test(async t => {
  const db = await openDatabase(t);
  const transaction = db.transaction('large', 'readonly');
  const large = transaction.objectStore('large');
  const events = [];
  const cursorRequest = large.openCursor();
  let steps = 0;
  cursorRequest.onsuccess = () => {
    const cursor = cursorRequest.result;
    if (!cursor) {
      events.push('cursor end');
      return;
    }
    events.push(`cursor ${cursor.key}`);
    if (steps++ == 0) {
      record(large.getAll(), 'getAll large', events);
      record(large.count(), 'count large', events);
    }
    cursor.continue();
  };
  await transactionDone(transaction, events);

  const expected = ['cursor 0', 'getAll large success', 'count large success'];
  for (let key = 1; key < LARGE_VALUE_COUNT; key++) {
    expected.push(`cursor ${key}`);
  }
  expected.push('cursor end', 'complete');
  assert_array_equals(events, expected);
}, 'A cursor that continues after other requests completes after them');
