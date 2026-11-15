// META: global=window,worker
// META: title=IndexedDB: the connection of an aborted upgrade is closed

// https://w3c.github.io/IndexedDB/#open-a-database-connection: after an aborted upgrade the open
// request fails with AbortError and its connection is closed (step 10.8), so transaction()
// throws InvalidStateError. Other connections to the database keep working.

'use strict';

function requestDone(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = event => {
      event.preventDefault();
      reject(request.error);
    };
  });
}

function transactionDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => reject(transaction.error);
  });
}

promise_test(async t => {
  const name = 'connection-after-aborted-upgrade-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  const first = indexedDB.open(name, 1);
  first.onupgradeneeded = () => {
    first.result.createObjectStore('store').put('value', 'key');
  };
  (await requestDone(first)).close();

  let stale;
  let thrownInAbortHandler;
  const upgrade = indexedDB.open(name, 2);
  upgrade.onupgradeneeded = t.step_func(() => {
    stale = upgrade.result;
    const transaction = upgrade.transaction;
    transaction.onabort = () => {
      try {
        stale.transaction('store', 'readonly');
      } catch (e) {
        thrownInAbortHandler = e.name;
      }
    };
    transaction.abort();
  });
  await promise_rejects_dom(t, 'AbortError', requestDone(upgrade));
  assert_equals(thrownInAbortHandler, 'InvalidStateError',
                'transaction() throws in the abort handler of the upgrade transaction');
  assert_equals(stale.version, 1, 'The version is reverted');
  assert_throws_dom('InvalidStateError', () => stale.transaction('store', 'readwrite'),
                    'transaction() throws after the open request failed');

  const fresh = await requestDone(indexedDB.open(name));
  t.add_cleanup(() => fresh.close());
  assert_equals(fresh.version, 1);
  const writes = fresh.transaction('store', 'readwrite');
  writes.objectStore('store').put('fresh', 'other');
  await transactionDone(writes);
  const reads = fresh.transaction('store', 'readonly');
  const get = reads.objectStore('store').get('other');
  await transactionDone(reads);
  assert_equals(get.result, 'fresh');
}, 'The connection of an aborted upgrade is closed, and other connections keep working');
