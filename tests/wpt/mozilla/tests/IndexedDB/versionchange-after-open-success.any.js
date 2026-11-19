// META: global=window,worker
// META: title=IndexedDB: versionchange comes after the success of the connection's open request
// META: timeout=long

// https://w3c.github.io/IndexedDB/#open-a-database-connection: of two opens of an existing
// database made in one task, the first succeeds without an upgrade, and the second, for a higher
// version, then fires versionchange at the first's connection. The connection exists by then, so
// it gets the event, closes, and the second open proceeds without being blocked. The backend
// sends the first open's success and the versionchange back to back; they must arrive in order,
// also when they cross processes (run with --multiprocess).

'use strict';

const ROUNDS = 25;

function requestDone(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = event => {
      event.preventDefault();
      reject(request.error);
    };
  });
}

promise_test(async t => {
  for (let round = 0; round < ROUNDS; round++) {
    const name = `versionchange-after-open-success-${round}-${Math.random()}`;
    t.add_cleanup(() => {
      indexedDB.deleteDatabase(name);
    });
    const create = indexedDB.open(name, 1);
    create.onupgradeneeded = () => create.result.createObjectStore('store');
    (await requestDone(create)).close();

    const events = [];
    const first = indexedDB.open(name, 1);
    const second = indexedDB.open(name, 2);
    first.onsuccess = () => {
      events.push('first success');
      first.result.onversionchange = event => {
        events.push(`versionchange ${event.oldVersion} ${event.newVersion}`);
        first.result.close();
      };
    };
    second.onblocked = () => events.push('second blocked');
    second.onupgradeneeded = () => events.push('second upgradeneeded');
    const db = await requestDone(second);
    db.close();
    assert_array_equals(events, [
      'first success',
      'versionchange 1 2',
      'second upgradeneeded',
    ], `Events of round ${round}`);
  }
}, 'versionchange reaches a connection after its open request succeeded');
