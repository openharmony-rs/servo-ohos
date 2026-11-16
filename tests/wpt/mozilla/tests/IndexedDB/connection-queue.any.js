// META: global=window,dedicatedworker,sharedworker
// META: title=IndexedDB: connection queues, versionchange, blocked and close pending
// META: timeout=long

// https://w3c.github.io/IndexedDB/#connection-queue: open and delete requests for a database are
// processed one after another, in the order they were made. A request that needs the other
// connections closed (a higher version, or a delete) fires versionchange at those whose close
// isn't pending, then blocked at itself if any is still open, and proceeds once all have closed,
// which a connection with transactions does when they have finished.

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

async function createDatabase(t, version = 1) {
  const name = 'connection-queue-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  const request = indexedDB.open(name, version);
  request.onupgradeneeded = () => request.result.createObjectStore('store');
  return [name, await requestDone(request)];
}

function versionChangeText(prefix, event) {
  return `${prefix} ${event.oldVersion} ${event.newVersion}`;
}

promise_test(async t => {
  const [name, first] = await createDatabase(t);
  const second = await requestDone(indexedDB.open(name));
  const third = await requestDone(indexedDB.open(name));
  const events = [];
  first.onversionchange = event => {
    events.push(versionChangeText('first versionchange', event));
    first.close();
  };
  second.onversionchange = event => {
    events.push(versionChangeText('second versionchange', event));
  };
  third.onversionchange = event => {
    events.push(versionChangeText('third versionchange', event));
    third.close();
  };
  const upgrade = indexedDB.open(name, 2);
  upgrade.onblocked = event => {
    events.push(versionChangeText('blocked', event));
    second.close();
  };
  upgrade.onupgradeneeded = event => events.push(versionChangeText('upgradeneeded', event));
  const db = await requestDone(upgrade);
  db.close();
  assert_array_equals(events, [
    'first versionchange 1 2',
    'second versionchange 1 2',
    'third versionchange 1 2',
    'blocked 1 2',
    'upgradeneeded 1 2',
  ]);
}, 'versionchange reaches every connection in the order they were opened, then blocked fires');

promise_test(async t => {
  const [name, db] = await createDatabase(t, 3);
  const put = db.transaction('store', 'readwrite');
  put.objectStore('store').put('value', 'key');
  await transactionDone(put);

  const events = [];
  db.onversionchange = event => events.push(versionChangeText('versionchange', event));
  const deletion = indexedDB.deleteDatabase(name);
  deletion.onblocked = event => {
    events.push(versionChangeText('blocked', event));
    db.close();
  };
  const deleted = await new Promise((resolve, reject) => {
    deletion.onsuccess = resolve;
    deletion.onerror = () => reject(deletion.error);
  });
  events.push(versionChangeText('success', deleted));
  assert_array_equals(events, ['versionchange 3 null', 'blocked 3 null', 'success 3 null']);

  const reopen = indexedDB.open(name);
  let oldVersion;
  reopen.onupgradeneeded = event => oldVersion = event.oldVersion;
  const reopened = await requestDone(reopen);
  t.add_cleanup(() => reopened.close());
  assert_equals(oldVersion, 0, 'the deleted database is created again');
  assert_equals(reopened.version, 1);
  assert_array_equals([...reopened.objectStoreNames], []);
}, 'deleteDatabase() fires versionchange, waits for the connections to close, and deletes');

promise_test(async t => {
  const name = 'connection-queue-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  const events = [];
  const record = text => () => events.push(text);
  const done = [];
  const expectOpen = (label, version, onSuccess) => {
    const request = indexedDB.open(name, version);
    request.onblocked = record(`${label} blocked`);
    request.onupgradeneeded = event => events.push(versionChangeText(`${label} upgradeneeded`, event));
    done.push(requestDone(request).then(db => {
      events.push(`${label} success`);
      db.onversionchange = event => {
        events.push(versionChangeText(`${label} versionchange`, event));
        t.step_timeout(() => db.close(), 0);
      };
      if (onSuccess) {
        onSuccess(db);
      }
    }));
  };
  const expectDelete = label => {
    const request = indexedDB.deleteDatabase(name);
    request.onblocked = record(`${label} blocked`);
    done.push(new Promise((resolve, reject) => {
      request.onsuccess = event => {
        events.push(versionChangeText(`${label} success`, event));
        resolve();
      };
      request.onerror = () => reject(request.error);
    }));
  };

  expectOpen('open1', 1);
  expectDelete('delete1');
  expectOpen('open2', 2);
  expectOpen('open3', 2);
  expectOpen('open4', 3);
  expectDelete('delete2');
  await Promise.all(done);
  assert_array_equals(events, [
    'open1 upgradeneeded 0 1',
    'open1 success',
    'open1 versionchange 1 null',
    'delete1 blocked',
    'delete1 success 1 null',
    'open2 upgradeneeded 0 2',
    'open2 success',
    'open3 success',
    'open2 versionchange 2 3',
    'open3 versionchange 2 3',
    'open4 blocked',
    'open4 upgradeneeded 2 3',
    'open4 success',
    'open4 versionchange 3 null',
    'delete2 blocked',
    'delete2 success 3 null',
  ]);
}, 'Interleaved opens and deletes are processed in order');

promise_test(async t => {
  const name = 'connection-queue-' + Math.random();
  t.add_cleanup(() => {
    indexedDB.deleteDatabase(name);
  });
  const events = [];
  const request = indexedDB.open(name, 1);
  request.onupgradeneeded = () => {
    const db = request.result;
    db.createObjectStore('store').put('value', 'key');
    db.close();
    assert_throws_dom('InvalidStateError', () => db.transaction('store'));
    request.transaction.oncomplete = () => events.push('complete');
    request.transaction.onabort = () => events.push('abort');
  };
  const error = await requestDone(request).then(
      () => assert_unreached('the open must fail'), error => error);
  events.push(`error ${error.name}`);
  assert_array_equals(events, ['complete', 'error AbortError']);

  const reopened = await requestDone(indexedDB.open(name));
  t.add_cleanup(() => reopened.close());
  assert_equals(reopened.version, 1, 'the upgrade committed');
  const read = reopened.transaction('store').objectStore('store').get('key');
  assert_equals(await requestDone(read), 'value');
}, 'A connection closed during its upgrade commits it, and the open fails with AbortError');

promise_test(async t => {
  const [name, db] = await createDatabase(t);
  const events = [];
  db.onversionchange = () => events.push('versionchange');
  const transaction = db.transaction('store', 'readwrite');
  const store = transaction.objectStore('store');
  const writes = 200;
  for (let i = 0; i < writes; i++) {
    store.put('x'.repeat(1000), i);
  }
  transaction.oncomplete = () => events.push('complete');
  db.close();

  const upgrade = indexedDB.open(name, 2);
  upgrade.onupgradeneeded = () => {
    events.push('upgradeneeded');
    const count = upgrade.transaction.objectStore('store').count();
    count.onsuccess = () => events.push(`count ${count.result}`);
  };
  (await requestDone(upgrade)).close();
  assert_array_equals(events, ['complete', 'upgradeneeded', `count ${writes}`]);
}, 'An upgrade waits for the transactions of a connection whose close is pending, which gets ' +
   'no versionchange event');

promise_test(async t => {
  const [name, db] = await createDatabase(t);
  const events = [];
  db.onversionchange = () => {
    events.push('versionchange');
    db.close();
  };
  const first = indexedDB.deleteDatabase(name);
  const second = indexedDB.deleteDatabase(name);
  first.onblocked = () => events.push('first blocked');
  second.onblocked = () => events.push('second blocked');
  const versions = await Promise.all([first, second].map(request => new Promise((resolve, reject) => {
    request.onsuccess = event => resolve(event.oldVersion);
    request.onerror = () => reject(request.error);
  })));
  assert_array_equals(versions, [1, 0], 'the second delete finds no database');
  assert_array_equals(events, ['versionchange']);
}, 'A connection that closes in its versionchange handler doesn\'t block a delete');
