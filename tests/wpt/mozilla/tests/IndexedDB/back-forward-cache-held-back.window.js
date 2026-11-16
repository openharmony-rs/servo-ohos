// META: title=IndexedDB: a cached page that holds back another page's request or transaction is evicted
// META: script=/common/dispatcher/dispatcher.js
// META: script=/common/utils.js
// META: script=/IndexedDB/resources/support.js
// META: script=/html/browsers/browsing-the-web/back-forward-cache/resources/rc-helper.js
// META: script=/html/browsers/browsing-the-web/remote-context-helper/resources/remote-context-helper.js
// META: timeout=long

'use strict';

// A page in the back/forward cache never finishes its transactions or upgrades, as its tasks are
// held back. When another page waits for one of them, or for a connection that closes only once
// they have finished, Servo evicts the cached page, as Chromium does (DisallowInactiveClient).
// The cached pages write or open on pagehide, as WPT's back-forward-cache-open-transaction does.

// Opens `name` with a store in `rc`; on pagehide, the page writes to the store and, if `close`
// is set, closes the connection while the write's transaction is still alive.
function writeOnPagehide(rc, name, close) {
  return rc.executeScript((name, close) => new Promise(resolve => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => request.result.createObjectStore('store');
    request.onsuccess = () => {
      const db = request.result;
      addEventListener('pagehide', () => {
        db.transaction('store', 'readwrite').objectStore('store').put('pagehide', 'key');
        if (close)
          db.close();
      });
      resolve();
    };
  }), [name, close]);
}

// The events of a delete of `name` in `rc`.
function deleteDatabase(rc, name) {
  return rc.executeScript(name => new Promise((resolve, reject) => {
    const events = [];
    const request = indexedDB.deleteDatabase(name);
    request.onblocked = event => events.push(`blocked ${event.oldVersion}`);
    request.onsuccess = event => {
      events.push(`success ${event.oldVersion}`);
      resolve(events.join(', '));
    };
    request.onerror = () => reject(request.error);
  }), [name]);
}

// The events of an open of `name` at `version`, or without one if it is null, in `rc`.
function openDatabase(rc, name, version) {
  return rc.executeScript((name, version) => new Promise((resolve, reject) => {
    const events = [];
    const request = version ? indexedDB.open(name, version) : indexedDB.open(name);
    request.onblocked = event => events.push(`blocked ${event.oldVersion}`);
    request.onupgradeneeded = event => events.push(`upgradeneeded ${event.oldVersion}`);
    request.onsuccess = () => {
      events.push(`success ${request.result.version}`);
      request.result.close();
      resolve(events.join(', '));
    };
    request.onerror = () => reject(request.error);
  }), [name, version]);
}

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await writeOnPagehide(rc1, name, true);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  assert_equals(await deleteDatabase(rc2, name), 'blocked 1, success 1');

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'A delete evicts the cached page whose closing connection still writes');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await writeOnPagehide(rc1, name, true);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  assert_equals(
      await openDatabase(rc2, name, 2), 'blocked 1, upgradeneeded 1, success 2');

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'An upgrade evicts the cached page whose closing connection still writes');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await writeOnPagehide(rc1, name, false);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  const value = await rc2.executeScript(name => new Promise((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const db = request.result;
      const get = db.transaction('store', 'readonly').objectStore('store').get('key');
      get.onsuccess = () => {
        db.close();
        resolve(String(get.result));
      };
      get.onerror = () => reject(get.error);
    };
    request.onerror = () => reject(request.error);
  }), [name]);
  assert_equals(value, 'undefined', 'The evicted page\'s write is rolled back');

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'A read evicts the cached page whose transaction writes to the store');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await rc1.executeScript(name => {
    addEventListener('pagehide', () => {
      const request = indexedDB.open(name, 1);
      // Keeps the upgrade transaction busy for as long as the page runs.
      request.onupgradeneeded = () => {
        const store = request.result.createObjectStore('store');
        const spin = () => {
          store.get(0).onsuccess = spin;
        };
        spin();
      };
    });
  }, [name]);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  assert_equals(await openDatabase(rc2, name, null), 'upgradeneeded 0, success 1');

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'An open evicts the cached page whose upgrade it waits for');
