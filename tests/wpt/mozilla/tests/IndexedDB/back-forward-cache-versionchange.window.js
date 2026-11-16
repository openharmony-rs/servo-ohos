// META: title=IndexedDB: versionchange at a connection in the back/forward cache evicts its page
// META: script=/common/dispatcher/dispatcher.js
// META: script=/common/utils.js
// META: script=/common/get-host-info.sub.js
// META: script=/IndexedDB/resources/support.js
// META: script=/html/browsers/browsing-the-web/back-forward-cache/resources/rc-helper.js
// META: script=/html/browsers/browsing-the-web/remote-context-helper/resources/remote-context-helper.js
// META: timeout=long

'use strict';

// A page in the back/forward cache can't fire a versionchange event, so a connection it holds
// would block upgrades and deletes until the page is shown again. Servo evicts the page instead,
// like Gecko and Chromium: the page's connections close, and going back loads it again.

function deleteDatabase(rc, name) {
  return rc.executeScript(name => new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = event => resolve(`deleted ${event.oldVersion}`);
    request.onblocked = () => resolve('blocked');
    request.onerror = () => reject(request.error);
  }), [name]);
}

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await waitUntilIndexedDBOpenForTesting(rc1, name, 1);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  assert_equals(await deleteDatabase(rc2, name), 'deleted 1');

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'A delete evicts the cached page whose connection it waits for');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const frame = await rc1.addIframe();
  const name = t.name + Math.random();
  await waitUntilIndexedDBOpenForTesting(frame, name, 1);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  await waitUntilIndexedDBOpenForTesting(rc2, name, 2);

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'An upgrade evicts the cached page whose iframe holds a connection');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const frame = await rc1.addIframe({origin: 'HTTP_NOTSAMESITE_ORIGIN'});
  const name = t.name + Math.random();
  await waitUntilIndexedDBOpenForTesting(frame, name, 1);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  const otherFrame = await rc2.addIframe({origin: 'HTTP_NOTSAMESITE_ORIGIN'});
  await waitUntilIndexedDBOpenForTesting(otherFrame, name, 2);

  await rc2.historyBack();
  await assertNotRestoredFromBFCache(rc1);
}, 'An upgrade evicts the cached page whose cross-site iframe holds a connection');

promise_test(async t => {
  const rcHelper = new RemoteContextHelper();
  const rc1 = await rcHelper.addWindow(null, {features: 'noopener'});
  const name = t.name + Math.random();
  await waitUntilIndexedDBOpenForTesting(rc1, name, 1);

  await prepareForBFCache(rc1);
  const rc2 = await rc1.navigateToNew();
  await rc2.historyBack();
  await assertImplementsBFCacheOptional(rc1);
}, 'A cached page with an open connection is restored when nothing waits for it');
