/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// Inspects the sqlite registry file directly, so it only applies where sqlite
// is unambiguously the active backend — with the RDB backend compiled in the
// choice is a runtime one.
#![cfg(all(feature = "sqlite-backend", not(ohos_rdb)))]

use std::path::PathBuf;

use rusqlite::Connection;
use servo_base::generic_channel;
use servo_base::generic_channel::GenericCallback;
use servo_base::id::{
    BrowsingContextId, PIPELINE_NAMESPACE, PipelineNamespace, PipelineNamespaceId, WebViewId,
};
use servo_url::ServoUrl;
use storage::ClientStorageThreadFactory;
use storage_traits::client_storage::{
    ClientStorageThreadHandle, ClientStorageThreadMessage, StorageIdentifier, StorageProxyMap,
    StorageType,
};
use storage_traits::indexeddb::Utf16String;

fn install_test_namespace() {
    if PIPELINE_NAMESPACE.get().is_none() {
        PipelineNamespace::install(PipelineNamespaceId(1));
    }
}

fn registry_db_path(tmp_dir: &tempfile::TempDir) -> PathBuf {
    tmp_dir
        .path()
        .join("clientstorage")
        .join("default_v1")
        .join("reg.sqlite")
}

fn open_registry(tmp_dir: &tempfile::TempDir) -> Connection {
    Connection::open(registry_db_path(tmp_dir)).expect("Registry database should exist")
}

fn obtain_bottle_map(
    handle: &ClientStorageThreadHandle,
    storage_type: StorageType,
    webview: Option<WebViewId>,
    storage_identifier: StorageIdentifier,
    origin: servo_url::ImmutableOrigin,
) -> StorageProxyMap {
    handle
        .obtain_a_storage_bottle_map(storage_type, webview, storage_identifier, origin)
        .recv()
        .unwrap()
        .unwrap()
}

#[test]
fn test_exit() {
    let handle: ClientStorageThreadHandle = ClientStorageThreadFactory::new(None, false);

    let (sender, receiver) = generic_channel::channel().unwrap();
    handle
        .send(ClientStorageThreadMessage::Exit(sender))
        .unwrap();
    receiver.recv().unwrap();

    // Workaround for https://github.com/servo/servo/issues/32912
    #[cfg(windows)]
    std::thread::sleep(std::time::Duration::from_millis(1000));
}

/// Requests to a client storage thread that has exited report the disconnection.
#[test]
fn test_requests_after_exit_fail() {
    let handle: ClientStorageThreadHandle = ClientStorageThreadFactory::new(None, false);
    let (sender, receiver) = generic_channel::channel().unwrap();
    handle
        .send(ClientStorageThreadMessage::Exit(sender))
        .unwrap();
    receiver.recv().unwrap();

    assert!(handle.find_database(1, "db").recv().is_err());
    assert!(handle.list_databases(1).recv().is_err());
    assert!(handle.create_database(1, "db").recv().is_err());
    assert!(handle.delete_database(1, "db").recv().is_err());
}

#[test]
fn test_workflow() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);

    let url = ServoUrl::parse("https://example.com").unwrap();

    // Obtain a first storage proxy map.
    let storage_proxy_map = handle
        .obtain_a_storage_bottle_map(
            StorageType::Local,
            Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID)),
            StorageIdentifier::IndexedDB,
            url.origin(),
        )
        .recv()
        .unwrap()
        .unwrap();

    // Create a db.
    let receiver = handle.create_database(storage_proxy_map.bottle_id, "test1".to_string());
    let (path, created) = receiver.recv().unwrap().expect("Path should be created");
    assert!(created);

    assert!(std::fs::read_dir(path.clone()).is_ok());

    // Create another db with the same name.
    let receiver = handle.create_database(storage_proxy_map.bottle_id, "test1".to_string());
    let (path, created) = receiver.recv().unwrap().expect("Path should be created");
    assert!(!created);

    // Create another db with a different same.
    let receiver = handle.create_database(storage_proxy_map.bottle_id, "test2".to_string());
    let (yet_another_path, created) = receiver.recv().unwrap().expect("Path should be created");
    assert_ne!(path, yet_another_path);
    assert!(created);

    // Delete the dbs.
    let receiver = handle.delete_database(storage_proxy_map.bottle_id, "test1".to_string());
    receiver.recv().unwrap().expect("Db should be deleted");
    let receiver = handle.delete_database(storage_proxy_map.bottle_id, "test2".to_string());
    receiver.recv().unwrap().expect("Db should be deleted");

    assert!(std::fs::read_dir(path).is_err());

    // Get another proxy map fro the same parameters.
    let second_proxy_map = handle
        .obtain_a_storage_bottle_map(
            StorageType::Local,
            Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID)),
            StorageIdentifier::IndexedDB,
            url.origin(),
        )
        .recv()
        .unwrap()
        .unwrap();

    // Bottle id should be the same, because the manager returned the existing id.
    assert_eq!(second_proxy_map.bottle_id, storage_proxy_map.bottle_id);

    // Workaround for https://github.com/servo/servo/issues/32912
    #[cfg(windows)]
    std::thread::sleep(std::time::Duration::from_millis(1000));
}

/// A database name with an unpaired surrogate names its own database, and a well-formed name
/// keeps its registry entry as text.
#[test]
fn test_database_names_with_unpaired_surrogates() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);
    let bottle_id = obtain_bottle_map(
        &handle,
        StorageType::Local,
        Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID)),
        StorageIdentifier::IndexedDB,
        ServoUrl::parse("https://example.com").unwrap().origin(),
    )
    .bottle_id;

    let names = [
        Utf16String::from_units(vec![0xD800]),
        Utf16String::from_units(vec![0xDC00]),
        Utf16String::from("\u{FFFD}"),
        Utf16String::from("plain"),
    ];
    let create = |name: &Utf16String| {
        handle
            .create_database(bottle_id, name.clone())
            .recv()
            .unwrap()
            .unwrap()
    };
    let paths: Vec<PathBuf> = names
        .iter()
        .map(|name| {
            let (path, created) = create(name);
            assert!(created);
            path
        })
        .collect();
    for (index, path) in paths.iter().enumerate() {
        assert!(!paths[..index].contains(path));
    }
    assert_eq!(create(&names[0]), (paths[0].clone(), false));

    let registry = open_registry(&tmp_dir);
    let stored: Vec<(String, Vec<u8>)> = registry
        .prepare("SELECT typeof(name), CAST(name AS BLOB) FROM databases ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        stored,
        vec![
            ("blob".to_owned(), vec![0xD8, 0x00]),
            ("blob".to_owned(), vec![0xDC, 0x00]),
            ("text".to_owned(), "\u{FFFD}".as_bytes().to_vec()),
            ("text".to_owned(), b"plain".to_vec()),
        ]
    );

    handle
        .delete_database(bottle_id, names[0].clone())
        .recv()
        .unwrap()
        .unwrap();
    assert!(!paths[0].exists());
    assert!(paths[1..].iter().all(|path| path.exists()));
}

/// The registry finds and lists the databases of a bottle, also those whose names it holds as
/// BLOBs, and no longer once they are deleted.
#[test]
fn test_find_and_list_databases() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);
    let bottle_of = |url: &str| {
        obtain_bottle_map(
            &handle,
            StorageType::Local,
            Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID)),
            StorageIdentifier::IndexedDB,
            ServoUrl::parse(url).unwrap().origin(),
        )
        .bottle_id
    };
    let (bottle, other_bottle) = (
        bottle_of("https://example.com"),
        bottle_of("https://example.org"),
    );
    let create = |bottle_id: i64, name: &Utf16String| {
        handle
            .create_database(bottle_id, name.clone())
            .recv()
            .unwrap()
            .unwrap()
            .0
    };
    let find = |bottle_id: i64, name: &Utf16String| {
        handle
            .find_database(bottle_id, name.clone())
            .recv()
            .unwrap()
            .unwrap()
    };
    let list = |bottle_id: i64| handle.list_databases(bottle_id).recv().unwrap().unwrap();

    let names = [
        Utf16String::from("one"),
        Utf16String::from_units(vec![0xD800, u16::from(b'x')]),
        Utf16String::from("two"),
    ];
    let other = Utf16String::from("other");
    let paths: Vec<PathBuf> = names.iter().map(|name| create(bottle, name)).collect();
    let other_path = create(other_bottle, &other);

    assert_eq!(find(bottle, &names[1]), Some(paths[1].clone()));
    assert_eq!(find(bottle, &Utf16String::from("missing")), None);
    assert_eq!(find(bottle, &other), None);
    assert_eq!(find(other_bottle, &other), Some(other_path.clone()));
    let listed: Vec<(Utf16String, PathBuf)> =
        names.iter().cloned().zip(paths.iter().cloned()).collect();
    assert_eq!(list(bottle), listed);
    assert_eq!(list(other_bottle), vec![(other, other_path)]);

    handle
        .delete_database(bottle, names[0].clone())
        .recv()
        .unwrap()
        .unwrap();
    assert_eq!(find(bottle, &names[0]), None);
    assert_eq!(list(bottle), listed[1..].to_vec());
}

#[test]
fn test_repeated_local_obtain_reuses_same_logical_rows() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);

    let origin = ServoUrl::parse("https://example.com").unwrap().origin();
    let webview = Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID));

    let first = obtain_bottle_map(
        &handle,
        StorageType::Local,
        webview,
        StorageIdentifier::IndexedDB,
        origin.clone(),
    );
    let second = obtain_bottle_map(
        &handle,
        StorageType::Local,
        webview,
        StorageIdentifier::IndexedDB,
        origin.clone(),
    );

    assert_eq!(first.bottle_id, second.bottle_id);

    let registry = open_registry(&tmp_dir);
    let local_shed_count: i64 = registry
        .query_row(
            "SELECT COUNT(*) FROM sheds WHERE storage_type = 'local' AND browsing_context IS NULL;",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let shelf_count: i64 = registry
        .query_row(
            "SELECT COUNT(*) FROM shelves WHERE origin = ?1;",
            [origin.ascii_serialization().into_owned()],
            |row| row.get(0),
        )
        .unwrap();
    let bucket_count: i64 = registry
        .query_row("SELECT COUNT(*) FROM buckets;", [], |row| row.get(0))
        .unwrap();
    let bottle_count: i64 = registry
        .query_row("SELECT COUNT(*) FROM bottles;", [], |row| row.get(0))
        .unwrap();

    assert_eq!(local_shed_count, 1);
    assert_eq!(shelf_count, 1);
    assert_eq!(bucket_count, 1);
    assert_eq!(bottle_count, 4);
}

#[test]
fn test_repeated_session_obtain_reuses_same_logical_rows() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);

    let origin = ServoUrl::parse("https://example.com").unwrap().origin();
    let webview = Some(WebViewId::new(servo_base::id::TEST_PAINTER_ID));
    let browsing_context = Into::<BrowsingContextId>::into(webview.clone().unwrap()).to_string();

    let first = obtain_bottle_map(
        &handle,
        StorageType::Session,
        webview,
        StorageIdentifier::SessionStorage,
        origin.clone(),
    );
    let second = obtain_bottle_map(
        &handle,
        StorageType::Session,
        webview,
        StorageIdentifier::SessionStorage,
        origin.clone(),
    );

    assert_eq!(first.bottle_id, second.bottle_id);

    let registry = open_registry(&tmp_dir);
    let session_shed_count: i64 = registry
        .query_row(
            "SELECT COUNT(*) FROM sheds WHERE storage_type = 'session' AND browsing_context = ?1;",
            [browsing_context],
            |row| row.get(0),
        )
        .unwrap();
    let shelf_count: i64 = registry
        .query_row(
            "SELECT COUNT(*) FROM shelves WHERE origin = ?1;",
            [origin.ascii_serialization().into_owned()],
            |row| row.get(0),
        )
        .unwrap();
    let bucket_count: i64 = registry
        .query_row("SELECT COUNT(*) FROM buckets;", [], |row| row.get(0))
        .unwrap();
    let bottle_count: i64 = registry
        .query_row("SELECT COUNT(*) FROM bottles;", [], |row| row.get(0))
        .unwrap();

    assert_eq!(session_shed_count, 1);
    assert_eq!(shelf_count, 1);
    assert_eq!(bucket_count, 1);
    assert_eq!(bottle_count, 1);
}

#[test]
fn test_local_persistence_and_estimate() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);

    let origin = ServoUrl::parse("https://example.com").unwrap().origin();
    let webview = WebViewId::new(servo_base::id::TEST_PAINTER_ID);
    let storage_proxy_map = obtain_bottle_map(
        &handle,
        StorageType::Local,
        Some(webview),
        StorageIdentifier::IndexedDB,
        origin.clone(),
    );

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persisted(origin.clone(), cb).unwrap();
    assert!(!rx.recv().unwrap().unwrap());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persist(origin.clone(), false, cb).unwrap();
    assert!(!rx.recv().unwrap().unwrap());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persisted(origin.clone(), cb).unwrap();
    assert!(!rx.recv().unwrap().unwrap());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persist(origin.clone(), true, cb).unwrap();
    assert!(rx.recv().unwrap().unwrap());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persisted(origin.clone(), cb).unwrap();
    assert!(rx.recv().unwrap().unwrap());

    let (path, _created) = handle
        .create_database(storage_proxy_map.bottle_id, "estimate".to_string())
        .recv()
        .unwrap()
        .unwrap();
    let payload = vec![0x5a; 8192];
    std::fs::write(path.join("payload.bin"), &payload).unwrap();

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.estimate(origin, cb).unwrap();
    let (usage, quota) = rx.recv().unwrap().unwrap();
    assert!(usage >= payload.len() as u64);
    assert!(quota > usage);
}

#[test]
fn test_storage_manager_operations_fail_for_opaque_origins() {
    install_test_namespace();
    let tmp_dir = tempfile::tempdir().unwrap();
    let handle: ClientStorageThreadHandle =
        ClientStorageThreadFactory::new(Some(tmp_dir.path().to_path_buf()), false);

    let origin = ServoUrl::parse("data:text/plain,hello").unwrap().origin();

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persisted(origin.clone(), cb).unwrap();
    assert!(rx.recv().unwrap().is_err());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.persist(origin.clone(), true, cb).unwrap();
    assert!(rx.recv().unwrap().is_err());

    let (cb, rx) = GenericCallback::new_blocking().unwrap();
    handle.estimate(origin, cb).unwrap();
    assert!(rx.recv().unwrap().is_err());
}
