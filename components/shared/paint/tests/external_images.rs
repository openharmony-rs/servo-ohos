/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use euclid::default::Size2D as UntypedSize2D;
use paint_api::{
    CanvasImageHandler, ExternalImageSource, WebRenderExternalImageApi,
    WebRenderExternalImageIdManager, WebRenderImageHandlerType,
};

/// A backend-supplied handler standing in for a real GPU texture producer.
struct FakeBackendHandler {
    texture_id: u32,
    size: UntypedSize2D<i32>,
    locks: Arc<AtomicUsize>,
    unlocks: Arc<AtomicUsize>,
}

impl WebRenderExternalImageApi for FakeBackendHandler {
    fn lock(&mut self, _id: u64) -> (ExternalImageSource<'_>, UntypedSize2D<i32>) {
        self.locks.fetch_add(1, Ordering::SeqCst);
        (
            ExternalImageSource::NativeTexture(self.texture_id),
            self.size,
        )
    }

    fn unlock(&mut self, _id: u64) {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_canvas2d_external_image_ids_are_unique_and_removable() {
    let mut manager = WebRenderExternalImageIdManager::default();
    let first = manager.next_id(WebRenderImageHandlerType::Canvas2D);
    let second = manager.next_id(WebRenderImageHandlerType::Canvas2D);
    assert!(first != second);
    assert!(matches!(
        manager.get(&first),
        Some(WebRenderImageHandlerType::Canvas2D)
    ));

    manager.remove(&first);
    assert!(manager.get(&first).is_none());
    assert!(matches!(
        manager.get(&second),
        Some(WebRenderImageHandlerType::Canvas2D)
    ));
}

#[test]
fn test_canvas_image_handler_forwards_to_installed_backend() {
    let mut handler = CanvasImageHandler::default();

    // With no backend installed the handler is inert.
    let (source, size) = handler.lock(1);
    assert!(matches!(source, ExternalImageSource::Invalid));
    assert_eq!(size, UntypedSize2D::zero());

    let locks = Arc::new(AtomicUsize::new(0));
    let unlocks = Arc::new(AtomicUsize::new(0));
    handler.install(Some(Box::new(FakeBackendHandler {
        texture_id: 42,
        size: UntypedSize2D::new(3, 5),
        locks: locks.clone(),
        unlocks: unlocks.clone(),
    })));

    let (source, size) = handler.lock(1);
    assert!(matches!(source, ExternalImageSource::NativeTexture(42)));
    assert_eq!(size, UntypedSize2D::new(3, 5));
    handler.unlock(1);
    assert_eq!(locks.load(Ordering::SeqCst), 1);
    assert_eq!(unlocks.load(Ordering::SeqCst), 1);

    // Uninstalling restores the inert behaviour.
    handler.install(None);
    let (source, _) = handler.lock(1);
    assert!(matches!(source, ExternalImageSource::Invalid));
}
