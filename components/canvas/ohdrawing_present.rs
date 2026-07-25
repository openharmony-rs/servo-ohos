/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Zero-copy presentation plumbing for the `ohdrawing` backend: the process-global registry that
//! connects each canvas draw target (producer, canvas paint thread) to the single WebRender
//! external-image handler (consumer, WR renderer thread), plus that handler.
//!
//! The two halves never share OHOS types across the Stage-1 seam — the draw target owns an
//! `OH_NativeImage` producer window (via [`ohos_drawing::Surface::create_on_screen`]) and the
//! handler owns the [`ohos_drawing::NativeImageConsumer`]; they rendezvous only through
//! [`SharedSlot`], keyed by the `ExternalImageId` the canvas layer assigned.

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use euclid::default::Size2D;
use ohos_drawing::{GL_TEXTURE_EXTERNAL_OES, NativeImageConsumer};
use paint_api::{ExternalImageSource, WebRenderExternalImageApi};

/// The buffer queue's depth: up to 3 buffers may sit genuinely queued unconsumed; on a platform
/// that queues one buffer per flush (DAYU200/open-Skia; S1) the next flush would block the canvas
/// thread until the consumer drains one. The S7 spike (DAYU200, drop-mode burst;
/// spikes/native-image-probe/s7-run.log) confirmed `SetDropBufferMode(true)` does NOT lift this
/// producer back-pressure — it only makes `UpdateSurfaceImage` drain to the latest queued buffer.
/// So when the *genuine* queue is full the producer must *skip the flush* — dropping that frame
/// producer-side while staying on the external present (eligible frames are self-contained full
/// repaints, so the next flush carries complete, newest content). It must never block, and never
/// fall back to readback because of queue depth: a readback present stops WebRender from locking
/// the id, `consumed` then freezes, and the fallback would latch permanently.
///
/// "Genuine" is load-bearing. `Surface::flush()` returning Ok does not prove a buffer was enqueued:
/// on the PLR-AL00 (Maleoon 920, DDGR/Vulkan-Skia) only the *first* flush of a fresh pipeline
/// enqueues a buffer; later flushes render in place into that one already-acquired buffer and
/// enqueue nothing (verified with spikes/native-image-probe MODE=interleave: exactly one
/// frame-available callback and one successful `UpdateSurfaceImage` per pipeline lifetime, yet the
/// consumer samples fresh content every frame *while the producer keeps flushing*). Trusting
/// flush-Ok therefore drifts the in-flight estimate up until it hits `MAX_QUEUED`, latching the
/// skip-guard so the producer *stops flushing* and the shared buffer freezes on-screen. In-flight
/// depth is instead derived from [`QueueCounters`], whose `queued` advances only on a real
/// frame-available callback.
pub(crate) const MAX_QUEUED: u64 = 3;

/// Genuine buffer-queue counters for one presentation pipeline, keyed by the pipeline's producer
/// window and shared (behind an `Arc`) by the producer draw target and the consumer handler. They
/// survive canvas recreate: the pipeline (window, `OH_NativeImage`, OES texture) outlives the
/// `ExternalImageId`, and so must the count of buffers in its queue.
///
/// `queued` is advanced ONLY by the `OH_NativeImage` frame-available callback — one real enqueued
/// buffer per callback — never by a flush returning Ok (see [`MAX_QUEUED`]). `consumed` is set by
/// the consumer to the `queued` value it has caught up to (drop-buffer mode drains to the latest
/// buffer, retiring the intermediate ones). `queued - consumed` is the honest in-flight depth.
pub(crate) struct QueueCounters {
    pub queued: AtomicU64,
    pub consumed: AtomicU64,
}

fn pipeline_counters_map() -> &'static Mutex<HashMap<usize, Arc<QueueCounters>>> {
    static COUNTERS: OnceLock<Mutex<HashMap<usize, Arc<QueueCounters>>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The (get-or-create) genuine queue counters for pipeline `window`. Both the producer (to size its
/// in-flight estimate) and the consumer (to publish `queued`/`consumed`) reach the same `Arc`.
pub(crate) fn pipeline_counters(window: usize) -> Arc<QueueCounters> {
    pipeline_counters_map()
        .lock()
        .unwrap()
        .entry(window)
        .or_insert_with(|| {
            Arc::new(QueueCounters {
                queued: AtomicU64::new(0),
                consumed: AtomicU64::new(0),
            })
        })
        .clone()
}

fn remove_pipeline_counters(window: usize) {
    pipeline_counters_map().lock().unwrap().remove(&window);
}

/// The `OH_NativeImage` frame-available callback: one real enqueued buffer. Fires on an internal
/// queue thread, so it only touches an atomic.
///
/// # Safety
/// `ctx` must be the `Arc::as_ptr` of a live [`QueueCounters`] (kept alive by the owning
/// `ConsumerState` for as long as the listener is registered).
unsafe extern "C" fn on_frame_available(ctx: *mut c_void) {
    if ctx.is_null() {
        return;
    }
    // SAFETY: `ctx` points at a `QueueCounters` kept alive by the `ConsumerState` that registered
    // this listener; the listener is unset before that state (and its `Arc`) is dropped.
    let counters = unsafe { &*(ctx as *const QueueCounters) };
    counters.queued.fetch_add(1, Ordering::Release);
}

/// Cross-thread rendezvous between one canvas draw target (producer) and the external-image
/// handler (consumer), keyed by `ExternalImageId`. Both sides hold an `Arc<SharedSlot>`; all fields
/// are atomics, so no OHOS type and no lock crosses the seam. Genuine queue depth lives in the
/// per-pipeline [`QueueCounters`] (keyed by `window`), not here, so it survives canvas recreate.
pub(crate) struct SharedSlot {
    /// Buffer geometry, fixed at registration (a resize re-registers under the same id).
    pub width: i32,
    pub height: i32,
    /// Producer `OHNativeWindow` pointer as `usize`. Published by the consumer once it lazily
    /// creates the `OH_NativeImage`, or pre-published by the producer when it adopts a parked
    /// pipeline (whose native image already exists consumer-side). `0` = not yet available. This is
    /// also the stable identity of the pipeline: consumer state and [`QueueCounters`] are keyed by
    /// it, so a pipeline survives the canvas (and its `ExternalImageId`) being recreated.
    pub window: AtomicUsize,
    /// Set by the producer draw target on drop; tells the consumer to tear its resources down.
    pub dead: AtomicBool,
}

impl SharedSlot {
    fn new(width: i32, height: i32) -> Arc<SharedSlot> {
        Arc::new(SharedSlot {
            width,
            height,
            window: AtomicUsize::new(0),
            dead: AtomicBool::new(false),
        })
    }
}

fn registry() -> &'static Mutex<HashMap<u64, Arc<SharedSlot>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u64, Arc<SharedSlot>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register (or re-register, on resize) the slot for `id` and return the producer's handle to it.
pub(crate) fn register_slot(id: u64, width: i32, height: i32) -> Arc<SharedSlot> {
    let slot = SharedSlot::new(width, height);
    registry().lock().unwrap().insert(id, slot.clone());
    slot
}

/// The consumer's handle to the slot for `id`, if the producer has registered one.
fn lookup_slot(id: u64) -> Option<Arc<SharedSlot>> {
    registry().lock().unwrap().get(&id).cloned()
}

/// Producer windows whose pipeline has been retired (parked-pipeline pool eviction or a canvas
/// dying without parking). The consumer destroys the corresponding native image + OES texture on
/// its next `lock`/`unlock`.
fn retired_windows() -> &'static Mutex<Vec<usize>> {
    static RETIRED: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
    RETIRED.get_or_init(|| Mutex::new(Vec::new()))
}

/// Schedule the consumer-side state of `window`'s pipeline for destruction.
pub(crate) fn retire_window(window: usize) {
    if window != 0 {
        retired_windows().lock().unwrap().push(window);
    }
}

// ---------------------------------------------------------------------------------------------
// Minimal GLES FFI for the consumer's OES external texture (created in WR's GL context).
// ---------------------------------------------------------------------------------------------

type GLenum = u32;
type GLuint = u32;
type GLint = i32;
type GLsizei = i32;

const GL_TEXTURE_MIN_FILTER: GLenum = 0x2801;
const GL_TEXTURE_MAG_FILTER: GLenum = 0x2800;
const GL_TEXTURE_WRAP_S: GLenum = 0x2802;
const GL_TEXTURE_WRAP_T: GLenum = 0x2803;
const GL_LINEAR: GLint = 0x2601;
const GL_CLAMP_TO_EDGE: GLint = 0x812F;

unsafe extern "C" {
    fn glGenTextures(n: GLsizei, textures: *mut GLuint);
    fn glDeleteTextures(n: GLsizei, textures: *const GLuint);
    fn glBindTexture(target: GLenum, texture: GLuint);
    fn glTexParameteri(target: GLenum, pname: GLenum, param: GLint);
}

/// Generate and configure a `GL_TEXTURE_EXTERNAL_OES` texture in the current (WR) GL context.
fn create_oes_texture() -> GLuint {
    // SAFETY: called on the WR renderer thread with WR's GL context current (the `lock()`
    // contract). Standard texture creation + parameter setup for an external-OES sampler target.
    unsafe {
        let mut tex: GLuint = 0;
        glGenTextures(1, &mut tex);
        glBindTexture(GL_TEXTURE_EXTERNAL_OES, tex);
        glTexParameteri(GL_TEXTURE_EXTERNAL_OES, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
        glTexParameteri(GL_TEXTURE_EXTERNAL_OES, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
        glTexParameteri(GL_TEXTURE_EXTERNAL_OES, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE);
        glTexParameteri(GL_TEXTURE_EXTERNAL_OES, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE);
        tex
    }
}

fn delete_oes_texture(tex: GLuint) {
    // SAFETY: WR GL context current; `tex` was created by `create_oes_texture`.
    unsafe { glDeleteTextures(1, &tex) }
}

// ---------------------------------------------------------------------------------------------
// Producer-side EGL: `OH_Drawing_GpuContextCreate` wraps the currently-current EGL context, so the
// canvas paint thread needs one current before it builds its GpuContext for the on-screen surface.
// ---------------------------------------------------------------------------------------------

type EglBool = u32;
type EglInt = i32;
type EglDisplay = *mut core::ffi::c_void;
type EglConfig = *mut core::ffi::c_void;
type EglContext = *mut core::ffi::c_void;
type EglSurface = *mut core::ffi::c_void;

const EGL_OPENGL_ES_API: u32 = 0x30A0;
const EGL_SURFACE_TYPE: EglInt = 0x3033;
const EGL_PBUFFER_BIT: EglInt = 0x0001;
const EGL_RENDERABLE_TYPE: EglInt = 0x3040;
const EGL_OPENGL_ES3_BIT: EglInt = 0x0040;
const EGL_RED_SIZE: EglInt = 0x3024;
const EGL_GREEN_SIZE: EglInt = 0x3023;
const EGL_BLUE_SIZE: EglInt = 0x3022;
const EGL_ALPHA_SIZE: EglInt = 0x3021;
const EGL_NONE: EglInt = 0x3038;
const EGL_WIDTH: EglInt = 0x3057;
const EGL_HEIGHT: EglInt = 0x3056;
const EGL_CONTEXT_CLIENT_VERSION: EglInt = 0x3098;

unsafe extern "C" {
    fn eglGetCurrentContext() -> EglContext;
    fn eglGetDisplay(display_id: *mut core::ffi::c_void) -> EglDisplay;
    fn eglInitialize(dpy: EglDisplay, major: *mut EglInt, minor: *mut EglInt) -> EglBool;
    fn eglBindAPI(api: u32) -> EglBool;
    fn eglChooseConfig(
        dpy: EglDisplay,
        attrib_list: *const EglInt,
        configs: *mut EglConfig,
        config_size: EglInt,
        num_config: *mut EglInt,
    ) -> EglBool;
    fn eglCreateContext(
        dpy: EglDisplay,
        config: EglConfig,
        share_context: EglContext,
        attrib_list: *const EglInt,
    ) -> EglContext;
    fn eglCreatePbufferSurface(
        dpy: EglDisplay,
        config: EglConfig,
        attrib_list: *const EglInt,
    ) -> EglSurface;
    fn eglMakeCurrent(
        dpy: EglDisplay,
        draw: EglSurface,
        read: EglSurface,
        ctx: EglContext,
    ) -> EglBool;
}

/// Ensure the calling (canvas paint) thread has an EGL ES3 context current, creating a small
/// pbuffer context if none is. `OH_Drawing_GpuContextCreate` wraps the current context, so this must
/// run before the thread's GpuContext is built. Best-effort: on any failure it leaves the thread as
/// it was (a device where `GpuContextCreate` provides its own context still works).
pub(crate) fn ensure_producer_egl_context() {
    // SAFETY: standard EGL bring-up on the current thread; all pointers below are stack buffers of
    // the correct size and every result is checked before use.
    unsafe {
        if !eglGetCurrentContext().is_null() {
            return;
        }
        let dpy = eglGetDisplay(core::ptr::null_mut());
        if dpy.is_null() || eglInitialize(dpy, core::ptr::null_mut(), core::ptr::null_mut()) == 0 {
            return;
        }
        if eglBindAPI(EGL_OPENGL_ES_API) == 0 {
            return;
        }
        let config_attrs = [
            EGL_SURFACE_TYPE,
            EGL_PBUFFER_BIT,
            EGL_RENDERABLE_TYPE,
            EGL_OPENGL_ES3_BIT,
            EGL_RED_SIZE,
            8,
            EGL_GREEN_SIZE,
            8,
            EGL_BLUE_SIZE,
            8,
            EGL_ALPHA_SIZE,
            8,
            EGL_NONE,
        ];
        let mut config: EglConfig = core::ptr::null_mut();
        let mut num_config: EglInt = 0;
        if eglChooseConfig(dpy, config_attrs.as_ptr(), &mut config, 1, &mut num_config) == 0 ||
            num_config == 0
        {
            return;
        }
        let pbuffer_attrs = [EGL_WIDTH, 16, EGL_HEIGHT, 16, EGL_NONE];
        let surface = eglCreatePbufferSurface(dpy, config, pbuffer_attrs.as_ptr());
        if surface.is_null() {
            return;
        }
        let context_attrs = [EGL_CONTEXT_CLIENT_VERSION, 3, EGL_NONE];
        let context = eglCreateContext(dpy, config, core::ptr::null_mut(), context_attrs.as_ptr());
        if context.is_null() {
            return;
        }
        eglMakeCurrent(dpy, surface, surface, context);
    }
}

// ---------------------------------------------------------------------------------------------
// Consumer: the single external-image handler installed on the WR renderer thread.
// ---------------------------------------------------------------------------------------------

/// Consumer-side, WR-renderer-thread-only state for one presenting canvas. Owns the OES texture and
/// the `OH_NativeImage`; both are created and destroyed with WR's GL context current.
struct ConsumerState {
    oes_texture: GLuint,
    /// `Some` for this state's whole useful life; only `Drop` takes it, so that the
    /// `OH_NativeImage` is destroyed *before* the OES texture it is bound to (see `Drop`).
    consumer: Option<NativeImageConsumer>,
    /// The pipeline's genuine queue counters. Held here to keep the `Arc` (and thus the atomic the
    /// frame-available listener's `context` points at) alive for the listener's whole lifetime.
    counters: Arc<QueueCounters>,
    /// Buffer geometry this state was built for; a resize re-registers the slot with a new size and
    /// the consumer rebuilds when it no longer matches.
    width: i32,
    height: i32,
    /// Whether at least one buffer has been drained into `oes_texture` (before that, the texture is
    /// undefined and must not be sampled).
    ready: bool,
    /// Number of `lock()` calls served (diagnostics).
    locks: u64,
}

impl Drop for ConsumerState {
    fn drop(&mut self) {
        // Destroy the native image first (releasing the buffer queue and its binding to the OES
        // texture), then the texture. Both on the WR thread with WR's context current (the handler
        // is only touched there).
        //
        // The native image must go first and be dropped *explicitly*: a plain field would be
        // dropped only after this body returns, so `OH_NativeImage_Destroy` would tear down the
        // EGLImage binding against a texture name `delete_oes_texture` had already freed.
        //
        // `counters` outlives the consumer for the same reason it is held here at all: the
        // frame-available listener's `context` points into it, and unsetting the listener is not
        // documented to join a callback already running.
        if let Some(consumer) = self.consumer.take() {
            let _ = consumer.unset_frame_available_listener();
            drop(consumer);
        }
        delete_oes_texture(self.oes_texture);
    }
}

/// The `ohdrawing` external-image handler. One instance per process, installed into the shared
/// `CanvasImageHandler` slot at canvas-paint-thread start-up; every method runs on the WR renderer
/// thread with WR's GL context current.
pub(crate) struct OhDrawingImageHandler {
    /// Consumer state per pipeline, keyed by the pipeline's producer window pointer.
    states: HashMap<usize, ConsumerState>,
    /// How many external images WebRender currently holds locked. Destroying a pipeline's texture
    /// while WebRender still holds *any* lock from this frame would pull a texture out from under a
    /// draw it has already resolved, so teardown only runs at a frame boundary (depth 0).
    locked: usize,
}

// SAFETY: the handler's `!Send` contents (each `ConsumerState`'s `NativeImageConsumer` and GL
// texture) are created, accessed and destroyed exclusively on the WebRender renderer thread inside
// `lock`/`unlock`. The value itself is only ever transferred once — empty (`states` empty) — from
// the canvas paint thread into the shared `CanvasImageHandler` slot at start-up, before any
// consumer state exists. No `!Send` state is ever moved across threads.
unsafe impl Send for OhDrawingImageHandler {}

impl OhDrawingImageHandler {
    pub(crate) fn new() -> Self {
        OhDrawingImageHandler {
            states: HashMap::new(),
            locked: 0,
        }
    }

    /// Destroy consumer state for retired pipelines and drop registry entries of dead ids. Called
    /// at the top of every `lock`/`unlock` so teardown is bounded by ongoing presentation activity.
    /// Note states are keyed by *window* (pipeline identity), not id: a dead id does not destroy
    /// its pipeline, which may be parked for adoption by the canvas's replacement.
    fn reap_dead(&mut self) {
        for window in retired_windows().lock().unwrap().drain(..) {
            // Drop the consumer state first (its `Drop` unsets the frame-available listener, so no
            // callback can fire after this), then release the pipeline's queue counters.
            self.states.remove(&window);
            remove_pipeline_counters(window);
        }
        registry()
            .lock()
            .unwrap()
            .retain(|_, slot| !slot.dead.load(Ordering::Acquire));
    }

    /// Ensure a `ConsumerState` exists for `id`, creating the OES texture + `OH_NativeImage` in
    /// WR's context on first use and publishing the producer window into the slot.
    /// The consumer state for `slot`'s pipeline, keyed by producer window. When the slot has no
    /// window yet (fresh pipeline), create the OES texture + `OH_NativeImage` in WR's context and
    /// publish the window; when the producer pre-published an adopted pipeline's window, the
    /// existing state is reused as-is (surviving the canvas recreate).
    fn ensure_state(&mut self, id: u64, slot: &SharedSlot) -> Option<&mut ConsumerState> {
        let mut window = slot.window.load(Ordering::Acquire);
        if window == 0 {
            let oes_texture = create_oes_texture();
            // SAFETY: `oes_texture` is a live external-OES texture in the current (WR) GL context,
            // which stays current for this handler's `update_surface_image` calls.
            let consumer = match unsafe { NativeImageConsumer::new(oes_texture) } {
                Ok(consumer) => consumer,
                Err(_) => {
                    delete_oes_texture(oes_texture);
                    return None;
                },
            };
            window = match consumer.acquire_window(slot.width, slot.height) {
                Ok(window) => window,
                Err(_) => {
                    delete_oes_texture(oes_texture);
                    return None;
                },
            };
            let _ = consumer.set_drop_buffer_mode(true);
            // Honest queue accounting: count real enqueues via the frame-available callback, not
            // flushes returning Ok. The `context` points at the pipeline's `QueueCounters`, kept
            // alive by this `ConsumerState` for as long as the listener is registered.
            let counters = pipeline_counters(window);
            // SAFETY: `on_frame_available` only touches the referenced `QueueCounters` atomically;
            // the pointer stays valid because `counters` is stored in the `ConsumerState` below and
            // the listener is unset in that state's `Drop` before the `Arc` is released.
            let _ = unsafe {
                consumer.set_frame_available_listener(
                    Arc::as_ptr(&counters) as *mut c_void,
                    on_frame_available,
                )
            };
            slot.window.store(window, Ordering::Release);
            log::info!(
                "ohdrawing: consumer created OH_NativeImage for external image id={id} ({}x{}); zero-copy present active",
                slot.width,
                slot.height
            );
            self.states.insert(
                window,
                ConsumerState {
                    oes_texture,
                    consumer: Some(consumer),
                    counters,
                    width: slot.width,
                    height: slot.height,
                    ready: false,
                    locks: 0,
                },
            );
        }
        let state = self.states.get_mut(&window)?;
        if state.width != slot.width || state.height != slot.height {
            log::warn!(
                "ohdrawing: pipeline size mismatch for id={id} ({}x{} vs slot {}x{})",
                state.width,
                state.height,
                slot.width,
                slot.height
            );
            return None;
        }
        Some(state)
    }
}

impl WebRenderExternalImageApi for OhDrawingImageHandler {
    fn lock(&mut self, id: u64) -> (ExternalImageSource<'_>, Size2D<i32>) {
        // Only safe to tear pipelines down when nothing is locked: WebRender resolves every
        // external image for a frame up front, so a retired pipeline's texture may already have
        // been handed out by an earlier `lock` in this same frame.
        if self.locked == 0 {
            self.reap_dead();
        }
        self.locked += 1;
        let Some(slot) = lookup_slot(id) else {
            return (ExternalImageSource::Invalid, Size2D::zero());
        };
        if slot.dead.load(Ordering::Acquire) {
            return (ExternalImageSource::Invalid, Size2D::zero());
        }
        let size = Size2D::new(slot.width, slot.height);
        let Some(state) = self.ensure_state(id, &slot) else {
            return (ExternalImageSource::Invalid, Size2D::zero());
        };

        // Drain to the latest genuinely-queued buffer (drop-buffer mode keeps only the newest).
        // On a platform that enqueues one buffer per flush this drains the whole queue; on the PLR,
        // where only the first flush enqueues and later flushes render in place into the same
        // acquired buffer, it succeeds once at bootstrap and then returns non-zero — but the OES
        // texture still tracks the shared buffer's live content, so WebRender keeps compositing the
        // producer's newest render as long as the producer keeps flushing.
        let mut drained = 0u64;
        for _ in 0..8 {
            let Some(consumer) = state.consumer.as_ref() else {
                break;
            };
            // SAFETY: WR's GL context (which owns `oes_texture`) is current on this thread.
            let rc = unsafe { consumer.update_surface_image() };
            if rc != 0 {
                break;
            }
            drained += 1;
        }
        let queued = state.counters.queued.load(Ordering::Acquire);
        if drained > 0 {
            state.ready = true;
            // Caught up to the latest enqueued buffer; older dropped frames are moot under
            // drop-buffer mode. Publishing the genuine `queued` value (not a flush count) keeps the
            // producer's in-flight estimate honest and stops the skip-guard from latching.
            state.counters.consumed.store(queued, Ordering::Release);
        }
        state.locks += 1;
        if state.locks <= 6 {
            let consumed = state.counters.consumed.load(Ordering::Acquire);
            log::info!(
                "ohdrawing: lock #{} id={id} drained={drained} queued={queued} consumed={consumed} ready={}",
                state.locks,
                state.ready
            );
        }

        if state.ready {
            (ExternalImageSource::NativeTexture(state.oes_texture), size)
        } else {
            (ExternalImageSource::Invalid, Size2D::zero())
        }
    }

    fn unlock(&mut self, _id: u64) {
        self.locked = self.locked.saturating_sub(1);
        if self.locked == 0 {
            self.reap_dead();
        }
    }
}
