/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! A canvas-2D [`GenericDrawTarget`] backed by OpenHarmony's ArkGraphics 2D (system Skia),
//! through the safe [`ohos_drawing`] wrapper. It renders into a GPU surface when a GPU context is
//! available on the canvas thread, and transparently falls back to a CPU bitmap otherwise. The
//! presentation contract (premultiplied RGBA8, `ImageDescriptorFlags::empty()`) matches the
//! `vello_cpu` backend exactly, so the two are pixel-swappable via the `dom_canvas_backend` pref.

use std::cell::{Cell, OnceCell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::ffi::c_void;
use std::hash::{BuildHasher, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use euclid::default::{Point2D, Rect, Size2D, Transform2D};
use fonts::FontIdentifier;
use kurbo::PathEl;
use ohos_drawing::{
    AlphaFormat, Bitmap, BlendMode, Brush, Canvas, ClipOp, Color, ColorFormat, FillType,
    FilterMode, Font, GpuContext, Image, ImageInfo, LineCap, LineJoin, Matrix, MemoryStream,
    MipmapMode, Path as OhPath, PathEffect, Pen, Point, Rect as OhRect, SamplingOptions,
    ShaderEffect, ShadowLayer, SrcRectConstraint, Surface, TextBlobBuilder, TileMode, Typeface,
};
use paint_api::{SerializableImageData, WebRenderExternalImageApi};
use pixels::{Snapshot, SnapshotAlphaMode, SnapshotPixelFormat};
use profile_traits::mem::ReportKind;
use servo_base::generic_channel::GenericSharedMemory;
use servo_canvas_traits::canvas::{
    CanvasGradientStop, CompositionOptions, CompositionOrBlending, CompositionStyle,
    FillOrStrokeStyle, FillRule, LineCapStyle, LineJoinStyle, LineOptions, Path, ShadowOptions,
    TextRun,
};
use style::color::AbsoluteColor;
use webrender_api::{ExternalImageId, ImageBufferKind, ImageDescriptor, ImageDescriptorFlags};

use crate::backend::{CanvasStoreSizesPerType, GenericDrawTarget, PresentationData};
use crate::canvas_data::Filter;
use crate::ohdrawing_present::{self, OhDrawingImageHandler, QueueCounters, SharedSlot};

thread_local! {
    /// The GPU context shared by every canvas rendering on this thread. Created lazily on first use
    /// and leaked so it lives for the whole thread; `None` once creation has failed (CPU fallback).
    static GPU_CONTEXT: OnceCell<Option<&'static GpuContext>> = const { OnceCell::new() };

    /// The typeface cache shared by all canvases rendering on this thread, mirroring the
    /// `vello_cpu` `SHARED_FONT_CACHE` shape (keyed by [`FontIdentifier`]).
    static SHARED_FONT_CACHE: RefCell<HashMap<FontIdentifier, Rc<Typeface>>> = RefCell::default();

    /// Randomly keyed hasher state for [`content_key`]. Built once per thread from the process's
    /// random source, so the key is not predictable by page content (see [`content_key`]).
    static CONTENT_HASH_STATE: RandomState = RandomState::new();

    /// Whether the chosen render mode has already been logged once for this thread.
    static MODE_LOGGED: RefCell<bool> = const { RefCell::new(false) };

    /// Whether the first zero-copy external present has been logged once for this thread.
    static EXTERNAL_LOGGED: Cell<bool> = const { Cell::new(false) };

    /// Content-addressed cache of source images uploaded for `draw_surface`/`draw_image`. Keyed by a
    /// hash of the (premultiplied RGBA) source pixels so that redrawing the same image reuses the
    /// already-built [`Image`] — and hence the GPU texture the system Skia caches by that image's
    /// identity — instead of re-uploading a fresh texture on every `drawImage`. See
    /// [`source_image_cached`].
    static SOURCE_IMAGE_CACHE: RefCell<Vec<SourceImageCacheEntry>> = const { RefCell::new(Vec::new()) };
}

/// Upper bound on the number of distinct source images kept resident on this thread.
const SOURCE_CACHE_MAX_ENTRIES: usize = 32;
/// Upper bound on the total source-pixel bytes kept resident (evicts LRU beyond this).
const SOURCE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

struct SourceImageCacheEntry {
    key: u64,
    size: Size2D<i32>,
    bytes: usize,
    image: Rc<Image>,
}

/// A source image handed to the draw-surface family: a shared, already-uploaded [`Image`] plus its
/// pixel size (kept alongside because [`Image`] does not expose its dimensions).
#[derive(Clone)]
pub(crate) struct SourceImage {
    image: Rc<Image>,
    size: Size2D<i32>,
}

/// Hash the premultiplied-RGBA `bytes` of a source image together with its `size` into a stable
/// content key.
///
/// The hasher is **randomly keyed per process**, which is a security requirement rather than a
/// preference. This cache lives on the single canvas paint thread shared by every document from
/// every origin, and a key collision draws one origin's image in place of another's — which a page
/// can then read back with `getImageData`. With a fixed-seed, non-cryptographic hash (this used
/// `FxHasher`, an invertible per-word multiply chain) an attacker can *construct* a colliding image
/// offline, so "astronomically unlikely" held only for accidental collisions, not adversarial ones.
/// A random key removes the offline attack; `size` is still matched exactly on lookup.
fn content_key(bytes: &[u8], size: Size2D<i32>) -> u64 {
    CONTENT_HASH_STATE.with(|state| {
        let mut hasher = state.build_hasher();
        hasher.write_i32(size.width);
        hasher.write_i32(size.height);
        hasher.write_usize(bytes.len());
        hasher.write(bytes);
        hasher.finish()
    })
}

/// Return the cached [`Image`] for `key`/`size`, or build it from `bytes` (uploading once) and
/// insert it. The cache is a small move-to-front LRU bounded by both entry count and total bytes.
fn source_image_cached(key: u64, size: Size2D<i32>, bytes: &[u8]) -> Option<Rc<Image>> {
    SOURCE_IMAGE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(index) = cache
            .iter()
            .position(|entry| entry.key == key && entry.size == size)
        {
            let entry = cache.remove(index);
            let image = entry.image.clone();
            cache.insert(0, entry);
            return Some(image);
        }

        let bitmap = Bitmap::from_pixels(
            ImageInfo::rgba8888_premul(size.width, size.height),
            bytes,
            (size.width * 4) as u32,
        )
        .ok()?;
        let image = Rc::new(Image::from_bitmap(&bitmap).ok()?);
        cache.insert(
            0,
            SourceImageCacheEntry {
                key,
                size,
                bytes: bytes.len(),
                image: image.clone(),
            },
        );

        let mut total: usize = cache.iter().map(|entry| entry.bytes).sum();
        while cache.len() > 1 &&
            (cache.len() > SOURCE_CACHE_MAX_ENTRIES || total > SOURCE_CACHE_MAX_BYTES)
        {
            if let Some(evicted) = cache.pop() {
                total -= evicted.bytes;
            }
        }
        Some(image)
    })
}

/// Return this thread's GPU context, creating (and leaking) it on first call. `None` when no GPU
/// context is available, in which case draw targets fall back to a CPU bitmap.
fn gpu_context() -> Option<&'static GpuContext> {
    GPU_CONTEXT.with(|cell| {
        *cell.get_or_init(|| {
            // `GpuContextCreate` wraps the currently-current EGL context; make sure this thread has
            // one so the same GpuContext can drive both the offscreen and the on-screen surface.
            ohdrawing_present::ensure_producer_egl_context();
            match GpuContext::new() {
                Ok(context) => {
                    let context: &'static GpuContext = Box::leak(Box::new(context));
                    Some(context)
                },
                Err(_) => None,
            }
        })
    })
}

fn log_external_present_once() {
    EXTERNAL_LOGGED.with(|logged| {
        if !logged.get() {
            log::info!(
                "ohdrawing canvas backend: presenting frames zero-copy as WebRender external images (readback skipped)"
            );
            logged.set(true);
        }
    });
}

fn log_mode_once(gpu: bool) {
    MODE_LOGGED.with(|logged| {
        let mut logged = logged.borrow_mut();
        if !*logged {
            if gpu {
                log::info!("ohdrawing canvas backend: using GPU surface");
            } else {
                log::info!("ohdrawing canvas backend: using CPU bitmap (no GPU context)");
            }
            *logged = true;
        }
    });
}

/// The pixel target a canvas draws into: a GPU surface or a CPU bitmap.
enum Backing {
    /// A GPU surface (borrowing the leaked thread-local context, hence `'static`).
    Gpu(Surface<'static>),
    /// A CPU bitmap, wrapped so a canvas can be bound to it through a shared reference.
    Cpu(RefCell<Bitmap>),
}

/// A clip pushed onto the target, stored in its own user-space transform so it can be re-applied on
/// every draw (Skia models clips as save/restore state; the trait models them as a push/pop stack).
struct Clip {
    path: OhPath,
    transform: Transform2D<f64>,
}

/// Per-canvas zero-copy presentation state on the producer (canvas paint thread) side. Created
/// either by adopting a parked same-size pipeline (canvas-recreate fast path) or on the first
/// external present once the canvas layer assigns an `ExternalImageId`.
struct ZeroCopy {
    /// The cross-thread rendezvous with the consumer handler, present once an id is assigned.
    slot: RefCell<Option<Arc<SharedSlot>>>,
    /// The on-screen surface bound to the pipeline's producer window. Frames eligible for
    /// zero-copy present are teed here.
    onscreen: RefCell<Option<Surface<'static>>>,
    /// The pipeline's producer window (its stable identity), once known — immediately for an
    /// adopted pipeline, or cached from the slot after the consumer publishes it.
    window: Cell<usize>,
    /// The pipeline's genuine queue counters, cached once the window is known. Keyed by `window`,
    /// they persist across canvas recreate/adoption, so an adopted pipeline resumes with its real
    /// in-flight depth (no seeding needed).
    counters: RefCell<Option<Arc<QueueCounters>>>,
}

impl ZeroCopy {
    /// The pipeline's window, resolving (and caching) from the slot when the consumer created it.
    fn resolve_window(&self) -> usize {
        let mut window = self.window.get();
        if window == 0 {
            if let Some(slot) = self.slot.borrow().as_ref() {
                window = slot.window.load(Ordering::Acquire);
            }
            if window != 0 {
                self.window.set(window);
            }
        }
        window
    }

    /// The pipeline's genuine queue counters, once its window is known (cached on first resolve).
    fn counters(&self) -> Option<Arc<QueueCounters>> {
        if self.counters.borrow().is_none() {
            let window = self.resolve_window();
            if window != 0 {
                *self.counters.borrow_mut() = Some(ohdrawing_present::pipeline_counters(window));
            }
        }
        self.counters.borrow().clone()
    }

    /// Producer-side honest estimate of buffers genuinely in the queue: `queued` (advanced only by
    /// real frame-available callbacks) minus `consumed`. Zero until the pipeline's window exists.
    fn in_flight(&self) -> u64 {
        match self.counters() {
            Some(counters) => counters
                .queued
                .load(Ordering::Acquire)
                .saturating_sub(counters.consumed.load(Ordering::Acquire)),
            None => 0,
        }
    }
}

/// A same-size presentation pipeline parked when its canvas died, awaiting adoption by a
/// replacement canvas. Keeping the pipeline alive across ECharts-style dispose/recreate cycles
/// avoids re-paying the (observed multi-second, size-dependent) first-flush cost of a fresh
/// buffer queue, and skips the external-present bootstrap entirely.
struct ParkedPipeline {
    width: i32,
    height: i32,
    onscreen: Surface<'static>,
    window: usize,
}

thread_local! {
    /// Parked pipelines available for adoption, oldest first (see [`ParkedPipeline`]).
    static PIPELINE_POOL: RefCell<Vec<ParkedPipeline>> = const { RefCell::new(Vec::new()) };
}

/// Parked pipelines kept per thread; evicting one retires its consumer-side state.
const PIPELINE_POOL_MAX: usize = 2;

/// A successful on-screen flush at or above this duration marks its buffer size as
/// stall-prone: fresh pipelines of that size are never engaged again on this thread (the
/// canvas presents via the recycled readback path instead). Observed on DAYU200: the first
/// flush of a fresh ~720x1018-class queue reliably blocks ~10 s inside the platform (empty
/// queue, unaffected by SET_TIMEOUT/SET_SWAP_INTERVAL), while 512x512 and 2048x2048 never do.
const FLUSH_BLACKLIST_MS: u128 = 1000;

thread_local! {
    /// Buffer sizes whose fresh-pipeline flush stalled (see [`FLUSH_BLACKLIST_MS`]).
    static BLACKLISTED_SIZES: RefCell<Vec<(i32, i32)>> = const { RefCell::new(Vec::new()) };
}

fn size_blacklisted(size: Size2D<i32>) -> bool {
    BLACKLISTED_SIZES.with(|sizes| sizes.borrow().contains(&(size.width, size.height)))
}

fn blacklist_size(size: Size2D<i32>) {
    BLACKLISTED_SIZES.with(|sizes| {
        let mut sizes = sizes.borrow_mut();
        if !sizes.contains(&(size.width, size.height)) {
            sizes.push((size.width, size.height));
        }
    });
}

/// A 1x1 fully transparent source image, used when a snapshot cannot be built. Drawing it is a
/// no-op, which keeps a failure local to one draw instead of killing the canvas paint thread.
fn blank_source_image() -> SourceImage {
    let size = Size2D::new(1, 1);
    let image = Bitmap::from_pixels(ImageInfo::rgba8888_premul(1, 1), &[0u8; 4], 4)
        .and_then(|bitmap| Image::from_bitmap(&bitmap))
        .expect("1x1 transparent source image");
    SourceImage {
        image: Rc::new(image),
        size,
    }
}

fn pool_take(size: Size2D<i32>) -> Option<ParkedPipeline> {
    PIPELINE_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        let index = pool
            .iter()
            .position(|parked| parked.width == size.width && parked.height == size.height)?;
        Some(pool.remove(index))
    })
}

fn pool_park(parked: ParkedPipeline) {
    PIPELINE_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() == PIPELINE_POOL_MAX {
            let evicted = pool.remove(0);
            retire_pipeline(evicted.onscreen, evicted.window);
        }
        pool.push(parked);
    });
}

/// Retire a pipeline, destroying the producer surface *before* publishing the window for consumer
/// teardown.
///
/// `retire_window` only queues the window; the WebRender thread drains that queue and destroys the
/// `OH_NativeImage`, which owns the `OHNativeWindow` this surface renders into. Dropping the
/// surface afterwards would let the consumer free the window while the producer surface still
/// references it — a use-after-free in the driver. Ordering the drop first is what makes
/// `Surface::create_on_screen`'s "window outlives the surface" precondition actually hold.
fn retire_pipeline(onscreen: Surface, window: usize) {
    drop(onscreen);
    ohdrawing_present::retire_window(window);
}

pub(crate) struct OhDrawingDrawTarget {
    backing: Backing,
    size: Size2D<i32>,
    clips: Vec<Clip>,
    /// Zero-copy presentation state (GPU backing only).
    present: RefCell<Option<ZeroCopy>>,
    /// Whether the first draw op of the current frame has been seen yet (reset at each present).
    frame_started: Cell<bool>,
    /// Whether the current frame began with a full-viewport clear/opaque fill — the gate for
    /// presenting it zero-copy (S6: incremental frames must go through readback).
    frame_eligible: Cell<bool>,
    /// Whether the current frame's ops are being teed to the on-screen surface for zero-copy.
    tee_active: Cell<bool>,
    /// Count of full-viewport-clear frames seen. Zero-copy is only bootstrapped once a canvas is
    /// established as *animating* (>= [`ELIGIBLE_FRAMES_BEFORE_EXTERNAL`] such frames), so a static
    /// page — or one whose only full-clear frame is its last (e.g. a final verdict repaint) — never
    /// takes the one-frame-content-less external bootstrap and always presents correctly via
    /// readback.
    eligible_frames: Cell<u64>,
    /// External presents made while waiting for the consumer to attach (publish the producer
    /// window). Beyond [`MAX_PENDING_ATTACH_PRESENTS`] the consumer is assumed broken and zero-copy
    /// is abandoned for this draw target.
    pending_attach_presents: Cell<u32>,
    /// Flushes skipped because the buffer queue was full (diagnostics; see `should_skip_flush`).
    skipped_flushes: Cell<u64>,
    /// Retired readback presentation buffers kept for allocation reuse (same recycling shape as the
    /// vello_cpu present path): WebRender may still hold a just-presented buffer, so a small ring
    /// lets its pipeline drain before an allocation is reclaimed.
    present_buffers: RefCell<Vec<Arc<Vec<u8>>>>,
}

/// Number of full-viewport-clear frames a canvas must produce before the backend starts presenting
/// it zero-copy. Gates out static/single-frame canvases (for which readback is already correct and
/// cheap) and confines the unavoidable one-frame content-less external bootstrap to a genuine
/// animation, where it is imperceptible.
const ELIGIBLE_FRAMES_BEFORE_EXTERNAL: u64 = 2;

/// External presents tolerated while the consumer has not yet attached before abandoning zero-copy
/// for the draw target (the canvas shows nothing during these frames; in the working case the
/// consumer attaches on WebRender's first lock, i.e. after one present).
const MAX_PENDING_ATTACH_PRESENTS: u32 = 60;

/// An on-screen surface flush at or above this duration indicates producer back-pressure the
/// skip-flush guard should have prevented; it is logged as a watchdog warning.
const FLUSH_WATCHDOG_MS: u128 = 100;

/// Retired readback presentation buffers kept per draw target.
const PRESENT_BUFFER_RING: usize = 3;

impl OhDrawingDrawTarget {
    /// Run `f` with a canvas bound to the canonical (offscreen) backing pixels. For the GPU backing
    /// this is the surface's own persistent canvas; for the CPU backing a fresh canvas is bound to
    /// the bitmap. Callers must leave the canvas save-stack balanced, as the GPU canvas persists
    /// between calls.
    fn with_canvas<R>(&self, f: impl FnOnce(&Canvas) -> R) -> R {
        match &self.backing {
            Backing::Gpu(surface) => {
                let canvas = surface.canvas().expect("surface canvas");
                f(&canvas)
            },
            Backing::Cpu(cell) => {
                let mut bitmap = cell.borrow_mut();
                let canvas = Canvas::for_bitmap(&mut bitmap).expect("bitmap canvas");
                f(&canvas)
            },
        }
    }

    /// Record the first draw op of a frame and decide whether the frame is eligible for zero-copy
    /// present (its first op is a full-viewport clear or opaque fill). On an eligible GPU frame with
    /// an assigned id and a live consumer window, lazily create the on-screen surface and arm
    /// teeing so this frame's ops are mirrored there. Queue depth is deliberately NOT consulted
    /// here: a full queue is handled at present time by skipping the flush (still external), never
    /// by falling back to readback, which would stop WebRender from locking the id and latch.
    fn begin_frame_op(&self, full_clear: bool) {
        if self.frame_started.get() {
            return;
        }
        self.frame_started.set(true);
        self.frame_eligible.set(full_clear);
        self.tee_active.set(false);
        if !full_clear || !matches!(self.backing, Backing::Gpu(_)) {
            return;
        }
        self.eligible_frames.set(self.eligible_frames.get() + 1);
        // Only animate-established canvases go zero-copy; a static or terminal-clear frame stays on
        // readback so it never shows the one-frame content-less external bootstrap.
        if self.eligible_frames.get() < ELIGIBLE_FRAMES_BEFORE_EXTERNAL {
            return;
        }
        let mut present = self.present.borrow_mut();
        if present.is_none() && size_blacklisted(self.size) {
            // This size's fresh-pipeline flush stalls on this device; stay on readback.
            return;
        }
        if present.is_none() {
            // A same-size pipeline parked by a predecessor canvas skips both the fresh-queue
            // first-flush cost and the external-present bootstrap.
            if let Some(parked) = pool_take(self.size) {
                *present = Some(ZeroCopy {
                    slot: RefCell::new(None),
                    onscreen: RefCell::new(Some(parked.onscreen)),
                    window: Cell::new(parked.window),
                    counters: RefCell::new(None),
                });
            }
        }
        let Some(zc) = present.as_ref() else {
            return;
        };
        let window = zc.resolve_window();
        if window == 0 {
            return;
        }
        {
            let mut onscreen = zc.onscreen.borrow_mut();
            if onscreen.is_none() {
                let Some(context) = gpu_context() else {
                    return;
                };
                let info = ImageInfo::rgba8888_premul(self.size.width, self.size.height);
                // SAFETY: `window` is the live producer window of this pipeline's `OH_NativeImage`;
                // it outlives this surface (the consumer destroys the native image only after this
                // draw target parks or retires the pipeline on drop).
                match unsafe { Surface::create_on_screen(context, info, window as *mut c_void) } {
                    Ok(surface) => {
                        // Surface creation can reset the window's buffer-request configuration;
                        // re-apply the non-blocking settings so an abnormal dequeue wait fails
                        // fast into the readback fallback instead of stalling the canvas thread.
                        // SAFETY: `window` is this pipeline's live producer window.
                        let _ = unsafe { ohos_drawing::configure_window_nonblocking(window, 100) };
                        *onscreen = Some(surface)
                    },
                    Err(_) => return,
                }
            }
        }
        self.tee_active.set(true);
    }

    /// Run `draw` inside a fresh save frame that re-establishes every clip and then sets `transform`
    /// as the active user-space transform, on the canonical surface — and, when teeing an eligible
    /// zero-copy frame, on the on-screen surface as well (double GPU raster).
    fn with_state(&self, transform: Transform2D<f64>, draw: impl Fn(&Canvas)) {
        self.with_canvas(|canvas| apply_state(canvas, &self.clips, transform, &draw));
        if self.tee_active.get() {
            if let Some(zc) = self.present.borrow().as_ref() {
                if let Some(surface) = zc.onscreen.borrow().as_ref() {
                    if let Ok(canvas) = surface.canvas() {
                        apply_state(&canvas, &self.clips, transform, &draw);
                    }
                }
            }
        }
    }

    /// Read the whole target back as premultiplied RGBA8 into `buf` (sized width*height*4) — the
    /// byte layout both `vello` backends and this one present to WebRender.
    fn read_premul_rgba_into(&self, buf: &mut [u8]) {
        let (width, height) = (self.size.width, self.size.height);
        let info = ImageInfo::rgba8888_premul(width, height);
        let stride = (width * 4) as u32;
        match &self.backing {
            Backing::Gpu(surface) => {
                let _ = surface.flush();
                let canvas = surface.canvas().expect("surface canvas");
                let _ = canvas.read_pixels(info, buf, stride, 0, 0);
            },
            Backing::Cpu(cell) => {
                let mut bitmap = cell.borrow_mut();
                let canvas = Canvas::for_bitmap(&mut bitmap).expect("bitmap canvas");
                let _ = canvas.read_pixels(info, buf, stride, 0, 0);
            },
        }
    }

    fn read_premul_rgba(&self) -> Vec<u8> {
        let mut buf = vec![0u8; (self.size.width * self.size.height * 4) as usize];
        self.read_premul_rgba_into(&mut buf);
        buf
    }

    /// Read the target back into an `Arc` buffer handed to WebRender without a second copy,
    /// reclaiming a retired same-size buffer's allocation when WebRender has released it (sole
    /// ownership). A full-clear-per-frame canvas on the readback path reaches an allocation-free,
    /// single-copy (GPU readback only) steady state. Reclaimed buffers are not zeroed: the
    /// readback overwrites every byte.
    fn read_premul_rgba_arc(&self) -> Arc<Vec<u8>> {
        let expected = (self.size.width * self.size.height * 4) as usize;
        let mut ring = self.present_buffers.borrow_mut();
        let mut buf = match ring
            .iter()
            .position(|buffer| Arc::strong_count(buffer) == 1 && buffer.len() == expected)
        {
            Some(index) => {
                Arc::try_unwrap(ring.swap_remove(index)).expect("sole owner checked above")
            },
            None => vec![0u8; expected],
        };
        self.read_premul_rgba_into(&mut buf);
        let buffer = Arc::new(buf);
        if ring.len() == PRESENT_BUFFER_RING {
            ring.remove(0);
        }
        ring.push(buffer.clone());
        buffer
    }

    /// Decide the present mode for the frame just drawn and, when zero-copy, flush the teed
    /// on-screen surface. Returns `true` if the frame is presented as a WebRender external image.
    ///
    /// Invariant (D4 fix): once a canvas is presenting external, an eligible frame must never fall
    /// back to readback because of queue depth — WebRender only locks (and thus drains the queue)
    /// while it is sampling the external image, so a depth-triggered readback would freeze
    /// `consumed` and latch the fallback permanently. A full queue instead skips the flush
    /// (dropping that frame producer-side; the next flush carries a complete newer frame).
    fn present_external(&mut self) -> bool {
        if self.tee_active.get() {
            let present = self.present.borrow();
            let Some(zc) = present.as_ref() else {
                return false;
            };
            let onscreen = zc.onscreen.borrow();
            let Some(surface) = onscreen.as_ref() else {
                return false;
            };
            if zc.in_flight() >= ohdrawing_present::MAX_QUEUED {
                let skipped = self.skipped_flushes.get() + 1;
                self.skipped_flushes.set(skipped);
                if skipped == 1 || skipped % 300 == 0 {
                    log::info!(
                        "ohdrawing: queue full; skipped on-screen flush (still external; total skips {skipped})"
                    );
                }
                return true;
            }
            let start = std::time::Instant::now();
            if surface.flush().is_ok() {
                let elapsed_ms = start.elapsed().as_millis();
                if elapsed_ms >= FLUSH_WATCHDOG_MS {
                    log::warn!(
                        "ohdrawing: on-screen flush took {elapsed_ms}ms (in_flight was {})",
                        zc.in_flight(),
                    );
                }
                if elapsed_ms >= FLUSH_BLACKLIST_MS {
                    log::warn!(
                        "ohdrawing: flush stall at {}x{}; future canvases of this size will present via readback",
                        self.size.width,
                        self.size.height
                    );
                    blacklist_size(self.size);
                }
                // The queue's `queued` count is advanced by the consumer's frame-available callback
                // when a buffer is *genuinely* enqueued, never here — a flush returning Ok does not
                // prove an enqueue (see `MAX_QUEUED`). Keeping the producer flushing every eligible
                // frame is what makes the shared-buffer platforms (PLR) animate.
                log_external_present_once();
                return true;
            }
            log::warn!("ohdrawing: on-screen surface flush failed; presenting frame via readback");
            return false;
        }
        if !self.frame_eligible.get() ||
            self.eligible_frames.get() < ELIGIBLE_FRAMES_BEFORE_EXTERNAL ||
            !matches!(self.backing, Backing::Gpu(_))
        {
            // Incremental frame (S6: readback by design) or animation not yet established.
            return false;
        }
        match self.present.borrow().as_ref() {
            // Bootstrap: no id yet. Return External once (with no queued content) so the canvas
            // layer allocates the id and the handler creates the native image on its next lock.
            None => !size_blacklisted(self.size),
            Some(zc) => {
                if zc.resolve_window() == 0 {
                    // Id assigned but the consumer has not attached yet. Keep presenting External so
                    // WebRender keeps locking the id (its lock is what performs the attach); a
                    // readback here would stop the locks and latch. Give up loudly if the consumer
                    // never comes up.
                    let pending = self.pending_attach_presents.get() + 1;
                    self.pending_attach_presents.set(pending);
                    if pending > MAX_PENDING_ATTACH_PRESENTS {
                        if pending == MAX_PENDING_ATTACH_PRESENTS + 1 {
                            log::warn!(
                                "ohdrawing: consumer never attached after {MAX_PENDING_ATTACH_PRESENTS} presents; abandoning zero-copy for this canvas"
                            );
                        }
                        return false;
                    }
                    true
                } else {
                    // Window exists but this frame was not teed (e.g. on-screen surface creation
                    // failed and will be retried next frame).
                    log::warn!(
                        "ohdrawing: eligible frame not teed despite live consumer; presenting via readback"
                    );
                    false
                }
            },
        }
    }

    /// Whether `rect` under `transform` covers the whole draw target (an axis-aligned transform
    /// whose image of `rect` contains `[0,0]..[size]`). Used to detect a frame-opening full clear.
    fn covers_viewport(&self, rect: &Rect<f32>, transform: Transform2D<f64>) -> bool {
        // An active clip makes "covers the viewport" a lie: the draw is applied through the clip
        // (see `apply_state`), so it touches only the clipped region. Treating it as a full repaint
        // would arm the tee and hand WebRender a *fresh* on-screen buffer whose unclipped area was
        // never written, while the canonical offscreen surface still holds the previous content --
        // so the displayed pixels and `getImageData` would disagree.
        if !self.clips.is_empty() {
            return false;
        }
        // Only axis-aligned (no rotation/shear) transforms are treated as viewport-covering.
        if transform.m12.abs() > 1e-4 || transform.m21.abs() > 1e-4 {
            return false;
        }
        let rect = rect.to_f64();
        let corners = [
            transform.transform_point(rect.min()),
            transform.transform_point(rect.max()),
        ];
        let min_x = corners[0].x.min(corners[1].x);
        let min_y = corners[0].y.min(corners[1].y);
        let max_x = corners[0].x.max(corners[1].x);
        let max_y = corners[0].y.max(corners[1].y);
        min_x <= 0.5 &&
            min_y <= 0.5 &&
            max_x >= self.size.width as f64 - 0.5 &&
            max_y >= self.size.height as f64 - 0.5
    }

    /// Whether `rect` is a full-viewport opaque solid fill with source-over compositing — the fill
    /// equivalent of a frame-opening clear (an ECharts-class canvas that repaints its background
    /// with a `fillRect` instead of `clearRect`).
    fn is_opaque_full_fill(
        &self,
        rect: &Rect<f32>,
        style: &FillOrStrokeStyle,
        composition_options: &CompositionOptions,
        transform: Transform2D<f64>,
    ) -> bool {
        if composition_options.alpha < 1.0 ||
            composition_options.composition_operation !=
                CompositionOrBlending::Composition(CompositionStyle::SourceOver)
        {
            return false;
        }
        let FillOrStrokeStyle::Color(color) = style else {
            return false;
        };
        if color.into_srgb_legacy().alpha < 1.0 {
            return false;
        }
        self.covers_viewport(rect, transform)
    }
}

impl Drop for OhDrawingDrawTarget {
    fn drop(&mut self) {
        let Some(zc) = self.present.borrow_mut().take() else {
            return;
        };
        let window = zc.resolve_window();
        // Retire the id's registry entry; the pipeline itself (window, native image and its
        // persistent `QueueCounters`) may live on in the pool for a replacement canvas to adopt.
        if let Some(slot) = zc.slot.borrow().as_ref() {
            slot.dead.store(true, Ordering::Release);
        }
        match (window, zc.onscreen.borrow_mut().take()) {
            (0, _) => {},
            (window, Some(onscreen)) if !size_blacklisted(self.size) => pool_park(ParkedPipeline {
                width: self.size.width,
                height: self.size.height,
                onscreen,
                window,
            }),
            // Bind the surface rather than discarding it with `_`: a wildcard leaves it owned by
            // the match scrutinee, so it would drop only at the end of this `match` — i.e. after
            // the window has already been queued for consumer teardown (see `retire_pipeline`).
            (window, Some(onscreen)) => retire_pipeline(onscreen, window),
            (window, None) => ohdrawing_present::retire_window(window),
        }
    }
}

impl GenericDrawTarget for OhDrawingDrawTarget {
    type SourceSurface = SourceImage;

    fn new(size: Size2D<u32>) -> Self {
        // Clamp to a non-degenerate extent. `CanvasData` applies a minimum to the canvas itself,
        // but `create_similar_draw_target` does not: a shadowed draw of an empty rect
        // (`ctx.shadowBlur = 5; ctx.fillRect(x, y, 0, h)`) asks for a zero-extent target. Neither a
        // bitmap nor a GPU surface can be allocated at that size, and the readback path would then
        // produce a zero-byte buffer, so page content could take down the process-wide canvas paint
        // thread. A 1x1 target allocates fine and paints nothing observable.
        let size = Size2D::new(size.width.max(1), size.height.max(1)).cast::<i32>();
        let backing = gpu_context()
            .and_then(|context| {
                Surface::from_gpu_context(
                    context,
                    ImageInfo::rgba8888_premul(size.width, size.height),
                )
                .ok()
            })
            .map(Backing::Gpu)
            .unwrap_or_else(|| {
                let bitmap = Bitmap::new(
                    size.width as u32,
                    size.height as u32,
                    ColorFormat::Rgba8888,
                    AlphaFormat::Premul,
                )
                .expect("cpu bitmap allocation");
                Backing::Cpu(RefCell::new(bitmap))
            });
        log_mode_once(matches!(backing, Backing::Gpu(_)));
        Self {
            backing,
            size,
            clips: Vec::new(),
            present: RefCell::new(None),
            frame_started: Cell::new(false),
            frame_eligible: Cell::new(false),
            tee_active: Cell::new(false),
            eligible_frames: Cell::new(0),
            pending_attach_presents: Cell::new(0),
            skipped_flushes: Cell::new(0),
            present_buffers: RefCell::new(Vec::new()),
        }
    }

    fn create_similar_draw_target(&self, size: &Size2D<i32>) -> Self {
        Self::new(size.cast())
    }

    /// The surface, bitmap, paths and native image are allocated by OH_Drawing (or live in GPU
    /// memory), so `MallocSizeOfOps` cannot measure them. The readback presentation ring is
    /// ordinary Rust heap; report only the buffers still solely owned here, since a buffer
    /// WebRender is holding is accounted for on its side.
    fn canvas_store_sizes(
        &self,
        _ops: &mut malloc_size_of::MallocSizeOfOps,
    ) -> Option<Vec<CanvasStoreSizesPerType>> {
        let size = self
            .present_buffers
            .borrow()
            .iter()
            .filter(|buffer| Arc::strong_count(buffer) == 1)
            .map(|buffer| buffer.capacity())
            .sum();
        Some(vec![CanvasStoreSizesPerType {
            name: "readback-buffers",
            size,
            kind: ReportKind::ExplicitJemallocHeapSize,
        }])
    }

    fn get_size(&self) -> Size2D<i32> {
        self.size
    }

    fn clear_rect(&mut self, rect: &Rect<f32>, transform: Transform2D<f64>) {
        self.begin_frame_op(self.covers_viewport(rect, transform));
        self.with_state(transform, |canvas| {
            let Some(rect) = oh_rect_f32(rect) else {
                return;
            };
            let mut brush = Brush::new().expect("brush");
            brush.set_blend_mode(BlendMode::Clear);
            brush.set_antialias(false);
            canvas.with_brush(&brush, |canvas| canvas.draw_rect(&rect));
        });
    }

    fn fill(
        &mut self,
        path: &Path,
        fill_rule: FillRule,
        style: FillOrStrokeStyle,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        let Some(oh_path) = build_path(path, fill_rule) else {
            return;
        };
        self.with_state(transform, |canvas| {
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_fill_brush(&style, composition_options.alpha, |brush| {
                        canvas.with_brush(brush, |canvas| canvas.draw_path(&oh_path));
                    });
                },
            );
        });
    }

    fn fill_rect(
        &mut self,
        rect: &Rect<f32>,
        style: FillOrStrokeStyle,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(self.is_opaque_full_fill(
            rect,
            &style,
            &composition_options,
            transform,
        ));
        self.with_state(transform, |canvas| {
            let Some(rect) = oh_rect_f32(rect) else {
                return;
            };
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_fill_brush(&style, composition_options.alpha, |brush| {
                        canvas.with_brush(brush, |canvas| canvas.draw_rect(&rect));
                    });
                },
            );
        });
    }

    fn stroke(
        &mut self,
        path: &Path,
        style: FillOrStrokeStyle,
        line_options: LineOptions,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        let Some(oh_path) = build_path(path, FillRule::Nonzero) else {
            return;
        };
        self.with_state(transform, |canvas| {
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_stroke_pen(&style, &line_options, composition_options.alpha, |pen| {
                        canvas.with_pen(pen, |canvas| canvas.draw_path(&oh_path));
                    });
                },
            );
        });
    }

    fn stroke_rect(
        &mut self,
        rect: &Rect<f32>,
        style: FillOrStrokeStyle,
        line_options: LineOptions,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        self.with_state(transform, |canvas| {
            let Some(rect) = oh_rect_f32(rect) else {
                return;
            };
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_stroke_pen(&style, &line_options, composition_options.alpha, |pen| {
                        canvas.with_pen(pen, |canvas| canvas.draw_rect(&rect));
                    });
                },
            );
        });
    }

    fn fill_text(
        &mut self,
        text_runs: Vec<TextRun>,
        style: FillOrStrokeStyle,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        self.with_state(transform, |canvas| {
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_fill_brush(&style, composition_options.alpha, |brush| {
                        draw_text_runs(canvas, &text_runs, |canvas, blob| {
                            canvas
                                .with_brush(brush, |canvas| canvas.draw_text_blob(blob, 0.0, 0.0));
                        });
                    });
                },
            );
        });
    }

    fn stroke_text(
        &mut self,
        text_runs: Vec<TextRun>,
        style: FillOrStrokeStyle,
        line_options: LineOptions,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        self.with_state(transform, |canvas| {
            with_composition(
                canvas,
                composition_options.composition_operation,
                |canvas| {
                    with_stroke_pen(&style, &line_options, composition_options.alpha, |pen| {
                        draw_text_runs(canvas, &text_runs, |canvas, blob| {
                            canvas.with_pen(pen, |canvas| canvas.draw_text_blob(blob, 0.0, 0.0));
                        });
                    });
                },
            );
        });
    }

    fn draw_surface(
        &mut self,
        surface: Self::SourceSurface,
        dest: Rect<f64>,
        source: Rect<f64>,
        filter: Filter,
        composition_options: CompositionOptions,
        transform: Transform2D<f64>,
    ) {
        self.begin_frame_op(false);
        let sampling = sampling_for(filter);
        self.with_state(transform, |canvas| {
            let (Some(dst), Some(src)) = (oh_rect_f64(&dest), oh_rect_f64(&source)) else {
                return;
            };
            with_layer(
                canvas,
                composition_options.composition_operation,
                composition_options.alpha,
                |canvas| {
                    canvas.draw_image_rect_with_src(
                        &surface.image,
                        &src,
                        &dst,
                        &sampling,
                        SrcRectConstraint::Strict,
                    )
                },
            );
        });
    }

    fn draw_surface_with_shadow(
        &self,
        surface: Self::SourceSurface,
        dest: &Point2D<f32>,
        shadow_options: ShadowOptions,
        composition_options: CompositionOptions,
    ) {
        let width = surface.size.width as f32;
        let height = surface.size.height as f32;
        let image: &Image = &surface.image;
        // Position the image shader so the surface's top-left samples at `dest`.
        let mut matrix = match Matrix::new() {
            Ok(matrix) => matrix,
            Err(_) => return,
        };
        self.begin_frame_op(false);
        matrix.set(1.0, 0.0, dest.x, 0.0, 1.0, dest.y, 0.0, 0.0, 1.0);
        let sampling = sampling_for(Filter::Bilinear);
        let Ok(shader) = ShaderEffect::image_shader(
            image,
            TileMode::Decal,
            TileMode::Decal,
            &sampling,
            Some(&matrix),
        ) else {
            return;
        };
        // Canvas `shadowBlur` maps to a Gaussian standard deviation of blur / 2 (as in Blink/Gecko).
        let shadow = ShadowLayer::new(
            (shadow_options.blur / 2.0) as f32,
            shadow_options.offset_x as f32,
            shadow_options.offset_y as f32,
            to_color(shadow_options.color, 1.0),
        );
        self.with_state(Transform2D::identity(), |canvas| {
            let Some(dst) = OhRect::new(dest.x, dest.y, dest.x + width, dest.y + height).ok()
            else {
                return;
            };
            with_layer(
                canvas,
                composition_options.composition_operation,
                composition_options.alpha,
                |canvas| {
                    // An image blit ignores the attached brush, so draw the surface as an image
                    // shader on a brushed rect; that path honors the brush's shadow layer, painting
                    // the blurred, offset shadow beneath the content.
                    let mut brush = Brush::new().expect("brush");
                    brush.set_antialias(true);
                    brush.set_shader_effect(&shader);
                    if let Ok(shadow) = &shadow {
                        brush.set_shadow_layer(shadow);
                    }
                    canvas.with_brush(&brush, |canvas| canvas.draw_rect(&dst));
                },
            );
        });
    }

    fn copy_surface(
        &mut self,
        surface: Self::SourceSurface,
        source: Rect<i32>,
        destination: Point2D<i32>,
    ) {
        // This blit bypasses `with_state`, so it is not mirrored to the on-screen surface; disarm
        // teeing for the frame so it falls back to a correct readback present.
        self.begin_frame_op(false);
        self.tee_active.set(false);
        self.with_canvas(|canvas| {
            let base = canvas.save();
            canvas.reset_matrix();
            let dst = OhRect::new(
                destination.x as f32,
                destination.y as f32,
                (destination.x + source.size.width) as f32,
                (destination.y + source.size.height) as f32,
            );
            let src = OhRect::new(
                source.origin.x as f32,
                source.origin.y as f32,
                source.max_x() as f32,
                source.max_y() as f32,
            );
            if let (Ok(dst), Ok(src)) = (dst, src) {
                let sampling = sampling_for(Filter::Nearest);
                let mut brush = Brush::new().expect("brush");
                brush.set_blend_mode(BlendMode::Src);
                canvas.save_layer(Some(&dst), Some(&brush));
                canvas.draw_image_rect_with_src(
                    &surface.image,
                    &src,
                    &dst,
                    &sampling,
                    SrcRectConstraint::Strict,
                );
                canvas.restore();
            }
            canvas.restore_to_count(base);
        });
    }

    fn create_source_surface_from_data(&self, mut data: Snapshot) -> Option<Self::SourceSurface> {
        let size = data.size().cast::<i32>();
        data.transform(
            SnapshotAlphaMode::Transparent {
                premultiplied: true,
            },
            SnapshotPixelFormat::RGBA,
        );
        let bytes = data.as_raw_bytes();
        let key = content_key(bytes, size);
        let image = source_image_cached(key, size, bytes)?;
        Some(SourceImage { image, size })
    }

    fn surface(&mut self) -> Self::SourceSurface {
        let data = self.read_premul_rgba();
        // The target's own pixels are mutable, so this snapshot is not cached: build a fresh image.
        // `new` clamps the size, so the documented `from_pixels` rejections are unreachable here;
        // fall back to a blank 1x1 rather than panicking the shared canvas paint thread if the
        // allocation fails anyway.
        let built = Bitmap::from_pixels(
            ImageInfo::rgba8888_premul(self.size.width, self.size.height),
            &data,
            (self.size.width * 4) as u32,
        )
        .and_then(|bitmap| Image::from_bitmap(&bitmap));
        match built {
            Ok(image) => SourceImage {
                image: Rc::new(image),
                size: self.size,
            },
            Err(error) => {
                log::error!(
                    "[ohdrawing] snapshotting a {:?} draw target failed ({error:?}); \
                     substituting a blank source",
                    self.size
                );
                blank_source_image()
            },
        }
    }

    fn push_clip(&mut self, path: &Path, fill_rule: FillRule, transform: Transform2D<f64>) {
        if let Some(path) = build_path(path, fill_rule) {
            self.clips.push(Clip { path, transform });
        }
    }

    fn push_clip_rect(&mut self, rect: &Rect<i32>) {
        let mut path = Path::new();
        let rect = rect.cast::<f64>();
        path.rect(
            rect.origin.x,
            rect.origin.y,
            rect.size.width,
            rect.size.height,
        );
        self.push_clip(&path, FillRule::Nonzero, Transform2D::identity());
    }

    fn pop_clip(&mut self) {
        self.clips.pop();
    }

    fn present(&mut self) -> (ImageDescriptor, PresentationData) {
        let descriptor = ImageDescriptor {
            format: webrender_api::ImageFormat::RGBA8,
            size: self.size.cast_unit(),
            stride: None,
            offset: 0,
            flags: ImageDescriptorFlags::empty(),
        };
        let external = self.present_external();
        self.frame_started.set(false);
        self.frame_eligible.set(false);
        self.tee_active.set(false);
        if external {
            // Zero-copy: the frame is already queued on the on-screen surface; hand WebRender the
            // external OES texture instead of reading pixels back.
            (
                descriptor,
                PresentationData::External(ImageBufferKind::TextureExternal),
            )
        } else {
            let data = SerializableImageData::Raw(GenericSharedMemory::from_arc_vec(
                self.read_premul_rgba_arc(),
            ));
            (descriptor, PresentationData::Raw(data))
        }
    }

    fn set_external_image_id(&mut self, id: ExternalImageId) {
        // Only a GPU draw target can present zero-copy; a CPU-bitmap fallback always reads back.
        if !matches!(self.backing, Backing::Gpu(_)) {
            return;
        }
        let mut present = self.present.borrow_mut();
        match present.as_mut() {
            Some(zc) => {
                if zc.slot.borrow().is_some() {
                    return;
                }
                // Adopted pipeline getting its id: publish the known window. The pipeline's genuine
                // queue depth already lives in its persistent `QueueCounters` (keyed by window), so
                // nothing needs seeding here.
                let slot =
                    ohdrawing_present::register_slot(id.0, self.size.width, self.size.height);
                let window = zc.window.get();
                if window != 0 {
                    slot.window.store(window, Ordering::Release);
                }
                *zc.slot.borrow_mut() = Some(slot);
            },
            None => {
                let slot =
                    ohdrawing_present::register_slot(id.0, self.size.width, self.size.height);
                *present = Some(ZeroCopy {
                    slot: RefCell::new(Some(slot)),
                    onscreen: RefCell::new(None),
                    window: Cell::new(0),
                    counters: RefCell::new(None),
                });
            },
        }
    }

    fn external_image_handler() -> Option<Box<dyn WebRenderExternalImageApi + Send>> {
        Some(Box::new(OhDrawingImageHandler::new()))
    }

    fn snapshot(&mut self) -> Snapshot {
        Snapshot::from_vec(
            self.size.cast(),
            SnapshotPixelFormat::RGBA,
            SnapshotAlphaMode::Transparent {
                premultiplied: true,
            },
            self.read_premul_rgba(),
        )
    }
}

/// Apply `draw` on `canvas` inside a fresh save frame that re-establishes every clip and sets
/// `transform` as the active user-space transform.
fn apply_state(
    canvas: &Canvas,
    clips: &[Clip],
    transform: Transform2D<f64>,
    draw: &impl Fn(&Canvas),
) {
    let base = canvas.save();
    for clip in clips {
        set_transform(canvas, clip.transform);
        canvas.clip_path(&clip.path, ClipOp::Intersect, true);
    }
    set_transform(canvas, transform);
    draw(canvas);
    canvas.restore_to_count(base);
}

/// Set `canvas`'s transform from a 2D affine, mapping `(x, y)` to
/// `(m11·x + m21·y + m31, m12·x + m22·y + m32)`.
fn set_transform(canvas: &Canvas, transform: Transform2D<f64>) {
    let mut matrix = Matrix::new().expect("matrix");
    matrix.set(
        transform.m11 as f32,
        transform.m21 as f32,
        transform.m31 as f32,
        transform.m12 as f32,
        transform.m22 as f32,
        transform.m32 as f32,
        0.0,
        0.0,
        1.0,
    );
    canvas.set_matrix(&matrix);
}

/// Draw `f` under a composition operator. Non-`SourceOver` operators need an offscreen layer so the
/// operator composites over the whole (clipped) surface, matching canvas semantics.
fn with_composition(canvas: &Canvas, op: CompositionOrBlending, f: impl FnOnce(&Canvas)) {
    if op == CompositionOrBlending::Composition(CompositionStyle::SourceOver) {
        f(canvas);
        return;
    }
    let mut brush = Brush::new().expect("brush");
    brush.set_blend_mode(composite_blend(op));
    canvas.save_layer(None, Some(&brush));
    f(canvas);
    canvas.restore();
}

/// Like [`with_composition`], but also folds a global alpha into the compositing layer (used for
/// image draws, whose draw calls take no brush of their own).
fn with_layer(canvas: &Canvas, op: CompositionOrBlending, alpha: f64, f: impl FnOnce(&Canvas)) {
    let is_source_over = op == CompositionOrBlending::Composition(CompositionStyle::SourceOver);
    if is_source_over && alpha >= 1.0 {
        f(canvas);
        return;
    }
    let mut brush = Brush::new().expect("brush");
    if !is_source_over {
        brush.set_blend_mode(composite_blend(op));
    }
    if alpha < 1.0 {
        brush.set_alpha((alpha.clamp(0.0, 1.0) * 255.0).round() as u8);
    }
    canvas.save_layer(None, Some(&brush));
    f(canvas);
    canvas.restore();
}

/// Build a brush for `style` with `alpha` folded in, then run `f` while its shader (if any) is alive.
fn with_fill_brush(style: &FillOrStrokeStyle, alpha: f64, f: impl FnOnce(&Brush)) {
    let mut brush = Brush::new().expect("brush");
    brush.set_antialias(true);
    match style {
        FillOrStrokeStyle::Color(color) => {
            brush.set_color(to_color(*color, alpha));
            f(&brush);
        },
        _ => {
            let Some(shader) = build_shader(style) else {
                return;
            };
            brush.set_shader_effect(&shader);
            if alpha < 1.0 {
                brush.set_alpha((alpha.clamp(0.0, 1.0) * 255.0).round() as u8);
            }
            f(&brush);
        },
    }
}

/// Build a pen for `style`/`line_options` with `alpha` folded in, then run `f` while its shader and
/// dash effect (if any) are alive.
fn with_stroke_pen(
    style: &FillOrStrokeStyle,
    line_options: &LineOptions,
    alpha: f64,
    f: impl FnOnce(&Pen),
) {
    let mut pen = Pen::new().expect("pen");
    pen.set_antialias(true);
    pen.set_width(line_options.width as f32);
    pen.set_miter_limit(line_options.miter_limit as f32);
    pen.set_cap(match line_options.cap_style {
        LineCapStyle::Butt => LineCap::Flat,
        LineCapStyle::Round => LineCap::Round,
        LineCapStyle::Square => LineCap::Square,
    });
    pen.set_join(match line_options.join_style {
        LineJoinStyle::Round => LineJoin::Round,
        LineJoinStyle::Bevel => LineJoin::Bevel,
        LineJoinStyle::Miter => LineJoin::Miter,
    });

    // Skia dashes require a non-empty, even-length interval list; the canvas spec repeats an
    // odd-length list to make it even.
    let dash = (!line_options.dash.is_empty()).then(|| {
        let mut intervals = line_options.dash.clone();
        if intervals.len() % 2 == 1 {
            intervals.extend_from_within(..);
        }
        PathEffect::dash(&intervals, line_options.dash_offset as f32)
    });
    if let Some(Ok(dash)) = &dash {
        pen.set_path_effect(dash);
    }

    match style {
        FillOrStrokeStyle::Color(color) => {
            pen.set_color(to_color(*color, alpha));
            f(&pen);
        },
        _ => {
            let Some(shader) = build_shader(style) else {
                return;
            };
            pen.set_shader_effect(&shader);
            if alpha < 1.0 {
                // A pen has no separate alpha setter; fold alpha into the stroke color's alpha,
                // which modulates the shader in Skia.
                pen.set_color(Color::argb(
                    (alpha.clamp(0.0, 1.0) * 255.0).round() as u8,
                    0,
                    0,
                    0,
                ));
            }
            f(&pen);
        },
    }
}

/// Build the gradient/image [`ShaderEffect`] for a non-solid fill style.
fn build_shader(style: &FillOrStrokeStyle) -> Option<ShaderEffect> {
    match style {
        FillOrStrokeStyle::Color(_) => None,
        FillOrStrokeStyle::LinearGradient(gradient) => {
            let start = Point::new(gradient.x0 as f32, gradient.y0 as f32).ok()?;
            let end = Point::new(gradient.x1 as f32, gradient.y1 as f32).ok()?;
            let (colors, positions) = gradient_stops(&gradient.stops);
            ShaderEffect::linear_gradient(&start, &end, &colors, Some(&positions), TileMode::Clamp)
                .ok()
        },
        FillOrStrokeStyle::RadialGradient(gradient) => {
            let (colors, positions) = gradient_stops(&gradient.stops);
            ShaderEffect::two_point_conical_gradient(
                (gradient.x0 as f32, gradient.y0 as f32),
                gradient.r0 as f32,
                (gradient.x1 as f32, gradient.y1 as f32),
                gradient.r1 as f32,
                &colors,
                Some(&positions),
                TileMode::Clamp,
                None,
            )
            .ok()
        },
        FillOrStrokeStyle::Surface(surface) => {
            let mut snapshot = surface.surface_data.to_owned();
            snapshot.transform(
                SnapshotAlphaMode::Transparent {
                    premultiplied: true,
                },
                SnapshotPixelFormat::RGBA,
            );
            let size = snapshot.size().cast::<i32>();
            let bitmap = Bitmap::from_pixels(
                ImageInfo::rgba8888_premul(size.width, size.height),
                snapshot.as_raw_bytes(),
                (size.width * 4) as u32,
            )
            .ok()?;
            let image = Image::from_bitmap(&bitmap).ok()?;
            let sampling = sampling_for(Filter::Bilinear);
            let tile_x = if surface.repeat_x {
                TileMode::Repeat
            } else {
                TileMode::Clamp
            };
            let tile_y = if surface.repeat_y {
                TileMode::Repeat
            } else {
                TileMode::Clamp
            };
            let transform = surface.transform;
            let mut matrix = Matrix::new().ok()?;
            matrix.set(
                transform.m11,
                transform.m21,
                transform.m31,
                transform.m12,
                transform.m22,
                transform.m32,
                0.0,
                0.0,
                1.0,
            );
            // The image and bitmap must outlive the shader, but the shader takes its own reference,
            // so they can be dropped here.
            ShaderEffect::image_shader(&image, tile_x, tile_y, &sampling, Some(&matrix)).ok()
        },
    }
}

/// Split canvas gradient stops into parallel color and offset arrays.
fn gradient_stops(stops: &[CanvasGradientStop]) -> (Vec<Color>, Vec<f32>) {
    let mut colors = Vec::with_capacity(stops.len());
    let mut positions = Vec::with_capacity(stops.len());
    for stop in stops {
        colors.push(to_color(stop.color, 1.0));
        positions.push(stop.offset as f32);
    }
    (colors, positions)
}

/// For each pre-shaped text run, build the typeface-backed blob (caching typefaces per identifier)
/// and hand it to `draw`.
fn draw_text_runs(
    canvas: &Canvas,
    text_runs: &[TextRun],
    draw: impl Fn(&Canvas, &ohos_drawing::TextBlob),
) {
    for text_run in text_runs {
        let identifier = &text_run.font.identifier;
        let typeface = SHARED_FONT_CACHE.with(|cache| {
            if let Some(typeface) = cache.borrow().get(identifier) {
                return Some(typeface.clone());
            }
            let data = text_run.font.font_data_and_index()?;
            let stream = MemoryStream::from_bytes(data.data.as_ref(), true).ok()?;
            let typeface = Rc::new(Typeface::from_stream(stream, data.index as i32).ok()?);
            cache
                .borrow_mut()
                .insert(identifier.clone(), typeface.clone());
            Some(typeface)
        });
        let Some(typeface) = typeface else {
            continue;
        };

        let mut font = Font::new().expect("font");
        font.set_typeface(&typeface);
        font.set_text_size(text_run.pt_size);

        let glyph_count = text_run.glyphs_and_positions.len();
        if glyph_count == 0 {
            continue;
        }
        let mut builder = TextBlobBuilder::new().expect("text blob builder");
        {
            let Ok(mut run) = builder.alloc_run_pos(&font, glyph_count as i32, None) else {
                continue;
            };
            let glyphs = run.glyphs();
            for (slot, glyph) in glyphs.iter_mut().zip(&text_run.glyphs_and_positions) {
                *slot = glyph.id as u16;
            }
            let positions = run.positions();
            for (i, glyph) in text_run.glyphs_and_positions.iter().enumerate() {
                positions[i * 2] = glyph.point.x;
                positions[i * 2 + 1] = glyph.point.y;
            }
        }
        let Ok(blob) = builder.make() else {
            continue;
        };
        draw(canvas, &blob);
    }
}

/// Convert a kurbo path to an `OH_Drawing_Path` with the given fill rule.
fn build_path(path: &Path, fill_rule: FillRule) -> Option<OhPath> {
    let mut oh_path = OhPath::new().ok()?;
    for element in path.0.elements() {
        match element {
            PathEl::MoveTo(p) => oh_path.move_to(p.x as f32, p.y as f32),
            PathEl::LineTo(p) => oh_path.line_to(p.x as f32, p.y as f32),
            PathEl::QuadTo(c, p) => oh_path.quad_to(c.x as f32, c.y as f32, p.x as f32, p.y as f32),
            PathEl::CurveTo(c1, c2, p) => oh_path.cubic_to(
                c1.x as f32,
                c1.y as f32,
                c2.x as f32,
                c2.y as f32,
                p.x as f32,
                p.y as f32,
            ),
            PathEl::ClosePath => oh_path.close(),
        }
    }
    oh_path.set_fill_type(match fill_rule {
        FillRule::Nonzero => FillType::Winding,
        FillRule::Evenodd => FillType::EvenOdd,
    });
    Some(oh_path)
}

/// Convert an [`AbsoluteColor`] to an `OH_Drawing` ARGB color, scaling its alpha by `alpha_scale`.
fn to_color(color: AbsoluteColor, alpha_scale: f64) -> Color {
    let srgb = color.into_srgb_legacy();
    let channel = |value: f32| (value.clamp(0.0, 1.0) * 255.0).round() as u8;
    Color::argb(
        channel(srgb.alpha * alpha_scale as f32),
        channel(srgb.components.0),
        channel(srgb.components.1),
        channel(srgb.components.2),
    )
}

/// Map a canvas composite/blend operator to an `OH_Drawing` blend mode.
fn composite_blend(op: CompositionOrBlending) -> BlendMode {
    use servo_canvas_traits::canvas::BlendingStyle;
    match op {
        CompositionOrBlending::Composition(style) => match style {
            CompositionStyle::Clear => BlendMode::Clear,
            CompositionStyle::Copy => BlendMode::Src,
            CompositionStyle::SourceOver => BlendMode::SrcOver,
            CompositionStyle::DestinationOver => BlendMode::DstOver,
            CompositionStyle::SourceIn => BlendMode::SrcIn,
            CompositionStyle::DestinationIn => BlendMode::DstIn,
            CompositionStyle::SourceOut => BlendMode::SrcOut,
            CompositionStyle::DestinationOut => BlendMode::DstOut,
            CompositionStyle::SourceAtop => BlendMode::SrcAtop,
            CompositionStyle::DestinationAtop => BlendMode::DstAtop,
            CompositionStyle::Xor => BlendMode::Xor,
            CompositionStyle::Lighter => BlendMode::Plus,
        },
        CompositionOrBlending::Blending(style) => match style {
            BlendingStyle::Multiply => BlendMode::Multiply,
            BlendingStyle::Screen => BlendMode::Screen,
            BlendingStyle::Overlay => BlendMode::Overlay,
            BlendingStyle::Darken => BlendMode::Darken,
            BlendingStyle::Lighten => BlendMode::Lighten,
            BlendingStyle::ColorDodge => BlendMode::ColorDodge,
            BlendingStyle::ColorBurn => BlendMode::ColorBurn,
            BlendingStyle::HardLight => BlendMode::HardLight,
            BlendingStyle::SoftLight => BlendMode::SoftLight,
            BlendingStyle::Difference => BlendMode::Difference,
            BlendingStyle::Exclusion => BlendMode::Exclusion,
            BlendingStyle::Hue => BlendMode::Hue,
            BlendingStyle::Saturation => BlendMode::Saturation,
            BlendingStyle::Color => BlendMode::Color,
            BlendingStyle::Luminosity => BlendMode::Luminosity,
        },
    }
}

fn sampling_for(filter: Filter) -> SamplingOptions {
    let filter_mode = match filter {
        Filter::Bilinear => FilterMode::Linear,
        Filter::Nearest => FilterMode::Nearest,
    };
    SamplingOptions::new(filter_mode, MipmapMode::None).expect("sampling options")
}

fn oh_rect_f32(rect: &Rect<f32>) -> Option<OhRect> {
    OhRect::new(rect.min_x(), rect.min_y(), rect.max_x(), rect.max_y()).ok()
}

fn oh_rect_f64(rect: &Rect<f64>) -> Option<OhRect> {
    OhRect::new(
        rect.min_x() as f32,
        rect.min_y() as f32,
        rect.max_x() as f32,
        rect.max_y() as f32,
    )
    .ok()
}
