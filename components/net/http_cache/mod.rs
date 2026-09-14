/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![deny(missing_docs)]

//! Servo's HTTP cache.
//!
//! The RFC 9111 rules — freshness, `Vary`, revalidation and the 304 merge — come
//! from `http-cache-semantics`, in [`policy`]. On top of them this module keeps
//! the parts of caching that belong to the fetch layer: `stale-while-revalidate`,
//! the fetch cache modes, invalidation of unsafe methods and range requests.
//!
//! Storage lives behind [`CacheStore`], which streams bodies in and out and never
//! hands out a complete body, so a cached response does not have to exist as a
//! single allocation in the net process.

use std::sync::Arc;
use std::time::SystemTime;

use futures::StreamExt;
use headers::HeaderMapExt;
use http::{HeaderMap, Method, StatusCode, header};
use log::debug;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use malloc_size_of_derive::MallocSizeOf as MallocSizeOfDerive;
use net_traits::http_status::HttpStatus;
use net_traits::request::{CacheMode, Request, RequestMode};
use net_traits::response::{CacheState, Response, ResponseBody};
use net_traits::{CacheEntryDescriptor, NetworkError, ResourceFetchTiming};
use parking_lot::Mutex;
use servo_arc::Arc as ServoArc;
use servo_config::pref;
use servo_url::ServoUrl;
use tokio::sync::mpsc::{UnboundedSender as TokioSender, unbounded_channel as unbounded};

use crate::connector::BoxedBody;
use crate::decoder::{Decoder, DecoderType};
use crate::fetch::methods::{Data, DoneChannel, retains_whole_body};
use crate::http_cache::inflight::{InFlight, InFlightState, InFlightWriter, RevalidationGuards};
use crate::http_cache::key::EntryId;
use crate::http_cache::memory_store::MemoryStore;
use crate::http_cache::policy::{EntryPolicy, Freshness, request_demands_revalidation};
use crate::http_cache::range::RangeOutcome;
use crate::http_cache::store::{BodyReader, CACHE_FORMAT, CacheStore, EntryMeta, EntryWriter};
use crate::http_cache::tee::TeeBody;

pub mod disk;
mod inflight;
mod key;
pub mod memory_store;
pub mod policy;
mod range;
pub mod store;
mod tee;

pub use crate::http_cache::inflight::RevalidationGuard;
pub use crate::http_cache::key::CacheKey;

/// How many times a lookup will wait for another fetch to finish writing the same
/// key before giving up and going to the network itself.
const MAX_INFLIGHT_WAITS: usize = 8;

/// Is this cache the private or the public one?
#[derive(Clone, Copy, Debug, MallocSizeOfDerive, PartialEq)]
pub enum HttpCacheAssignment {
    /// The cache shared by ordinary browsing, which may be backed by disk.
    Public,
    /// The private-browsing cache, which is always memory-only.
    Private,
}

/// Servo's HTTP cache.
pub struct HttpCache {
    store: Arc<dyn CacheStore>,
    inflight: Arc<InFlight>,
    revalidations: Arc<RevalidationGuards>,
    /// Which side of the browsing session this cache serves.
    assignment: HttpCacheAssignment,
}

impl MallocSizeOf for HttpCache {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.store.size_of(ops)
    }
}

impl HttpCache {
    /// Create a cache for the given side of the browsing session. The public cache
    /// is backed by disk when the embedder supplied a cache directory; everything
    /// else is memory-only.
    pub fn new(assignment: HttpCacheAssignment) -> Self {
        let memory_budget = pref!(network_http_memory_cache_size).max(0) as usize;
        let store: Arc<dyn CacheStore> = match assignment {
            HttpCacheAssignment::Public => disk::open_default_store()
                .unwrap_or_else(|| Arc::new(MemoryStore::new(memory_budget))),
            HttpCacheAssignment::Private => Arc::new(MemoryStore::new(memory_budget)),
        };
        Self {
            store,
            inflight: Arc::new(InFlight::default()),
            revalidations: Arc::new(RevalidationGuards::default()),
            assignment,
        }
    }

    /// A cache backed by an explicit store. Used by the store test suite.
    pub fn with_store(store: Arc<dyn CacheStore>) -> Self {
        Self {
            store,
            inflight: Arc::new(InFlight::default()),
            revalidations: Arc::new(RevalidationGuards::default()),
            assignment: HttpCacheAssignment::Public,
        }
    }

    /// The bytes this cache holds in the process, for `about:memory`.
    pub fn stored_bytes(&self) -> usize {
        self.store.stored_bytes()
    }

    /// The bytes this cache holds on disk, for `about:memory`.
    pub fn disk_bytes(&self) -> u64 {
        self.store.disk_bytes()
    }

    /// Whether this is the public cache.
    pub fn assignment(&self) -> HttpCacheAssignment {
        self.assignment
    }

    /// Descriptors for the entries devtools lists.
    pub(crate) async fn cache_entry_descriptors(&self) -> Vec<CacheEntryDescriptor> {
        self.store.descriptors().await
    }

    /// Drop everything this cache holds.
    pub(crate) async fn clear(&self) {
        self.store.clear().await;
    }

    /// Persist anything that only lives in memory. Called when the embedding
    /// application is backgrounded, since it may be killed without further notice.
    pub(crate) async fn flush(&self) {
        self.store.flush().await;
    }

    /// Flush anything that only lives in memory. Called when the resource thread exits.
    pub(crate) async fn shutdown(&self) {
        self.store.shutdown().await;
    }

    /// Consult the cache before going to the network.
    ///
    /// Implements the cache half of step 8.25 of
    /// <https://fetch.spec.whatwg.org/#concept-http-network-or-cache-fetch>: it
    /// selects a stored response, applies the request's cache mode to it, and sets
    /// the conditional headers when the stored response has to be revalidated.
    ///
    /// The returned [`CacheTransaction`] carries what the rest of the fetch needs:
    /// the entry being revalidated and, on a miss, the right to write this key.
    pub(crate) async fn read(
        &self,
        request: &mut Request,
        done_chan: &mut DoneChannel,
        revalidating_flag: &mut bool,
    ) -> (CacheTransaction, CacheLookup) {
        let mut transaction = self.new_transaction(request);
        let key = transaction.key.clone();
        *done_chan = None;

        if !transaction.storable {
            return (transaction, CacheLookup::default());
        }

        let selected = self.select_variant(&mut transaction, request).await;
        let mut lookup = CacheLookup::default();

        if let Some((id, meta, freshness)) = selected {
            let serve_stale = match &freshness {
                Freshness::Fresh => false,
                Freshness::Stale { stale_for, .. } => {
                    let window = meta.policy.stale_while_revalidate();
                    !window.is_zero() &&
                        *stale_for <= window &&
                        !request_demands_revalidation(request)
                },
                // `select_variant` only ever returns a matching variant.
                Freshness::NoMatch => false,
            };

            // Substeps 1 to 4: the fetch cache modes decide whether the stored
            // response may be used at all.
            let (usable, needs_synchronous_revalidation) = match (request.cache_mode, &request.mode)
            {
                (CacheMode::ForceCache, _) => (true, false),
                (CacheMode::OnlyIfCached, &RequestMode::SameOrigin) => (true, false),
                (CacheMode::OnlyIfCached, _) | (CacheMode::NoStore, _) | (CacheMode::Reload, _) => {
                    (false, false)
                },
                (_, _) => match &freshness {
                    Freshness::Fresh => (true, false),
                    Freshness::Stale { .. } if serve_stale => (true, false),
                    Freshness::Stale { .. } => (false, true),
                    // `select_variant` only ever returns a matching variant.
                    Freshness::NoMatch => (false, false),
                },
            };
            // RFC 9111 says a request's `no-store` "does not apply to the already
            // stored response", but no browser reuses one either, and the
            // web-platform tests require that it is not reused.
            let (usable, needs_synchronous_revalidation) =
                if request_forbids_stored_responses(request) {
                    (false, false)
                } else {
                    (usable, needs_synchronous_revalidation)
                };

            if needs_synchronous_revalidation {
                // Substep 5: revalidate before use.
                *revalidating_flag = true;
                if let Freshness::Stale { revalidation, .. } = &freshness {
                    copy_conditional_headers(&revalidation.headers, &mut request.headers);
                }
                transaction.revalidating = Some((id, meta));
            } else if usable {
                // Substep 6.
                match self
                    .serve(request, id, &meta, done_chan, CacheState::Local)
                    .await
                {
                    Ok(response) => {
                        self.store.touch(id);
                        if serve_stale {
                            lookup.revalidate_in_background = self.revalidations.try_acquire(&key);
                        }
                        lookup.response = Some(response);
                    },
                    Err(ServeError::Gone) => {
                        // The entry vanished between the lookup and the open, which
                        // the store is allowed to do. Fall back to the network.
                        self.store.remove(id).await;
                        *done_chan = None;
                    },
                    Err(ServeError::RangeNotSatisfiable) => {
                        // Keep the entry: this request just cannot be answered from it.
                        *done_chan = None;
                    },
                }
            }
        }

        if lookup.response.is_none() && transaction.writer.is_none() {
            transaction.writer = self.inflight.register(&transaction.key).ok();
        }
        (transaction, lookup)
    }

    fn new_transaction(&self, request: &Request) -> CacheTransaction {
        CacheTransaction {
            key: CacheKey::new(request),
            superseded: Vec::new(),
            revalidating: None,
            writer: None,
            storable: is_cacheable_request(request),
        }
    }

    /// Find the stored variant that answers `request`, waiting for a concurrent
    /// fetch of the same key if one is already writing it.
    async fn select_variant(
        &self,
        transaction: &mut CacheTransaction,
        request: &Request,
    ) -> Option<(EntryId, EntryMeta, Freshness)> {
        let now = SystemTime::now();
        for _ in 0..MAX_INFLIGHT_WAITS {
            let variants = self.store.lookup(&transaction.key).await;
            let mut stale = None;
            for (id, meta) in variants {
                if matches!(range::select(request, &meta), RangeOutcome::Unsatisfiable) {
                    continue;
                }
                match meta.policy.evaluate(request, &meta.headers, now) {
                    Freshness::Fresh => {
                        transaction.superseded.push(id);
                        return Some((id, meta, Freshness::Fresh));
                    },
                    freshness @ Freshness::Stale { .. } => {
                        transaction.superseded.push(id);
                        if stale.is_none() {
                            stale = Some((id, meta, freshness));
                        }
                    },
                    Freshness::NoMatch => {},
                }
            }
            if stale.is_some() {
                return stale;
            }

            // Nothing usable. Either become the writer for this key, or wait for
            // whoever already is.
            match self.inflight.register(&transaction.key) {
                Ok(writer) => {
                    transaction.writer = Some(writer);
                    return None;
                },
                Err(receiver) => match inflight::wait(receiver).await {
                    InFlightState::Committed(_) | InFlightState::Aborted => continue,
                    InFlightState::Writing => return None,
                },
            }
        }
        None
    }

    /// Build a response that streams a stored entry.
    async fn serve(
        &self,
        request: &Request,
        id: EntryId,
        meta: &EntryMeta,
        done_chan: &mut DoneChannel,
        cache_state: CacheState,
    ) -> Result<Response, ServeError> {
        let mut headers = policy::presented_headers(&meta.headers, &meta.policy, SystemTime::now());
        let mut status = meta.status.clone();

        let body_range = match range::select(request, meta) {
            RangeOutcome::Whole => None,
            RangeOutcome::Unsatisfiable => return Err(ServeError::RangeNotSatisfiable),
            RangeOutcome::Satisfiable(hit) => {
                range::apply_headers(&mut headers, &hit);
                status = StatusCode::PARTIAL_CONTENT.into();
                Some(hit.body)
            },
        };

        let reader = match self.store.open(id, meta, body_range).await {
            Ok(reader) => reader,
            Err(error) => {
                debug!("could not open cache entry for {}: {error}", meta.key.url());
                return Err(ServeError::Gone);
            },
        };

        let resource_timing = ResourceFetchTiming::new(request.timing_type());
        let mut response = Response::new(meta.final_url.clone(), resource_timing);
        response.status = status;
        response.headers = headers;
        response.referrer = request.referrer.to_url().cloned();
        response.referrer_policy = request.referrer_policy;
        response.cache_state = cache_state;

        let (sender, receiver) = unbounded();
        *done_chan = Some((sender.clone(), receiver));
        let accumulate = retains_whole_body(request);
        *response.body.lock() = if accumulate {
            ResponseBody::Receiving(Vec::new())
        } else {
            ResponseBody::Streamed
        };
        spawn_entry_stream(reader, sender, response.body.clone(), accumulate);
        Ok(response)
    }

    /// Freshening a stored response upon validation.
    /// <https://httpwg.org/specs/rfc9111.html#freshening.responses>
    pub(crate) async fn refresh(
        &self,
        transaction: &mut CacheTransaction,
        request: &Request,
        forward_response: &Response,
        done_chan: &mut DoneChannel,
    ) -> Option<Response> {
        let (id, meta) = transaction.revalidating.take()?;
        let status = forward_response.status.try_code()?;
        let (policy, merged) = meta.policy.refresh(
            request,
            meta.status.try_code()?,
            status,
            &forward_response.headers,
            &meta.headers,
            SystemTime::now(),
        )?;

        let mut refreshed = meta.clone();
        refreshed.policy = policy;
        refreshed.headers = merged.into();
        if let Err(error) = self.store.update_meta(id, refreshed.clone()).await {
            debug!("could not refresh cache entry: {error}");
            return None;
        }
        self.store.touch(id);
        self.serve(request, id, &refreshed, done_chan, CacheState::Validated)
            .await
            .ok()
    }

    /// Copy a network body into the cache as it is delivered, when the response may
    /// be stored. Returns the body to hand on to the decoder.
    ///
    /// <https://httpwg.org/specs/rfc9111.html#storing.responses.in.caches>
    pub(crate) async fn tee(
        &self,
        transaction: &mut CacheTransaction,
        request: &Request,
        status: &HttpStatus,
        headers: &HeaderMap,
        body: BoxedBody,
    ) -> BoxedBody {
        let Some(inflight) = transaction.writer.take() else {
            return body;
        };
        let Some(meta) = self.entry_meta(transaction, request, status, headers) else {
            inflight.abort();
            return body;
        };

        // A stored response replaces the variants this request already matched.
        for id in std::mem::take(&mut transaction.superseded) {
            self.store.remove(id).await;
        }

        match self.store.create(meta).await {
            Ok(writer) => tee_body(body, writer, inflight),
            Err(error) => {
                debug!("cache store declined the entry: {error}");
                inflight.abort();
                body
            },
        }
    }

    /// The metadata a response would be stored with, or `None` if it must not be stored.
    fn entry_meta(
        &self,
        transaction: &CacheTransaction,
        request: &Request,
        status: &HttpStatus,
        headers: &HeaderMap,
    ) -> Option<EntryMeta> {
        if !transaction.storable || request.cache_mode == CacheMode::NoStore {
            return None;
        }
        let code = status.try_code()?;
        // A partial response is only worth storing if it says which part it holds.
        if code == StatusCode::PARTIAL_CONTENT && !headers.contains_key(header::CONTENT_RANGE) {
            return None;
        }
        // `Vary: *` never matches a later request, so such an entry could only ever
        // occupy a slot under its key.
        if policy::varies_on_everything(headers) {
            return None;
        }
        let policy = EntryPolicy::new(request, code, headers, SystemTime::now());
        if !policy.is_storable() {
            return None;
        }
        // Refuse outsized entries up front rather than streaming one into a store
        // that will decline it at commit time anyway.
        if let Some(length) = headers.typed_get::<headers::ContentLength>() &&
            length.0 > self.store.max_entry_bytes()
        {
            return None;
        }

        let mut stored_headers = headers.clone();
        policy::strip_unstorable_fields(&mut stored_headers);

        Some(EntryMeta {
            format: CACHE_FORMAT,
            key: transaction.key.clone(),
            policy,
            content_encoding: DecoderType::detect(headers),
            headers: stored_headers.into(),
            status: status.clone(),
            final_url: transaction.key.url().clone(),
            body_len: 0,
        })
    }

    /// Invalidating stored responses after an unsafe method.
    /// <https://httpwg.org/specs/rfc9111.html#invalidation>
    pub(crate) async fn invalidate(
        &self,
        transaction: &CacheTransaction,
        request: &Request,
        response: &Response,
    ) {
        self.store.remove_key(&transaction.key).await;
        for header_name in [header::LOCATION, header::CONTENT_LOCATION] {
            if let Some(url) = resolve_location_url(request, response, header_name) {
                let key = CacheKey::from_url(url);
                if key != transaction.key {
                    self.store.remove_key(&key).await;
                }
            }
        }
    }
}

/// Whether a stored response is fresh, or needs validating before or after use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationStatus {
    /// Fresh; usable without any revalidation.
    Valid,
    /// Stale.
    Stale {
        /// Whether it may still be served immediately, leaving the caller to
        /// revalidate it in the background.
        revalidate_in_background: bool,
    },
}

#[cfg(feature = "test-util")]
impl HttpCache {
    /// Store a response and its body the way the network path would.
    /// Returns whether the response was storable.
    pub async fn store_for_test(
        &self,
        request: &Request,
        response: &Response,
        body: &[u8],
    ) -> bool {
        let transaction = self.new_transaction(request);
        let Some(meta) =
            self.entry_meta(&transaction, request, &response.status, &response.headers)
        else {
            return false;
        };
        let Ok(mut writer) = self.store.create(meta).await else {
            return false;
        };
        if !body.is_empty() {
            writer.push(bytes::Bytes::copy_from_slice(body));
        }
        writer.commit().await.is_ok()
    }

    /// What the cache would do with `request`, or `None` for a miss.
    pub async fn probe(&self, request: &Request) -> Option<ValidationStatus> {
        let mut transaction = self.new_transaction(request);
        let (_, meta, freshness) = self.select_variant(&mut transaction, request).await?;
        Some(match freshness {
            Freshness::Fresh => ValidationStatus::Valid,
            Freshness::Stale { stale_for, .. } => {
                let window = meta.policy.stale_while_revalidate();
                ValidationStatus::Stale {
                    revalidate_in_background: !window.is_zero() &&
                        stale_for <= window &&
                        !request_demands_revalidation(request),
                }
            },
            Freshness::NoMatch => return None,
        })
    }

    /// The headers a stored response would be served with. Only for tests.
    pub async fn headers_for_test(&self, request: &Request) -> Option<HeaderMap> {
        let mut transaction = self.new_transaction(request);
        let (_, meta, _) = self.select_variant(&mut transaction, request).await?;
        Some(policy::presented_headers(
            &meta.headers,
            &meta.policy,
            SystemTime::now(),
        ))
    }

    /// Read a stored response back as one buffer. Only for tests.
    pub async fn read_body_for_test(&self, request: &Request) -> Option<Vec<u8>> {
        let mut transaction = self.new_transaction(request);
        let (id, meta, _) = self.select_variant(&mut transaction, request).await?;
        let reader = self.store.open(id, &meta, None).await.ok()?;
        let mut stream = reader.into_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk.ok()?);
        }
        Some(body)
    }
}

/// Why a stored entry could not answer a request.
enum ServeError {
    /// The entry is no longer there.
    Gone,
    /// The entry is fine, but it cannot serve the requested range.
    RangeNotSatisfiable,
}

/// What a cache lookup produced.
#[derive(Default)]
pub(crate) struct CacheLookup {
    /// The response to serve, if the cache could answer the request.
    pub response: Option<Response>,
    /// Held while a `stale-while-revalidate` refresh should be started for this
    /// key; dropping it lets the next stale hit start another one.
    pub revalidate_in_background: Option<RevalidationGuard>,
}

/// State a fetch carries from consulting the cache to storing the network response.
pub(crate) struct CacheTransaction {
    key: CacheKey,
    /// Variants this request matched. A newly stored response replaces them.
    superseded: Vec<EntryId>,
    /// The entry a 304 would freshen.
    revalidating: Option<(EntryId, EntryMeta)>,
    /// Held while this fetch is the one allowed to write `key`.
    writer: Option<InFlightWriter>,
    /// False when this request can never be cached, whatever comes back.
    storable: bool,
}

/// Whether the request refuses to be answered from a cache at all.
fn request_forbids_stored_responses(request: &Request) -> bool {
    request
        .headers
        .typed_get::<headers::CacheControl>()
        .is_some_and(|directive| directive.no_store())
}

fn is_cacheable_request(request: &Request) -> bool {
    !pref!(network_http_cache_disabled) && request.method == Method::GET
}

/// Copy the conditional headers a revalidation needs onto the outgoing request.
fn copy_conditional_headers(from: &HeaderMap, to: &mut HeaderMap) {
    for name in [header::IF_NONE_MATCH, header::IF_MODIFIED_SINCE] {
        match from.get(&name) {
            Some(value) => {
                to.insert(name, value.clone());
            },
            None => {
                to.remove(&name);
            },
        }
    }
}

fn resolve_location_url(
    request: &Request,
    response: &Response,
    header_name: header::HeaderName,
) -> Option<ServoUrl> {
    response
        .headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .and_then(|location| request.current_url().join(location).ok())
}

fn tee_body(body: BoxedBody, writer: EntryWriter, inflight: InFlightWriter) -> BoxedBody {
    use http_body_util::BodyExt;
    TeeBody::new(body, writer, inflight).boxed()
}

/// Stream a stored entry to the fetch consumer, decoding it on the way.
fn spawn_entry_stream(
    reader: BodyReader,
    sender: TokioSender<Data>,
    body: ServoArc<Mutex<ResponseBody>>,
    accumulate: bool,
) {
    let encoding = reader.content_encoding();
    let stored_len = reader.len();
    tokio::spawn(async move {
        if encoding.is_none() {
            let _ = sender.send(Data::ContentLength(stored_len as usize));
        }
        let mut decoder = Box::pin(Decoder::for_stream(reader.into_stream(), encoding));
        while let Some(item) = decoder.next().await {
            match item {
                Ok(chunk) => {
                    if accumulate && let ResponseBody::Receiving(bytes) = &mut *body.lock() {
                        bytes.extend_from_slice(&chunk);
                    }
                    if sender.send(Data::Payload(chunk)).is_err() {
                        break;
                    }
                },
                Err(error) => {
                    debug!("error reading a cached body: {error}");
                    let _ = sender.send(Data::Error(NetworkError::DecompressionError));
                    break;
                },
            }
        }
        {
            let mut body = body.lock();
            if let ResponseBody::Receiving(bytes) = &mut *body {
                let bytes = std::mem::take(bytes);
                *body = ResponseBody::Done(bytes);
            }
        }
        let _ = sender.send(Data::Done);
    });
}
