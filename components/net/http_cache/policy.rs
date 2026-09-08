/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! RFC 9111 cache semantics for stored entries.
//!
//! Freshness, `Vary` matching, revalidation requests and the 304 merge come from
//! `http-cache-semantics`. Servo layers `stale-while-revalidate` (RFC 5861) on
//! top of it, since the crate does not implement that extension.

use std::time::{Duration, SystemTime};

use headers::HeaderMapExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use http_cache_semantics::{
    AfterResponse, BeforeRequest, CacheOptions, CachePolicy, RequestLike, ResponseLike,
};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use net_traits::request::Request;
use serde::{Deserialize, Serialize};
use servo_url::ServoUrl;
use sha1::{Digest, Sha1};

/// Servo's HTTP cache is a private, single-user cache: `private` responses and
/// responses to requests carrying `Authorization` are storable and `s-maxage` is
/// ignored. `cache_heuristic` and `immutable_min_time_to_live` are the crate
/// defaults, which match the values other engines use.
const CACHE_OPTIONS: CacheOptions = CacheOptions {
    shared: false,
    cache_heuristic: 0.1,
    immutable_min_time_to_live: Duration::from_secs(24 * 60 * 60),
    ignore_cargo_cult: false,
};

/// Headers that must not be served from a cache. Mirrors the list in
/// `http-cache-semantics`, which strips them from its own generated responses.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The statuses `http-cache-semantics` understands, which is RFC 9110's list of
/// heuristically cacheable ones. It refuses to store anything else, and computes a
/// zero freshness lifetime for it.
const HEURISTICALLY_CACHEABLE: &[u16] = &[
    200, 203, 204, 300, 301, 302, 303, 307, 308, 404, 405, 410, 414, 501,
];

/// RFC 9111 also allows storing any *other* final status that carries explicit
/// freshness information, which is what browsers do and what the `status`
/// web-platform tests require. `http-cache-semantics` gates both storability and
/// the freshness lifetime on its own list, so such a response is handed to it
/// under a status it does understand; the real status lives in `EntryMeta` and is
/// what a cache hit is served with.
///
/// <https://httpwg.org/specs/rfc9111.html#response.cacheability>
fn policy_status(status: StatusCode, headers: &HeaderMap) -> StatusCode {
    if HEURISTICALLY_CACHEABLE.contains(&status.as_u16()) ||
        // A 1xx is not a final response, and a 304 is an answer about a stored
        // response rather than one to store.
        status.is_informational() ||
        status == StatusCode::NOT_MODIFIED ||
        !has_explicit_freshness(headers)
    {
        return status;
    }
    StatusCode::OK
}

/// Whether a response carries permission to be reused without asking again, either
/// by saying for how long or by saying that it may be cached at all.
///
/// `public` counts even though it says nothing about duration: `heuristic.any.js`
/// requires that an unknown status with `Cache-Control: public` and a
/// `Last-Modified` be reused on heuristic freshness, and that the same response
/// without `public` is not. `private` is the same permission for a private cache,
/// which is what Servo's is.
fn has_explicit_freshness(headers: &HeaderMap) -> bool {
    if headers.contains_key(header::EXPIRES) {
        return true;
    }
    let Some(directive) = headers.typed_get::<headers::CacheControl>() else {
        return false;
    };
    directive.max_age().is_some() || directive.public() || directive.private()
}

/// Request header fields that carry credentials.
///
/// `CachePolicy` keeps a copy of the request headers it was built with, and the
/// policy is serialized into every stored entry, so these must never reach it
/// verbatim: a cache entry would otherwise hold the session cookie on disk,
/// outliving the cookie itself. The crate only ever reads a stored request field
/// back to compare it against the one presented, and only for the fields the
/// response's `Vary` names, so a digest compares exactly as well as the value --
/// which is also what Chromium stores (`HttpVaryData`).
const CREDENTIAL_HEADERS: [header::HeaderName; 3] = [
    header::COOKIE,
    header::AUTHORIZATION,
    header::PROXY_AUTHORIZATION,
];

/// A Servo [`Request`] seen through the eyes of `http-cache-semantics`.
pub(crate) struct PolicyRequest<'a> {
    uri: Uri,
    method: &'a Method,
    headers: HeaderMap,
}

impl<'a> PolicyRequest<'a> {
    /// `response_headers` are those of the response the policy describes, whose
    /// `Vary` decides which credentials still have to be comparable. Both the
    /// stored and the presented request must be built this way, so that the two
    /// are compared in the same form.
    pub(crate) fn new(request: &'a Request, response_headers: &HeaderMap) -> Self {
        let mut headers = request.headers.clone();
        for name in CREDENTIAL_HEADERS {
            if !headers.contains_key(&name) {
                continue;
            }
            if varies_on(response_headers, &name) {
                let digest = digest_of(headers.get_all(&name));
                headers.insert(name, digest);
            } else {
                headers.remove(&name);
            }
        }
        Self {
            uri: uri_for(&request.current_url()),
            method: &request.method,
            headers,
        }
    }
}

/// The request header fields a response's `Vary` nominates.
fn vary_fields(response_headers: &HeaderMap) -> impl Iterator<Item = &str> {
    response_headers
        .get_all(header::VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
}

/// Whether a response varies on every request field, which no stored entry can
/// ever match.
pub(crate) fn varies_on_everything(response_headers: &HeaderMap) -> bool {
    vary_fields(response_headers).any(|field| field == "*")
}

/// Whether a response's `Vary` nominates `name`.
fn varies_on(response_headers: &HeaderMap, name: &header::HeaderName) -> bool {
    vary_fields(response_headers)
        .any(|field| field == "*" || field.eq_ignore_ascii_case(name.as_str()))
}

/// A stable digest of a header field's values, used in place of a credential.
fn digest_of(values: header::GetAll<HeaderValue>) -> HeaderValue {
    let mut hasher = Sha1::new();
    for value in values {
        hasher.update(value.as_bytes());
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    HeaderValue::from_str(&hex).expect("hex is a valid header value")
}

impl RequestLike for PolicyRequest<'_> {
    fn uri(&self) -> Uri {
        self.uri.clone()
    }

    fn is_same_uri(&self, other: &Uri) -> bool {
        &self.uri == other
    }

    fn method(&self) -> &Method {
        self.method
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

/// A response seen through the eyes of `http-cache-semantics`.
pub(crate) struct PolicyResponse<'a> {
    status: StatusCode,
    headers: &'a HeaderMap,
}

impl<'a> PolicyResponse<'a> {
    pub(crate) fn new(status: StatusCode, headers: &'a HeaderMap) -> Self {
        Self { status, headers }
    }
}

impl ResponseLike for PolicyResponse<'_> {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn headers(&self) -> &HeaderMap {
        self.headers
    }
}

/// `ServoUrl` always round-trips through `Uri` for http(s) URLs, which are the
/// only ones that reach the HTTP cache. Anything else keeps the default `Uri`,
/// which is harmless because entries are already separated by their cache key.
fn uri_for(url: &ServoUrl) -> Uri {
    url.as_str().parse().unwrap_or_default()
}

/// What a stored variant can do for an incoming request.
pub(crate) enum Freshness {
    /// Usable as-is, without contacting the server.
    Fresh,
    /// Semantically the same resource, but it needs revalidating.
    Stale {
        /// How long the entry has been stale. Compared against the
        /// `stale-while-revalidate` window to decide whether it can still be served.
        stale_for: Duration,
        /// Conditional headers to copy onto the outgoing request.
        revalidation: Box<http::request::Parts>,
    },
    /// Not the resource this request is asking for (different method, or the
    /// `Vary`-nominated headers do not match).
    NoMatch,
}

/// The caching decisions attached to one stored variant.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EntryPolicy {
    policy: CachePolicy,
    /// When the response was received. `CachePolicy` keeps this privately but
    /// does not expose it, and it is needed to recover the freshness lifetime
    /// once the entry has gone stale.
    response_time: SystemTime,
    /// The `stale-while-revalidate` window (RFC 5861).
    stale_while_revalidate: Duration,
}

impl MallocSizeOf for EntryPolicy {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        // `CachePolicy` keeps a copy of the request and response header maps, and
        // exposes neither. Charge a fixed estimate rather than nothing at all.
        2 * 512
    }
}

impl EntryPolicy {
    pub(crate) fn new(
        request: &Request,
        status: StatusCode,
        headers: &HeaderMap,
        response_time: SystemTime,
    ) -> Self {
        let policy = CachePolicy::new_options(
            &PolicyRequest::new(request, headers),
            &PolicyResponse::new(policy_status(status, headers), headers),
            response_time,
            CACHE_OPTIONS,
        );
        Self {
            policy,
            response_time,
            stale_while_revalidate: stale_while_revalidate_window(headers),
        }
    }

    /// Whether this response may be written to the cache at all.
    /// <https://httpwg.org/specs/rfc9111.html#response.cacheability>
    pub(crate) fn is_storable(&self) -> bool {
        self.policy.is_storable()
    }

    /// The `stale-while-revalidate` window advertised by the response.
    pub(crate) fn stale_while_revalidate(&self) -> Duration {
        self.stale_while_revalidate
    }

    /// How long the stored response has been sitting in caches.
    pub(crate) fn age(&self, now: SystemTime) -> Duration {
        self.policy.age(now)
    }

    /// The freshness lifetime the response was stored with. `CachePolicy` only
    /// exposes the remaining time to live, which saturates at zero once the entry
    /// is stale, so it is recovered from the age at the time the entry was stored.
    fn freshness_lifetime(&self) -> Duration {
        self.policy.time_to_live(self.response_time) + self.policy.age(self.response_time)
    }

    /// `stored_headers` are the stored response's, whose `Vary` has to be applied
    /// to the presented request in the same way it was applied when the entry was
    /// written. See [`PolicyRequest::new`].
    pub(crate) fn evaluate(
        &self,
        request: &Request,
        stored_headers: &HeaderMap,
        now: SystemTime,
    ) -> Freshness {
        match self
            .policy
            .before_request(&PolicyRequest::new(request, stored_headers), now)
        {
            BeforeRequest::Fresh(_) => Freshness::Fresh,
            BeforeRequest::Stale { matches: false, .. } => Freshness::NoMatch,
            BeforeRequest::Stale { request, .. } => Freshness::Stale {
                stale_for: self
                    .policy
                    .age(now)
                    .saturating_sub(self.freshness_lifetime()),
                revalidation: Box::new(request),
            },
        }
    }

    /// Merge a revalidation response into this policy.
    /// <https://httpwg.org/specs/rfc9111.html#freshening.responses>
    ///
    /// Returns the refreshed policy and the merged response headers when the
    /// stored body may still be used, and `None` when it may not.
    pub(crate) fn refresh(
        &self,
        request: &Request,
        stored_status: StatusCode,
        status: StatusCode,
        headers: &HeaderMap,
        stored_headers: &HeaderMap,
        now: SystemTime,
    ) -> Option<(EntryPolicy, HeaderMap)> {
        let headers = validator_for_revalidation(status, headers, stored_headers);
        // A 304 may introduce or change `Vary`, so the request is redacted against
        // both header sets: a field either names credentials, and they stay
        // comparable, or it does not, and they are dropped.
        let mut vary_source = stored_headers.clone();
        for value in headers.get_all(header::VARY) {
            vary_source.append(header::VARY, value.clone());
        }
        let after = self.policy.after_response(
            &PolicyRequest::new(request, &vary_source),
            &PolicyResponse::new(status, &headers),
            now,
        );
        let AfterResponse::NotModified(_, parts) = after else {
            return None;
        };
        let merged = apply_revalidation_headers(parts.headers, &headers);
        // The crate's own merged policy is discarded: its merge walks only the
        // stored fields, so a field the 304 introduces -- a `Cache-Control` the
        // stored response did not have, say -- would be served but not obeyed. The
        // policy is rebuilt from the merged headers instead, which is what the
        // entry now is. `Age` and `Date` come from the 304 or from nowhere, since
        // the entry has just been validated and the stored values describe when it
        // was first received.
        let mut policy_headers = merged.clone();
        for name in [header::AGE, header::DATE] {
            match headers.get(&name) {
                Some(value) => policy_headers.insert(name, value.clone()),
                None => policy_headers.remove(&name),
            };
        }
        let policy = CachePolicy::new_options(
            &PolicyRequest::new(request, &policy_headers),
            &PolicyResponse::new(
                policy_status(stored_status, &policy_headers),
                &policy_headers,
            ),
            now,
            CACHE_OPTIONS,
        );
        Some((
            EntryPolicy {
                policy,
                response_time: now,
                stale_while_revalidate: stale_while_revalidate_window(&merged),
            },
            merged,
        ))
    }
}

/// Header fields a 304 must not change, because the stored body is reused.
/// Same list as `http-cache-semantics` uses for the fields it replaces.
const NOT_UPDATED_BY_REVALIDATION: &[&str] = &[
    "content-length",
    "content-encoding",
    "transfer-encoding",
    "content-range",
    // Presentation values the cache computes for itself.
    "age",
    "date",
];

/// Apply a 304's header fields to the merged response.
///
/// `http-cache-semantics` walks only the *stored* header fields and takes a single
/// value per field, so a field the 304 introduces is dropped and a field the 304
/// repeats keeps only its first value. RFC 9111 section 4.3.4 updates the stored
/// header fields with those from the 304, so each field the 304 carries replaces
/// the stored one outright, with all of its values.
fn apply_revalidation_headers(mut merged: HeaderMap, from_304: &HeaderMap) -> HeaderMap {
    for name in from_304.keys() {
        if NOT_UPDATED_BY_REVALIDATION.contains(&name.as_str()) ||
            HOP_BY_HOP_HEADERS.contains(&name.as_str())
        {
            continue;
        }
        merged.remove(name);
        for value in from_304.get_all(name) {
            merged.append(name.clone(), value.clone());
        }
    }
    strip_unstorable_fields(&mut merged);
    merged
}

/// Response header fields that are never written to a cache entry.
///
/// A stored `Set-Cookie` would put the cookie on disk for as long as the entry
/// lives, and nothing reads it back: cookies are applied on the network path
/// only, so a cache hit never re-applies one. Chromium strips it for the same
/// reason.
const NOT_STORED: &[&str] = &["set-cookie", "set-cookie2"];

/// Remove the fields a cache entry must not carry. Applied to what is stored and
/// therefore also to what a hit is served with.
pub(crate) fn strip_unstorable_fields(headers: &mut HeaderMap) {
    for name in NOT_STORED {
        while headers.remove(*name).is_some() {}
    }
}

/// A 304 answers the conditional request that this very entry produced, so it
/// refers to this entry even when the server echoed no validator back. RFC 9111
/// section 4.3.4 — which `http-cache-semantics` implements literally — would
/// refuse to freshen in that case; Chromium and Firefox do freshen, and so do we.
/// The stored validator is copied in so that the crate's own 304 merge applies.
fn validator_for_revalidation(
    status: StatusCode,
    headers: &HeaderMap,
    stored_headers: &HeaderMap,
) -> HeaderMap {
    if status != StatusCode::NOT_MODIFIED ||
        headers.contains_key(header::ETAG) ||
        headers.contains_key(header::LAST_MODIFIED)
    {
        return headers.clone();
    }
    let mut headers = headers.clone();
    for name in [header::ETAG, header::LAST_MODIFIED] {
        if let Some(value) = stored_headers.get(&name) {
            headers.insert(name, value.clone());
        }
    }
    headers
}

/// The headers a cached response is served with: the stored response headers
/// without hop-by-hop headers, with `Age` and `Date` brought up to date.
/// <https://httpwg.org/specs/rfc9111.html#constructing.responses.from.caches>
pub(crate) fn presented_headers(
    stored: &HeaderMap,
    policy: &EntryPolicy,
    now: SystemTime,
) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(stored.len());
    let connection_options: Vec<String> = stored
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|option| option.trim().to_ascii_lowercase())
        .collect();
    for (name, value) in stored.iter() {
        let lowercase = name.as_str();
        if HOP_BY_HOP_HEADERS.contains(&lowercase) ||
            connection_options.iter().any(|option| option == lowercase)
        {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }

    if let Ok(age) = HeaderValue::from_str(&policy.age(now).as_secs().to_string()) {
        headers.insert(header::AGE, age);
    }
    if let Ok(date) = httpdate_value(now) {
        headers.insert(header::DATE, date);
    }
    headers
}

fn httpdate_value(time: SystemTime) -> Result<HeaderValue, ()> {
    let date = headers::Date::from(time);
    let mut values = Vec::new();
    headers::Header::encode(&date, &mut values);
    values.into_iter().next().ok_or(())
}

/// The `headers` crate's `CacheControl` does not understand the
/// `stale-while-revalidate` directive, so the raw values are parsed here.
/// <https://datatracker.ietf.org/doc/html/rfc5861#section-3>
fn stale_while_revalidate_window(headers: &HeaderMap) -> Duration {
    for value in headers.get_all(header::CACHE_CONTROL) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for directive in value.split(',') {
            let Some((name, argument)) = directive.trim().split_once('=') else {
                continue;
            };
            if !name.trim().eq_ignore_ascii_case("stale-while-revalidate") {
                continue;
            }
            if let Ok(seconds) = argument.trim().trim_matches('"').parse::<u64>() {
                return Duration::from_secs(seconds);
            }
        }
    }
    Duration::ZERO
}

/// Whether the request itself demands revalidation, which suppresses serving a
/// stale response from the `stale-while-revalidate` window.
/// <https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2.1>
pub(crate) fn request_demands_revalidation(request: &Request) -> bool {
    use headers::HeaderMapExt;
    use net_traits::request::CacheMode;

    if matches!(
        request.cache_mode,
        CacheMode::NoCache | CacheMode::Reload | CacheMode::NoStore
    ) {
        return true;
    }
    let Some(directive) = request.headers.typed_get::<headers::CacheControl>() else {
        return false;
    };
    // The request's `no-store` directive is deliberately *not* treated as demanding
    // revalidation: <https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2.1.5>
    // says it "does not apply to the already stored response".
    directive.no_cache() || directive.max_age() == Some(Duration::ZERO)
}
