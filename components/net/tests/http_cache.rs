/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::sync::Arc;

use http::header::{
    CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, EXPIRES, HeaderValue,
    RANGE, SET_COOKIE, VARY,
};
use http::{HeaderMap, StatusCode};
use net::http_cache::memory_store::MemoryStore;
use net::http_cache::{HttpCache, ValidationStatus};
use net_traits::blob_url_store::UrlWithBlobClaim;
use net_traits::request::{Referrer, Request, RequestBuilder};
use net_traits::response::Response;
use net_traits::{ResourceFetchTiming, ResourceTimingType};
use servo_base::id::TEST_PIPELINE_ID;
use servo_url::ServoUrl;

const URL: &str = "https://servo.org/";

fn test_cache() -> HttpCache {
    HttpCache::with_store(Arc::new(MemoryStore::new(16 * 1024 * 1024)))
}

fn request_with_headers(headers: HeaderMap) -> Request {
    let url = ServoUrl::parse(URL).unwrap();
    RequestBuilder::new(
        None,
        UrlWithBlobClaim::new(url.clone(), None),
        Referrer::NoReferrer,
    )
    .pipeline_id(Some(TEST_PIPELINE_ID))
    .origin(url.origin())
    .headers(headers)
    .build()
}

fn request() -> Request {
    request_with_headers(HeaderMap::new())
}

fn response_with_headers(headers: &[(http::HeaderName, &str)]) -> Response {
    let url = ServoUrl::parse(URL).unwrap();
    let timing = ResourceFetchTiming::new(ResourceTimingType::Navigation);
    let mut response = Response::new(url, timing);
    response.status = StatusCode::OK.into();
    for (name, value) in headers {
        response
            .headers
            .insert(name.clone(), HeaderValue::from_str(value).unwrap());
    }
    response
}

#[tokio::test]
async fn test_fresh_response_is_valid() {
    let cache = test_cache();
    let request = request();
    let response = response_with_headers(&[(CACHE_CONTROL, "max-age=1000")]);
    assert!(cache.store_for_test(&request, &response, b"body").await);

    assert_eq!(cache.probe(&request).await, Some(ValidationStatus::Valid));
    assert_eq!(
        cache.read_body_for_test(&request).await.as_deref(),
        Some(&b"body"[..])
    );
}

#[tokio::test]
async fn test_no_store_response_is_not_stored() {
    let cache = test_cache();
    let request = request();
    let response = response_with_headers(&[(CACHE_CONTROL, "no-store")]);
    assert!(!cache.store_for_test(&request, &response, b"body").await);
    assert_eq!(cache.probe(&request).await, None);
}

fn partial_response() -> Response {
    let mut response = response_with_headers(&[
        (CACHE_CONTROL, "max-age=1000"),
        (CONTENT_RANGE, "bytes 0-4/10"),
        (CONTENT_LENGTH, "5"),
    ]);
    response.status = StatusCode::PARTIAL_CONTENT.into();
    response
}

#[tokio::test]
async fn test_partial_content_never_answers_a_request_for_the_whole_resource() {
    let cache = test_cache();
    let request = request();
    assert!(
        cache
            .store_for_test(&request, &partial_response(), b"12345")
            .await
    );
    assert_eq!(
        cache.probe(&request).await,
        None,
        "a stored 206 must not answer a request that asks for the whole resource"
    );
}

#[tokio::test]
async fn test_partial_content_serves_a_range_it_covers() {
    let cache = test_cache();
    assert!(
        cache
            .store_for_test(&request(), &partial_response(), b"12345")
            .await
    );
    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=1-3"));
    assert_eq!(
        cache.probe(&request_with_headers(headers)).await,
        Some(ValidationStatus::Valid)
    );
}

#[tokio::test]
async fn test_skip_incomplete_cache_for_range_request_with_no_end_bound() {
    // The stored 206 holds bytes 0 to 4 of a 10-byte resource, so it cannot answer
    // a request for everything from byte 0 onwards.
    let cache = test_cache();
    assert!(
        cache
            .store_for_test(&request(), &partial_response(), b"12345")
            .await
    );

    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=0-"));
    assert_eq!(cache.probe(&request_with_headers(headers)).await, None);
}

#[tokio::test]
async fn test_partial_content_does_not_serve_a_range_beyond_what_it_holds() {
    // The `Content-Range` claims five bytes, the body is longer: only what the
    // claim covers may be served, whatever the stored body's length says.
    let cache = test_cache();
    assert!(
        cache
            .store_for_test(&request(), &partial_response(), b"12345678901234567890")
            .await
    );

    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=0-9"));
    assert_eq!(cache.probe(&request_with_headers(headers)).await, None);
}

#[tokio::test]
async fn test_partial_content_does_not_answer_a_suffix_range_it_does_not_hold() {
    // A partial that claims to reach the end of the resource but holds less of it
    // must not answer a suffix range with the wrong bytes.
    let cache = test_cache();
    let mut response = response_with_headers(&[
        (CACHE_CONTROL, "max-age=1000"),
        (CONTENT_RANGE, "bytes 0-999/1000"),
    ]);
    response.status = StatusCode::PARTIAL_CONTENT.into();
    assert!(cache.store_for_test(&request(), &response, b"12345").await);

    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=-100"));
    assert_eq!(
        cache.probe(&request_with_headers(headers)).await,
        None,
        "the last 100 bytes of the resource are not among the 5 that are stored"
    );
}

#[tokio::test]
async fn test_range_is_ignored_when_if_range_does_not_match() {
    let cache = test_cache();
    let response = response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (ETAG, "\"a\"")]);
    assert!(cache.store_for_test(&request(), &response, b"body").await);

    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=0-1"));
    headers.insert("if-range", HeaderValue::from_static("\"b\""));
    // The condition fails, so the request is for the whole resource, which this
    // entry does hold.
    assert_eq!(
        cache.probe(&request_with_headers(headers)).await,
        Some(ValidationStatus::Valid)
    );
}

#[tokio::test]
async fn test_multiple_ranges_are_not_served_from_the_cache() {
    let cache = test_cache();
    let response = response_with_headers(&[(CACHE_CONTROL, "max-age=1000")]);
    assert!(
        cache
            .store_for_test(&request(), &response, b"0123456789")
            .await
    );

    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=0-1,4-5"));
    assert_eq!(
        cache.probe(&request_with_headers(headers)).await,
        None,
        "a multipart answer is not something the cache builds"
    );
}

#[tokio::test]
async fn test_vary_star_is_not_stored() {
    let cache = test_cache();
    let response = response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (VARY, "*")]);
    assert!(
        !cache.store_for_test(&request(), &response, b"body").await,
        "a `Vary: *` entry can never match a later request"
    );
}

#[tokio::test]
async fn test_set_cookie_is_not_kept_by_the_cache() {
    let cache = test_cache();
    let response =
        response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (SET_COOKIE, "sid=secret")]);
    assert!(cache.store_for_test(&request(), &response, b"body").await);
    assert_eq!(cache.probe(&request()).await, Some(ValidationStatus::Valid));
    assert!(
        !cache
            .headers_for_test(&request())
            .await
            .unwrap()
            .contains_key(SET_COOKIE),
        "a stored `Set-Cookie` would put the cookie on disk and be re-presented on every hit"
    );
}

#[tokio::test]
async fn test_vary_on_cookie_still_selects_by_cookie() {
    // Credentials are digested rather than stored, so the entry has to keep
    // matching the cookie it was stored for, and only that one.
    let cache = test_cache();
    let mut stored = HeaderMap::new();
    stored.insert("cookie", HeaderValue::from_static("sid=one"));
    let stored_request = request_with_headers(stored);
    let response = response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (VARY, "cookie")]);
    assert!(
        cache
            .store_for_test(&stored_request, &response, b"body")
            .await
    );

    assert_eq!(
        cache.probe(&stored_request).await,
        Some(ValidationStatus::Valid)
    );

    let mut other = HeaderMap::new();
    other.insert("cookie", HeaderValue::from_static("sid=two"));
    assert_eq!(
        cache.probe(&request_with_headers(other)).await,
        None,
        "another session's cookie must not be answered from this entry"
    );
}

#[tokio::test]
async fn test_vary_selects_the_matching_variant() {
    let cache = test_cache();

    let mut gzip_headers = HeaderMap::new();
    gzip_headers.insert("accept-encoding", HeaderValue::from_static("gzip"));
    let gzip_request = request_with_headers(gzip_headers);
    let response =
        response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (VARY, "accept-encoding")]);
    assert!(
        cache
            .store_for_test(&gzip_request, &response, b"gzip")
            .await
    );

    assert_eq!(
        cache.probe(&gzip_request).await,
        Some(ValidationStatus::Valid)
    );
    let mut br_headers = HeaderMap::new();
    br_headers.insert("accept-encoding", HeaderValue::from_static("br"));
    assert_eq!(
        cache.probe(&request_with_headers(br_headers)).await,
        None,
        "a request whose Vary-nominated headers differ must not match the stored variant"
    );
}

#[tokio::test]
async fn test_expired_response_needs_synchronous_validation() {
    let cache = test_cache();
    let request = request();
    let response = response_with_headers(&[(EXPIRES, "0"), (ETAG, "\"v1\"")]);
    assert!(cache.store_for_test(&request, &response, b"body").await);
    assert_eq!(
        cache.probe(&request).await,
        Some(ValidationStatus::Stale {
            revalidate_in_background: false
        })
    );
}

#[tokio::test]
async fn test_body_is_stored_encoded() {
    let cache = test_cache();
    let request = request();
    let response =
        response_with_headers(&[(CACHE_CONTROL, "max-age=1000"), (CONTENT_ENCODING, "gzip")]);
    let compressed = b"\x1f\x8b not really gzip";
    assert!(cache.store_for_test(&request, &response, compressed).await);
    assert_eq!(
        cache.read_body_for_test(&request).await.as_deref(),
        Some(&compressed[..]),
        "the cache must store the body exactly as it came off the wire"
    );
}

/// Store a response with the given `Cache-Control`, then report the freshness the
/// cache assigns it on a subsequent request.
async fn stale_while_revalidate_status_for(cache_control: &str) -> ValidationStatus {
    let cache = test_cache();
    let request = request();
    let response = response_with_headers(&[(CACHE_CONTROL, cache_control)]);
    assert!(cache.store_for_test(&request, &response, &[1, 2, 3]).await);
    cache
        .probe(&request)
        .await
        .expect("the stored response should be found")
}

#[tokio::test]
async fn test_stale_within_stale_while_revalidate_window_serves_immediately_and_revalidates_in_background()
 {
    assert_eq!(
        stale_while_revalidate_status_for("max-age=0, stale-while-revalidate=30").await,
        ValidationStatus::Stale {
            revalidate_in_background: true
        },
        "stale response within the stale-while-revalidate window should be served immediately \
         and revalidated in the background"
    );
}

#[tokio::test]
async fn test_stale_without_stale_while_revalidate_requires_synchronous_validation() {
    assert_eq!(
        stale_while_revalidate_status_for("max-age=0").await,
        ValidationStatus::Stale {
            revalidate_in_background: false
        },
        "stale response without stale-while-revalidate must be synchronously revalidated"
    );
}

#[tokio::test]
async fn test_stale_while_revalidate_not_used_when_request_demands_revalidation() {
    let cache = test_cache();
    let response =
        response_with_headers(&[(CACHE_CONTROL, "max-age=0, stale-while-revalidate=30")]);
    assert!(
        cache
            .store_for_test(&request(), &response, &[1, 2, 3])
            .await
    );

    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    assert_eq!(
        cache.probe(&request_with_headers(headers)).await,
        Some(ValidationStatus::Stale {
            revalidate_in_background: false
        }),
        "a no-cache request must trigger synchronous validation, not background revalidation"
    );
}
