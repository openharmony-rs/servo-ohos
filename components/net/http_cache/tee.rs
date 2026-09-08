/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Copying a network body into the cache while it is delivered.

use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::body::{Body, Frame, SizeHint};
use log::debug;

use crate::connector::BoxedBody;
use crate::http_cache::inflight::InFlightWriter;
use crate::http_cache::store::EntryWriter;

/// Wraps a network body and copies every frame into an [`EntryWriter`] on its way
/// through. The copy is a refcount clone of the `Bytes`, so the bytes are not
/// duplicated, and the writer's bounded queue back-pressures the network.
///
/// This sits *below* the content decoder, so what reaches the cache is the body
/// exactly as it came off the wire, with its `Content-Encoding` intact.
pub(crate) struct TeeBody {
    inner: BoxedBody,
    writer: Option<EntryWriter>,
    inflight: Option<InFlightWriter>,
}

impl TeeBody {
    pub(crate) fn new(inner: BoxedBody, writer: EntryWriter, inflight: InFlightWriter) -> Self {
        Self {
            inner,
            writer: Some(writer),
            inflight: Some(inflight),
        }
    }

    /// The body ended cleanly: publish the entry once the store has it all.
    fn finish(&mut self) {
        let (Some(writer), Some(inflight)) = (self.writer.take(), self.inflight.take()) else {
            return;
        };
        tokio::spawn(async move {
            match writer.commit().await {
                Ok(id) => inflight.commit(id),
                Err(error) => {
                    debug!("cache entry was not committed: {error}");
                    inflight.abort();
                },
            }
        });
    }

    /// The body failed or was cancelled: drop the partial entry. Dropping the
    /// writer aborts it, and dropping the in-flight handle releases the waiters.
    fn abandon(&mut self) {
        self.writer = None;
        self.inflight = None;
    }
}

impl Body for TeeBody {
    type Data = <BoxedBody as Body>::Data;
    type Error = <BoxedBody as Body>::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        // Do not pull from the network until the store can take another chunk.
        if let Some(writer) = this.writer.as_mut() &&
            writer.poll_ready(cx).is_pending()
        {
            return Poll::Pending;
        }

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(writer) = this.writer.as_mut() &&
                    let Some(data) = frame.data_ref()
                {
                    writer.push(data.clone());
                }
                Poll::Ready(Some(Ok(frame)))
            },
            Poll::Ready(Some(Err(error))) => {
                this.abandon();
                Poll::Ready(Some(Err(error)))
            },
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
