/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Coordination between concurrent fetches of the same resource.
//!
//! Exactly one fetch per key writes to the store; the others wait for it to
//! resolve and then read the committed entry, or go to the network if it aborted.

use std::sync::{Arc, Mutex};

use log::debug;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::watch;

use crate::http_cache::key::{CacheKey, EntryId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InFlightState {
    /// A fetch is writing this entry.
    Writing,
    /// The entry was committed and can be read.
    Committed(EntryId),
    /// The entry will not appear: the response was not storable, the fetch
    /// failed, or it was cancelled.
    Aborted,
}

/// The set of keys currently being written.
#[derive(Default)]
pub(crate) struct InFlight {
    writers: Mutex<FxHashMap<CacheKey, watch::Sender<InFlightState>>>,
}

impl InFlight {
    /// Become the writer for `key`, or get a receiver to wait on the fetch that
    /// already is.
    pub(crate) fn register(
        self: &Arc<Self>,
        key: &CacheKey,
    ) -> Result<InFlightWriter, watch::Receiver<InFlightState>> {
        let mut writers = self.writers.lock().unwrap();
        if let Some(sender) = writers.get(key) {
            return Err(sender.subscribe());
        }
        let (sender, _) = watch::channel(InFlightState::Writing);
        writers.insert(key.clone(), sender);
        Ok(InFlightWriter {
            inflight: self.clone(),
            key: key.clone(),
            resolved: false,
        })
    }

    fn resolve(&self, key: &CacheKey, state: InFlightState) {
        let sender = self.writers.lock().unwrap().remove(key);
        if let Some(sender) = sender {
            // Receivers keep the last value readable after the sender is dropped.
            let _ = sender.send(state);
        }
    }
}

/// Wait for the fetch that owns a key to resolve it.
pub(crate) async fn wait(mut receiver: watch::Receiver<InFlightState>) -> InFlightState {
    loop {
        let state = *receiver.borrow_and_update();
        if state != InFlightState::Writing {
            return state;
        }
        if receiver.changed().await.is_err() {
            // The writer was dropped without resolving, which its own `Drop` should
            // have prevented. Treat it as an abort rather than waiting forever.
            let state = *receiver.borrow();
            return match state {
                InFlightState::Writing => InFlightState::Aborted,
                state => state,
            };
        }
    }
}

/// Held by the one fetch that may write a given key. Resolves the key on drop, so
/// a panicking or cancelled fetch cannot leave waiters stuck.
pub(crate) struct InFlightWriter {
    inflight: Arc<InFlight>,
    key: CacheKey,
    resolved: bool,
}

impl InFlightWriter {
    pub(crate) fn commit(mut self, id: EntryId) {
        self.resolved = true;
        self.inflight
            .resolve(&self.key, InFlightState::Committed(id));
    }

    pub(crate) fn abort(mut self) {
        self.resolved = true;
        self.inflight.resolve(&self.key, InFlightState::Aborted);
    }
}

impl Drop for InFlightWriter {
    fn drop(&mut self) {
        if !self.resolved {
            debug!("in-flight cache entry for {} dropped", self.key.url());
            self.inflight.resolve(&self.key, InFlightState::Aborted);
        }
    }
}

/// Single-flight guard for `stale-while-revalidate` background refreshes, so a
/// stale entry served to many consumers is only revalidated once.
#[derive(Default)]
pub struct RevalidationGuards {
    keys: Mutex<FxHashSet<CacheKey>>,
}

impl RevalidationGuards {
    pub(crate) fn try_acquire(self: &Arc<Self>, key: &CacheKey) -> Option<RevalidationGuard> {
        if !self.keys.lock().unwrap().insert(key.clone()) {
            return None;
        }
        Some(RevalidationGuard {
            guards: self.clone(),
            key: key.clone(),
        })
    }
}

/// Held for as long as a background revalidation of one key is in flight.
pub struct RevalidationGuard {
    guards: Arc<RevalidationGuards>,
    key: CacheKey,
}

impl Drop for RevalidationGuard {
    fn drop(&mut self) {
        self.guards.keys.lock().unwrap().remove(&self.key);
    }
}
