/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![deny(unsafe_code)]

mod font_descriptor;
mod font_identifier;
mod font_template;
mod system_font_service_proxy;

use std::ops::{Deref, Range};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub use font_descriptor::*;
pub use font_identifier::*;
pub use font_template::*;
use malloc_size_of_derive::MallocSizeOf;
use num_derive::{NumOps, One, Zero};
use serde::{Deserialize, Serialize};
use servo_arc::Arc as ServoArc;
use servo_base::generic_channel::GenericSharedMemory;
use style::font_face::Descriptors;
use style::stylesheets::LockedFontFaceRule;
pub use system_font_service_proxy::*;
use webrender_api::euclid::num::One;

/// An index that refers to a byte offset in a text run. This could
/// the middle of a glyph.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    MallocSizeOf,
    NumOps,
    Ord,
    One,
    PartialEq,
    PartialOrd,
    Serialize,
    Zero,
)]
pub struct ByteIndex(pub usize);

impl ByteIndex {
    pub fn get(&self) -> usize {
        self.0
    }
}

/// A range of UTF-8 bytes in a text run. This is used to identify glyphs in a `GlyphRun`
/// by their original character byte offsets in the text.
#[derive(Clone, Debug, Default, Deserialize, MallocSizeOf, PartialEq, Serialize)]
pub struct TextByteRange(Range<ByteIndex>);

impl TextByteRange {
    pub fn len(&self) -> ByteIndex {
        self.0.end - self.0.start
    }

    #[inline]
    pub fn intersect(&self, other: &Self) -> Self {
        let begin = self.start.max(other.start);
        let end = self.end.min(other.end);

        if end < begin {
            Self::default()
        } else {
            Self::new(begin, end)
        }
    }

    #[inline]
    pub fn contains_inclusive(&self, index: ByteIndex) -> bool {
        index >= self.start && index <= self.end
    }
}

impl Deref for TextByteRange {
    type Target = Range<ByteIndex>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Iterator for TextByteRange {
    type Item = ByteIndex;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.start == self.0.end {
            None
        } else {
            let next = self.0.start;
            self.0.start = self.0.start + ByteIndex::one();
            Some(next)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.0.end.0 - self.0.start.0,
            Some(self.0.end.0 - self.0.start.0),
        )
    }
}

impl DoubleEndedIterator for TextByteRange {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.0.start == self.0.end {
            None
        } else {
            self.0.end = self.0.end - ByteIndex::one();
            Some(self.0.end)
        }
    }
}

impl TextByteRange {
    pub fn new(start: ByteIndex, end: ByteIndex) -> Self {
        Self(start..end)
    }

    pub fn iter(&self) -> Range<ByteIndex> {
        self.0.clone()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WebFontLoadEvent {
    LoadedSuccessfully {
        /// Whether laid-out text may use the font, because font matching has looked up
        /// its family before. If not, nothing needs to be laid out again.
        affects_laid_out_text: bool,
    },
    UnblockedFontReadyPromise,
}

pub type StylesheetWebFontLoadFinishedCallback =
    Arc<dyn Fn(WebFontLoadEvent) + Send + Sync + 'static>;

/// A data structure to store data for fonts. Data is stored internally in an
/// [`GenericSharedMemory`] handle, so that it can be sent without serialization
/// across IPC channels.
#[derive(Clone, Deserialize, MallocSizeOf, Serialize)]
pub struct FontData(#[conditional_malloc_size_of] pub(crate) Arc<GenericSharedMemory>);

impl FontData {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Arc::new(GenericSharedMemory::from_bytes(bytes)))
    }

    pub fn as_ipc_shared_memory(&self) -> Arc<GenericSharedMemory> {
        self.0.clone()
    }
}

impl AsRef<[u8]> for FontData {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Raw font data and an index
///
/// If the font data is of a TTC (TrueType collection) file, then the index of a specific font within
/// the collection. If the font data is for is single font then the index will always be 0.
#[derive(Deserialize, Clone, Serialize)]
pub struct FontDataAndIndex {
    /// The raw font file data (.ttf, .otf, .ttc, etc)
    pub data: FontData,
    /// The index of the font within the file (0 if the file is not a ttc)
    pub index: u32,
}

#[derive(Copy, Clone, PartialEq)]
pub enum FontDataError {
    FailedToLoad,
}

/// Describes how the set of active `@font-face` rules was changed after a call to `FontContext::rebuild_font_face_set`.
#[derive(Clone, Default)]
pub struct WebFontSetDifference {
    /// A list of `@font-face` rules that were added in this update.
    pub added_font_faces: Vec<ServoArc<FontFaceRuleInfo>>,
    /// A list of `@font-face` rules that were removed in this update.
    pub removed_font_faces: Vec<ServoArc<FontFaceRuleInfo>>,
    /// Whether the cascade index of any `@font-face` rule changed during this update.
    ///
    /// This can cause different fonts to be selected during font matching.
    pub cascade_index_of_any_rule_changed: bool,
    /// Whether this update can change the fonts of text that has already been laid out,
    /// because font matching has looked up the family of a rule that changed.
    pub affects_laid_out_text: bool,
}

impl WebFontSetDifference {
    /// Returns `true` iff the font face set remained unchanged by the update.
    pub fn is_empty(&self) -> bool {
        self.added_font_faces.is_empty() && self.removed_font_faces.is_empty()
    }
}

/// How far the font described by an `@font-face` rule has got towards being usable.
///
/// A rule starts out [`WebFontLoadState::Unloaded`]: its `src` is only fetched once font
/// matching picks the face for a character, as required by
/// <https://drafts.csswg.org/css-fonts-4/#font-face-loading>.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebFontLoadState {
    /// Nothing has asked for this face yet, so it has not been fetched.
    Unloaded,
    /// The face is being fetched, or is waiting to be.
    Loading,
    /// The face has a usable [`FontTemplate`].
    Loaded,
    /// None of the sources of the face could be used.
    Failed,
}

impl From<usize> for WebFontLoadState {
    fn from(value: usize) -> Self {
        match value {
            0 => Self::Unloaded,
            1 => Self::Loading,
            2 => Self::Loaded,
            _ => Self::Failed,
        }
    }
}

#[derive(MallocSizeOf)]
pub struct FontFaceRuleInfo {
    /// The index of this `@font-face` in the cascade, relative to all
    /// other `@font-face` rules.
    pub cascade_index: AtomicUsize,
    /// The descriptors on the `@font-face` rule.
    pub descriptors: Descriptors,
    /// The CSS rule that created this `@font-face`.
    ///
    /// This does *not* uniquely identify this struct across updates
    /// to the set of live `@font-face` rules.
    #[conditional_malloc_size_of]
    pub rule: ServoArc<LockedFontFaceRule>,
    /// The [`WebFontLoadState`] of this rule, shared with the `FontFace` object that
    /// exposes it to script.
    load_state: AtomicUsize,
}

impl FontFaceRuleInfo {
    pub fn new(
        cascade_index: usize,
        descriptors: Descriptors,
        rule: ServoArc<LockedFontFaceRule>,
    ) -> Self {
        Self {
            cascade_index: AtomicUsize::new(cascade_index),
            descriptors,
            rule,
            load_state: AtomicUsize::new(WebFontLoadState::Unloaded as usize),
        }
    }

    pub fn load_state(&self) -> WebFontLoadState {
        self.load_state.load(Ordering::SeqCst).into()
    }

    pub fn set_load_state(&self, state: WebFontLoadState) {
        self.load_state.store(state as usize, Ordering::SeqCst);
    }

    /// Move this rule from [`WebFontLoadState::Unloaded`] to [`WebFontLoadState::Loading`],
    /// returning true if this call was the one that started the load. Font matching runs on
    /// several threads at once, so only one of them may request the load.
    pub fn start_loading(&self) -> bool {
        self.load_state
            .compare_exchange(
                WebFontLoadState::Unloaded as usize,
                WebFontLoadState::Loading as usize,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }
}
