/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use dom_struct::dom_struct;
use js::context::JSContext;
use script_bindings::domstring::TracedDOMString;
use script_bindings::reflector::{Reflector, reflect_dom_object};

use crate::dom::bindings::codegen::Bindings::DOMStringListBinding::DOMStringListMethods;
use crate::dom::bindings::root::DomRoot;
use crate::dom::bindings::str::DOMString;
use crate::dom::globalscope::GlobalScope;

#[dom_struct]
pub(crate) struct DOMStringList {
    reflector_: Reflector,
    strings: Vec<TracedDOMString>,
}

impl DOMStringList {
    pub(crate) fn new_inherited(strings: Vec<DOMString>) -> DOMStringList {
        DOMStringList {
            reflector_: Reflector::new(),
            strings: strings
                .into_iter()
                .map(|domstring| domstring.traced())
                .collect(),
        }
    }

    pub(crate) fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        strings: Vec<DOMString>,
    ) -> DomRoot<DOMStringList> {
        reflect_dom_object(cx, Box::new(DOMStringList::new_inherited(strings)), global)
    }
}

// https://html.spec.whatwg.org/multipage/#domstringlist
impl DOMStringListMethods<crate::DomTypeHolder> for DOMStringList {
    /// <https://html.spec.whatwg.org/multipage/#dom-domstringlist-length>
    fn Length(&self) -> u32 {
        self.strings.len() as u32
    }

    /// <https://html.spec.whatwg.org/multipage/#dom-domstringlist-item>
    fn Item(&self, index: u32) -> Option<DOMString> {
        self.strings
            .get(index as usize)
            .cloned()
            .map(|domstring| domstring.root())
    }

    /// <https://html.spec.whatwg.org/multipage/#dom-domstringlist-contains>
    fn Contains(&self, string: DOMString) -> bool {
        self.strings.contains(&string)
    }

    // check-tidy: no specs after this line
    fn IndexedGetter(&self, index: u32) -> Option<DOMString> {
        self.Item(index)
    }
}
