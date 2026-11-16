/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use dom_struct::dom_struct;
use js::context::JSContext;
use js::jsapi::Heap;
use js::jsval::JSVal;
use js::rust::{HandleValue, MutableHandleValue};
use script_bindings::reflector::{Reflector, reflect_dom_object};
use storage_traits::indexeddb::IndexedDBKeyType;

use crate::dom::bindings::codegen::Bindings::IDBRecordBinding::IDBRecordMethods;
use crate::dom::bindings::root::DomRoot;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::key::key_type_to_jsval;

/// <https://w3c.github.io/IndexedDB/#record-interface>
#[dom_struct]
pub(crate) struct IDBRecord {
    reflector_: Reflector,
    /// <https://w3c.github.io/IndexedDB/#record-snapshot-key>
    #[no_trace]
    key: IndexedDBKeyType,
    /// <https://w3c.github.io/IndexedDB/#record-snapshot-primary-key>
    #[no_trace]
    primary_key: IndexedDBKeyType,
    /// The key converted to a value by the first `key` getter, undefined before.
    #[ignore_malloc_size_of = "mozjs"]
    key_value: Heap<JSVal>,
    /// The primary key converted to a value by the first `primaryKey` getter, undefined before.
    #[ignore_malloc_size_of = "mozjs"]
    primary_key_value: Heap<JSVal>,
    /// <https://w3c.github.io/IndexedDB/#record-snapshot-value>
    #[ignore_malloc_size_of = "mozjs"]
    value: Heap<JSVal>,
}

impl IDBRecord {
    fn new_inherited(key: IndexedDBKeyType, primary_key: IndexedDBKeyType) -> IDBRecord {
        IDBRecord {
            reflector_: Reflector::new(),
            key,
            primary_key,
            key_value: Heap::default(),
            primary_key_value: Heap::default(),
            value: Heap::default(),
        }
    }

    pub(crate) fn new(
        cx: &mut JSContext,
        global: &GlobalScope,
        key: IndexedDBKeyType,
        primary_key: IndexedDBKeyType,
        value: HandleValue,
    ) -> DomRoot<IDBRecord> {
        let record = reflect_dom_object(
            cx,
            Box::new(IDBRecord::new_inherited(key, primary_key)),
            global,
        );
        record.value.set(value.get());
        record
    }

    /// Sets `retval` to `key` converted to a value, which `cache` keeps for the next call, as a
    /// key never converts to undefined.
    fn converted_key(
        cx: &mut JSContext,
        key: &IndexedDBKeyType,
        cache: &Heap<JSVal>,
        mut retval: MutableHandleValue,
    ) {
        if cache.get().is_undefined() {
            key_type_to_jsval(cx, key, retval.reborrow());
            cache.set(retval.get());
        }
        retval.set(cache.get());
    }
}

impl IDBRecordMethods<crate::DomTypeHolder> for IDBRecord {
    /// <https://w3c.github.io/IndexedDB/#dom-idbrecord-key>
    fn Key(&self, cx: &mut JSContext, retval: MutableHandleValue) {
        // The key getter steps are to return the result of converting a key to a value with
        // this’s key.
        // NOTE: If key returns an object (e.g. a Date or Array), it returns the same object
        // instance every time it is inspected.
        Self::converted_key(cx, &self.key, &self.key_value, retval);
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbrecord-primarykey>
    fn PrimaryKey(&self, cx: &mut JSContext, retval: MutableHandleValue) {
        // The primaryKey getter steps are to return the result of converting a key to a value
        // with this’s primary key.
        Self::converted_key(cx, &self.primary_key, &self.primary_key_value, retval);
    }

    /// <https://w3c.github.io/IndexedDB/#dom-idbrecord-value>
    fn Value(&self, _cx: &mut JSContext, mut retval: MutableHandleValue) {
        // The value getter steps are to return this’s value.
        retval.set(self.value.get());
    }
}
