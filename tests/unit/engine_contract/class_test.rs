/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![allow(unsafe_op_in_unsafe_fn)]

//! Reserved-slot classes, class tracing and heap write barriers.

use std::cell::Cell;
use std::ffi::c_void;
use std::ptr;

use js::glue::{
    CallBigIntTracer, CallFunctionTracer, CallObjectTracer, CallScriptTracer, CallStringTracer,
    CallValueTracer, JS_GetReservedSlot,
};
use js::jsapi::{
    self, GCReason, Heap, JSCLASS_RESERVED_SLOTS_MASK, JSCLASS_RESERVED_SLOTS_SHIFT, JSClass,
    JSClassOps, JSObject, JSTracer,
};
use js::jsval::{NullValue, ObjectValue, PrivateValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{JS_GC, JS_NewObject, JS_NewStringCopyN};
use js::rust::{CompileOptionsWrapper, JSEngine, transform_str_to_source_text};

use crate::support::{Observation, Value, eval, record, with_global};

const SLOT: u32 = 0;

thread_local! {
    static TRACED: Cell<u32> = const { Cell::new(0) };
}

struct Held {
    object: Heap<*mut JSObject>,
    function: Heap<*mut jsapi::JSFunction>,
    string: Heap<*mut jsapi::JSString>,
    script: Heap<*mut jsapi::JSScript>,
    bigint: Heap<*mut jsapi::BigInt>,
    value: Heap<jsapi::Value>,
}

impl Held {
    fn new() -> Box<Self> {
        Box::new(Held {
            object: Heap::default(),
            function: Heap::default(),
            string: Heap::default(),
            script: Heap::default(),
            bigint: Heap::default(),
            value: Heap::default(),
        })
    }
}

unsafe extern "C" fn trace_object(trc: *mut JSTracer, obj: *mut JSObject) {
    let mut slot = UndefinedValue();
    JS_GetReservedSlot(obj, SLOT, &mut slot);
    let held = slot.to_private() as *mut Held;
    if held.is_null() {
        return;
    }
    let held = &*held;
    CallObjectTracer(trc, &held.object as *const _ as *mut _, c"object".as_ptr());
    CallFunctionTracer(
        trc,
        &held.function as *const _ as *mut _,
        c"function".as_ptr(),
    );
    CallStringTracer(trc, &held.string as *const _ as *mut _, c"string".as_ptr());
    CallScriptTracer(trc, &held.script as *const _ as *mut _, c"script".as_ptr());
    CallBigIntTracer(trc, &held.bigint as *const _ as *mut _, c"bigint".as_ptr());
    CallValueTracer(trc, &held.value as *const _ as *mut _, c"value".as_ptr());
    TRACED.with(|count| count.set(count.get() + 1));
}

static CLASS_OPS: JSClassOps = JSClassOps {
    addProperty: None,
    delProperty: None,
    enumerate: None,
    newEnumerate: None,
    resolve: None,
    mayResolve: None,
    finalize: None,
    call: None,
    construct: None,
    trace: Some(trace_object),
};

static CLASS: JSClass = JSClass {
    name: c"ContractReservedSlot".as_ptr(),
    flags: (1 & JSCLASS_RESERVED_SLOTS_MASK) << JSCLASS_RESERVED_SLOTS_SHIFT,
    cOps: &CLASS_OPS,
    spec: ptr::null(),
    ext: ptr::null(),
    oOps: ptr::null(),
};

fn reserved_slots_and_tracing(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let object = JS_NewObject(cx, &CLASS);
        assert!(!object.is_null());
        rooted!(&in(cx) let object = object);

        let held = Held::new();
        let inner = JS_NewObject(cx, ptr::null());
        assert!(!inner.is_null());
        held.object.set(inner);
        held.value.set(ObjectValue(inner));
        held.function.set(
            eval(cx, global, "(function traced() { return 41; })")
                .to_object()
                .cast(),
        );
        held.string.set(JS_NewStringCopyN(cx, c"slot".as_ptr(), 4));
        held.bigint.set(eval(cx, global, "42n").to_bigint());
        let options = CompileOptionsWrapper::new(cx, c"trace-script.js".to_owned(), 1);
        held.script.set(jsapi::Compile1(
            cx.raw_cx(),
            options.ptr,
            &mut transform_str_to_source_text("1 + 1"),
        ));
        assert!(!held.string.get().is_null());
        assert!(!held.bigint.get().is_null());
        assert!(!held.script.get().is_null());

        let raw = Box::into_raw(held);
        jsapi::JS_SetReservedSlot(object.get(), SLOT, &PrivateValue(raw.cast()));
        let mut slot = UndefinedValue();
        JS_GetReservedSlot(object.get(), SLOT, &mut slot);
        assert_eq!(slot.to_private(), raw.cast::<c_void>());

        // The rooted object keeps the class live; its trace hook must visit every heap edge.
        TRACED.with(|count| count.set(0));
        JS_GC(cx, GCReason::API);
        assert!(
            TRACED.with(Cell::get) > 0,
            "class trace hook did not run during JS_GC"
        );
        JS_GetReservedSlot(object.get(), SLOT, &mut slot);
        assert_eq!(slot.to_private(), raw.cast::<c_void>());

        jsapi::JS_SetReservedSlot(object.get(), SLOT, &NullValue());
        JS_GetReservedSlot(object.get(), SLOT, &mut slot);
        assert!(slot.is_null());
        drop(Box::from_raw(raw));
    });

    record(
        "class.reserved_slots_trace",
        &Observation::ok(Value::Boolean(true)),
        &[
            "CallFunctionTracer",
            "CallObjectTracer",
            "CallScriptTracer",
            "CallStringTracer",
            "CallValueTracer",
            "Compile1",
            "JS_GC",
            "JS_GetReservedSlot",
            "JS_NewObject",
            "JS_SetReservedSlot",
        ],
    );
}

fn write_barriers(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let object = JS_NewObject(cx, ptr::null());
        assert!(!object.is_null());
        let function = eval(cx, global, "(function () {})").to_object() as *mut jsapi::JSFunction;
        let string = JS_NewStringCopyN(cx, c"barrier".as_ptr(), 7);
        let bigint = eval(cx, global, "7n").to_bigint();
        let options = CompileOptionsWrapper::new(cx, c"barrier-script.js".to_owned(), 1);
        let script = jsapi::Compile1(
            cx.raw_cx(),
            options.ptr,
            &mut transform_str_to_source_text("7"),
        );
        assert!(!string.is_null() && !bigint.is_null() && !script.is_null());

        // Barriers must see a stable slot: a stack temporary is invalidated when the frame ends
        // and the store buffer's edge outlives it. `Heap::boxed` keeps each slot at a fixed
        // address, and `set` runs the matching barrier after writing it.
        let object_slot = Heap::boxed(object);
        object_slot.set(object);
        assert_eq!(object_slot.get(), object);
        let function_slot = Heap::boxed(function);
        function_slot.set(function);
        assert_eq!(function_slot.get(), function);
        let string_slot = Heap::boxed(string);
        string_slot.set(string);
        assert_eq!(string_slot.get(), string);
        let bigint_slot = Heap::boxed(bigint);
        bigint_slot.set(bigint);
        assert_eq!(bigint_slot.get(), bigint);
        let script_slot = Heap::boxed(script);
        script_slot.set(script);
        assert_eq!(script_slot.get(), script);

        let value_slot = Heap::boxed(ObjectValue(object));
        value_slot.set(ObjectValue(object));
        assert_eq!(value_slot.get(), ObjectValue(object));

        // Keep the slots rooted across a GC so the recorded edges are traced while alive. GC may
        // move the referents, so only check that each slot still holds a live value.
        jsapi::JS_GC(cx.raw_cx(), GCReason::API);
        assert!(!object_slot.get().is_null());
        assert!(!function_slot.get().is_null());
        assert!(!string_slot.get().is_null());
        assert!(!bigint_slot.get().is_null());
        assert!(!script_slot.get().is_null());
        assert!(!value_slot.get().is_undefined());
    });

    record(
        "class.heap_write_barriers",
        &Observation::ok(Value::Boolean(true)),
        &[
            "Compile1",
            "HeapBigIntWriteBarriers",
            "HeapObjectWriteBarriers",
            "HeapScriptWriteBarriers",
            "HeapStringWriteBarriers",
            "HeapValueWriteBarriers",
        ],
    );
}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("class.reserved_slots_trace", || {
        reserved_slots_and_tracing(engine)
    });
    crate::support::isolate_case("class.heap_write_barriers", || write_barriers(engine));
}
