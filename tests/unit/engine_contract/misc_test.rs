/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![allow(unsafe_op_in_unsafe_fn)]

//! Runtime-wide GC, memory accounting and engine options.

use std::ffi::c_void;
use std::ptr;

use js::jsapi::{
    self, ContextOptions, GCReason, GCOptions, HeapState, MemoryUse, ObjectPrivateVisitor,
    ServoSizes, SliceBudget, TraceKind, JSJitCompilerOption,
};
use js::jsval::Int32Value;
use js::rooted;
use js::rust::wrappers2::JS_NewPlainObject;
use js::rust::JSEngine;

use crate::support::{record, with_global, Observation, Value as Observed};

unsafe extern "C" fn size_of(_: *const c_void) -> usize {
    7
}

unsafe extern "C" fn object_size(_: *mut jsapi::JSObject) -> usize {
    11
}

unsafe extern "C" fn has_private(obj: *mut jsapi::JSObject, iface: *mut *mut jsapi::nsISupports) -> bool {
    let _ = obj;
    *iface = ptr::null_mut();
    false
}

unsafe extern "C" fn want_to_measure(_: *mut jsapi::JSObject) -> bool {
    false
}

fn gc_state(engine: &JSEngine) {
    with_global(engine, |cx, _global| unsafe {
        assert_eq!(jsapi::JS_GetParentRuntime(cx.raw_cx()), jsapi::JS_GetRuntime(cx.raw_cx()));
        assert_eq!(jsapi::RuntimeHeapState(), HeapState::Idle);

        let before = jsapi::GetGCHeapUsage(cx.raw_cx());
        rooted!(&in(cx) let unused = JS_NewPlainObject(cx));
        assert!(!unused.get().is_null());
        let after = jsapi::GetGCHeapUsage(cx.raw_cx());
        assert!(after >= before, "GC heap usage must not shrink on allocation");

        jsapi::NonIncrementalGC(cx.raw_cx(), GCOptions::Normal, GCReason::API);
        assert_eq!(jsapi::RuntimeHeapState(), HeapState::Idle);

        let budget = SliceBudget {
            counter: jsapi::SliceBudget_UnlimitedCounter,
            interruptRequested: ptr::null_mut(),
            budget: Default::default(),
            interrupted: false,
            idle: false,
            extended: false,
            keepGoing: false,
        };
        jsapi::StartIncrementalGC(cx.raw_cx(), GCOptions::Normal, GCReason::API, &budget);
        assert_eq!(jsapi::RuntimeHeapState(), HeapState::Idle);

        jsapi::SetLowMemoryState(cx.raw_cx(), true);
        jsapi::SetLowMemoryState(cx.raw_cx(), false);
        assert_eq!(jsapi::RuntimeHeapState(), HeapState::Idle);

        assert_eq!(
            std::ffi::CStr::from_ptr(jsapi::GCTraceKindToAscii(TraceKind::Object)).to_str(),
            Ok("JS Object")
        );
        assert_eq!(
            std::ffi::CStr::from_ptr(jsapi::GCTraceKindToAscii(TraceKind::Script)).to_str(),
            Ok("JS Script")
        );
        let mut value = 0;
        assert!(jsapi::JS_GetGlobalJitCompilerOption(
            cx.raw_cx(),
            JSJitCompilerOption::JSJITCOMPILER_ION_ENABLE,
            &mut value,
        ));
    });
    record(
        "misc.gc_state",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "GCTraceKindToAscii",
            "GetGCHeapUsage",
            "JS_GetGlobalJitCompilerOption",
            "JS_GetParentRuntime",
            "JS_GetRuntime",
            "JS_NewGlobalObject",
            "JS_NewPlainObject",
            "NonIncrementalGC",
            "RuntimeHeapState",
            "SetLowMemoryState",
            "StartIncrementalGC",
        ],
    );
}

fn roots_options_and_memory(engine: &JSEngine) {
    with_global(engine, |cx, _global| unsafe {
        let options: *mut ContextOptions = jsapi::ContextOptionsRef(cx.raw_cx());
        assert!(!options.is_null());
        assert_eq!(options, jsapi::ContextOptionsRef(cx.raw_cx()));
        let wasm = (*options).wasm_();
        (*options).set_wasm_(!wasm);
        assert_eq!((*options).wasm_(), !wasm);
        (*options).set_wasm_(wasm);
        let mut raw = Int32Value(9);
        assert!(jsapi::AddRawValueRoot(
            cx.raw_cx(),
            &mut raw,
            c"contract-root".as_ptr(),
        ));
        jsapi::RemoveRawValueRoot(cx.raw_cx(), &mut raw);
        rooted!(&in(cx) let object = JS_NewPlainObject(cx));
        assert!(!object.get().is_null());
        jsapi::AddAssociatedMemory(object.get(), 64, MemoryUse::DOMBinding);
        jsapi::RemoveAssociatedMemory(object.get(), 64, MemoryUse::DOMBinding);

        // The Servo stats glue dispatches through this process-global callback. It must be
        // installed before CollectServoSizes enumerates objects.
        js::glue::InitializeMemoryReporter(Some(want_to_measure));
        let mut sizes: ServoSizes = std::mem::zeroed();
        assert!(jsapi::AddServoSizeOf(
            cx.raw_cx(),
            Some(size_of),
            ptr::null_mut(),
            &mut sizes,
        ));
        let mut collected: ServoSizes = std::mem::zeroed();
        assert!(js::glue::CollectServoSizes(
            cx.raw_cx(),
            &mut collected,
            Some(object_size),
        ));

        let mut visitor = ObjectPrivateVisitor {
            vtable_: ptr::null(),
            getISupports_: Some(has_private),
        };
        let mut with_visitor: ServoSizes = std::mem::zeroed();
        assert!(jsapi::AddServoSizeOf(
            cx.raw_cx(),
            Some(size_of),
            &mut visitor,
            &mut with_visitor,
        ));
    });
    record(
        "misc.roots_options_memory",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "AddAssociatedMemory",
            "AddRawValueRoot",
            "AddServoSizeOf",
            "CollectServoSizes",
            "ContextOptionsRef",
            "JS_NewGlobalObject",
            "JS_NewPlainObject",
            "RemoveAssociatedMemory",
            "RemoveRawValueRoot",
        ],
    );
}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("misc.gc_state", || gc_state(engine));
    crate::support::isolate_case("misc.roots_options_memory", || roots_options_and_memory(engine));
}
