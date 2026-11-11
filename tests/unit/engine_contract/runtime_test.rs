/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Runtime/context ownership and warning-reporter contract.

use std::ptr;

use js::jsapi::{self, JSErrorReport, JSGCParamKey};
use js::rust::JSEngine;

use crate::support::{Observation, Value, record};

unsafe extern "C" fn warning_reporter(_cx: *mut jsapi::JSContext, _report: *mut JSErrorReport) {}

pub fn run(_engine: &JSEngine) {
    crate::support::isolate_case("runtime.context_lifecycle", || {
    unsafe {
        let cx = jsapi::JS_NewContext(1 << 20, ptr::null_mut());
        assert!(!cx.is_null(), "JS_NewContext");
        assert!(!jsapi::JS_GetRuntime(cx).is_null(), "JS_GetRuntime");

        jsapi::JS_SetNativeStackQuota(cx, 256 * 1024, 192 * 1024, 128 * 1024);

        let reporter: jsapi::WarningReporter = Some(warning_reporter);
        let previous = jsapi::SetWarningReporter(cx, reporter);
        assert_eq!(
            jsapi::GetWarningReporter(cx).map(|f| f as usize),
            Some(warning_reporter as *const () as usize)
        );
        assert_eq!(
            jsapi::SetWarningReporter(cx, previous).map(|f| f as usize),
            Some(warning_reporter as *const () as usize)
        );

        let private = 0x5a5a_u32 as usize as *mut std::ffi::c_void;
        jsapi::JS_SetContextPrivate(cx, private);
        assert_eq!(jsapi::JS_GetContextPrivate(cx), private);
        jsapi::JS_SetContextPrivate(cx, ptr::null_mut());
        assert!(jsapi::JS_GetContextPrivate(cx).is_null());

        jsapi::JS_DestroyContext(cx);
    }

    record(
        "runtime.context_lifecycle",
        &Observation::ok(Value::Boolean(true)),
        &[
            "JS_DestroyContext",
            "JS_GetContextPrivate",
            "JS_GetRuntime",
            "JS_NewContext",
            "JS_SetContextPrivate",
            "JS_SetNativeStackQuota",
            "GetWarningReporter",
            "SetWarningReporter",
        ],
    );

    unsafe {
        let cx = jsapi::JS_NewContext(1 << 20, ptr::null_mut());
        assert!(!cx.is_null());
        let previous = jsapi::JS_GetGCParameter(cx, JSGCParamKey::JSGC_MAX_BYTES);
        jsapi::JS_SetGCParameter(cx, JSGCParamKey::JSGC_MAX_BYTES, previous.saturating_sub(1));
        assert_eq!(
            jsapi::JS_GetGCParameter(cx, JSGCParamKey::JSGC_MAX_BYTES),
            previous.saturating_sub(1)
        );
        jsapi::JS_SetGCParameter(cx, JSGCParamKey::JSGC_MAX_BYTES, previous);
        jsapi::JS_DestroyContext(cx);
    }
    });
}
