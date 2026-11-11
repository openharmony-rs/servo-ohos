/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Realm entry/exit, global lookup, private storage and trace-hook contract.

use std::ptr;

use js::jsapi::{self, JSTracer};
use js::rust::JSEngine;
use js::rust::wrappers2::JS_NewGlobalObject;
use js::jsapi::OnNewGlobalHookOption;
use js::rooted;
use js::rust::Runtime;

use crate::support::{Observation, Value, record};

unsafe extern "C" fn trace_global(_trc: *mut JSTracer, _global: *mut jsapi::JSObject) {}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("realm.enter_exit_and_private", || {
    let mut runtime = Runtime::new(engine.handle());
    let cx = runtime.cx();
    unsafe {
        let options = jsapi::JS_NewRealmOptions();
        assert!(!options.is_null());
        (*options).creationOptions_.traceGlobal_ = Some(trace_global);

        rooted!(&in(cx) let global = JS_NewGlobalObject(
            cx,
            &js::rust::SIMPLE_GLOBAL_CLASS,
            ptr::null_mut(),
            OnNewGlobalHookOption::FireOnNewGlobalHook,
            options,
        ));
        assert!(!global.get().is_null());

        let global_realm = jsapi::GetObjectRealmOrNull(global.get());
        assert!(!global_realm.is_null());
        assert_eq!(
            jsapi::GetRealmGlobalOrNull(global_realm),
            global.get(),
            "realm global round trip"
        );
        assert_eq!(
            jsapi::GetNonCCWObjectGlobal(global.get()),
            global.get(),
            "non-CCW global lookup"
        );
        assert!(jsapi::CurrentGlobalOrNull(cx.raw_cx()).is_null());

        let previous = jsapi::EnterRealm(cx.raw_cx(), global.get());
        assert!(previous.is_null(), "no realm was entered before");
        assert_eq!(jsapi::GetCurrentRealmOrNull(cx.raw_cx()), global_realm);
        assert_eq!(jsapi::CurrentGlobalOrNull(cx.raw_cx()), global.get());
        let current = jsapi::CurrentGlobal(cx.raw_cx());
        assert!(!current.is_null());
        assert!(!(*current).is_null());

        let private = 0x1234_u32 as usize as *mut std::ffi::c_void;
        jsapi::SetRealmPrivate(global_realm, private);
        assert_eq!(jsapi::GetRealmPrivate(global_realm), private);
        jsapi::SetRealmPrivate(global_realm, ptr::null_mut());
        assert!(jsapi::GetRealmPrivate(global_realm).is_null());

        jsapi::LeaveRealm(cx.raw_cx(), previous);
        assert!(jsapi::CurrentGlobalOrNull(cx.raw_cx()).is_null());
        assert!(jsapi::GetCurrentRealmOrNull(cx.raw_cx()).is_null());

        jsapi::DeleteRealmOptions(options);
    }

    record(
        "realm.enter_exit_and_private",
        &Observation::ok(Value::Boolean(true)),
        &[
            "CurrentGlobal",
            "CurrentGlobalOrNull",
            "DeleteRealmOptions",
            "EnterRealm",
            "GetCurrentRealmOrNull",
            "GetNonCCWObjectGlobal",
            "GetObjectRealmOrNull",
            "GetRealmGlobalOrNull",
            "GetRealmPrivate",
            "JS_GlobalObjectTraceHook",
            "JS_NewRealmOptions",
            "LeaveRealm",
            "SetRealmPrivate",
        ],
    );
    });
}
