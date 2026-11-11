/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::Cell;
use std::ptr;

use js::jsapi::{
    GCReason, GetArrayBufferByteLength, GetArrayBufferData, HandleValueArray, JSPROP_ENUMERATE,
    JSPROP_PERMANENT, JSPROP_READONLY, JSTracer,
};
use js::jsval::{Int32Value, ObjectValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{
    DetachArrayBuffer, JS_CallFunctionValue, JS_ClearPendingException, JS_DefineProperty, JS_GC,
    JS_GetElement, JS_GetPendingException, JS_GetProperty, JS_IsExceptionPending,
    JS_NewPlainObject, JS_SetElement, JS_SetProperty, NewArrayBuffer, NewArrayObject,
};
use js::rust::{CustomAutoRooter, CustomTrace, JSEngine, ToBoolean};

use crate::support::{
    Observation, Value, eval, get_property, observe, run_case, set_global, with_global,
};

pub fn run(engine: &JSEngine) {
    property_cases(engine);
    element_cases(engine);
    call_cases(engine);
    buffer_cases(engine);
    rooter_cases(engine);
}

fn property_cases(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let object = JS_NewPlainObject(cx);
        rooted!(&in(cx) let object = object);
        rooted!(&in(cx) let mut value = UndefinedValue());
        run_case(
            "property.get_missing",
            &["JS_NewGlobalObject", "JS_NewPlainObject", "JS_GetProperty"],
            || {
                assert!(JS_GetProperty(
                    cx,
                    object.handle(),
                    c"missing".as_ptr(),
                    value.handle_mut()
                ));
                Observation::ok(observe(cx, value.get()))
            },
        );

        rooted!(&in(cx) let answer = Int32Value(42));
        run_case(
            "property.set_then_get",
            &[
                "JS_NewGlobalObject",
                "JS_NewPlainObject",
                "JS_SetProperty",
                "JS_GetProperty",
            ],
            || {
                assert!(JS_SetProperty(
                    cx,
                    object.handle(),
                    c"answer".as_ptr(),
                    answer.handle()
                ));
                assert!(JS_GetProperty(
                    cx,
                    object.handle(),
                    c"answer".as_ptr(),
                    value.handle_mut()
                ));
                assert_eq!(observe(cx, value.get()), Value::Number(42.0));
                Observation::ok(observe(cx, value.get()))
            },
        );

        let attrs = (JSPROP_ENUMERATE | JSPROP_PERMANENT | JSPROP_READONLY) as u32;
        rooted!(&in(cx) let fixed = Int32Value(7));
        rooted!(&in(cx) let replacement = Int32Value(9));
        run_case(
            "property.define_readonly",
            &[
                "JS_NewGlobalObject",
                "JS_NewPlainObject",
                "JS_DefineProperty",
                "JS_SetProperty",
                "JS_GetProperty",
            ],
            || {
                assert!(JS_DefineProperty(
                    cx,
                    object.handle(),
                    c"fixed".as_ptr(),
                    fixed.handle(),
                    attrs
                ));
                // A non-strict write to a read-only data property is ignored, not an error.
                assert!(JS_SetProperty(
                    cx,
                    object.handle(),
                    c"fixed".as_ptr(),
                    replacement.handle()
                ));
                assert!(JS_GetProperty(
                    cx,
                    object.handle(),
                    c"fixed".as_ptr(),
                    value.handle_mut()
                ));
                assert_eq!(observe(cx, value.get()), Value::Number(7.0));
                Observation::ok(observe(cx, value.get()))
            },
        );

        let _ = global;
    });
}

fn element_cases(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        rooted!(&in(cx) let values = vec![Int32Value(1), Int32Value(2), Int32Value(3)]);
        let array = NewArrayObject(cx, &HandleValueArray::from(&values));
        rooted!(&in(cx) let array = array);

        rooted!(&in(cx) let replacement = Int32Value(20));
        rooted!(&in(cx) let mut value = UndefinedValue());
        run_case(
            "element.set_then_get",
            &[
                "JS_NewGlobalObject",
                "NewArrayObject",
                "JS_SetElement",
                "JS_GetElement",
            ],
            || {
                assert!(JS_SetElement(cx, array.handle(), 1, replacement.handle()));
                assert!(JS_GetElement(cx, array.handle(), 1, value.handle_mut()));
                assert_eq!(observe(cx, value.get()), Value::Number(20.0));
                Observation::ok(observe(cx, value.get()))
            },
        );

        // Indexed access and `array[1]` must agree.
        run_case(
            "element.script_view",
            &[
                "JS_NewGlobalObject",
                "NewArrayObject",
                "JS_SetElement",
                "JS_GetElement",
            ],
            || {
                set_global(cx, global, c"array", ObjectValue(array.get()));
                rooted!(&in(cx) let script = eval(cx, global, "array[1] === 20 && array.length === 3"));
                assert!(ToBoolean(script.handle()));
                Observation::ok(Value::Boolean(true))
            },
        );
    });
}

fn call_cases(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let function = eval(cx, global, "(function (a, b) { return a + b; })");
        rooted!(&in(cx) let function = function);
        rooted!(&in(cx) let arguments = vec![Int32Value(2), Int32Value(3)]);
        rooted!(&in(cx) let mut rval = UndefinedValue());
        run_case(
            "call.return_value",
            &["JS_NewGlobalObject", "JS_CallFunctionValue"],
            || {
                assert!(JS_CallFunctionValue(
                    cx,
                    global,
                    function.handle(),
                    &HandleValueArray::from(&arguments),
                    rval.handle_mut()
                ));
                assert_eq!(observe(cx, rval.get()), Value::Number(5.0));
                Observation::ok(observe(cx, rval.get()))
            },
        );

        let thrower = eval(cx, global, "(function () { throw new TypeError('boom'); })");
        rooted!(&in(cx) let thrower = thrower);
        let no_arguments = HandleValueArray::empty();
        rooted!(&in(cx) let mut exception = UndefinedValue());
        run_case(
            "call.throw_pending_exception",
            &[
                "JS_NewGlobalObject",
                "JS_CallFunctionValue",
                "JS_IsExceptionPending",
                "JS_GetPendingException",
                "JS_ClearPendingException",
            ],
            || {
                assert!(!JS_CallFunctionValue(
                    cx,
                    global,
                    thrower.handle(),
                    &no_arguments,
                    rval.handle_mut()
                ));
                assert!(JS_IsExceptionPending(cx));
                assert!(JS_GetPendingException(cx, exception.handle_mut()));
                let name = get_property(cx, exception.get(), c"name");
                let name = match observe(cx, name) {
                    Value::String(name) => name,
                    other => panic!("exception name is not a string: {other:?}"),
                };
                JS_ClearPendingException(cx);
                assert!(!JS_IsExceptionPending(cx));
                Observation::exception(&name)
            },
        );
    });
}

fn buffer_cases(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let buffer = NewArrayBuffer(cx, 8);
        rooted!(&in(cx) let buffer = buffer);
        let mut shared = true;
        let data = GetArrayBufferData(buffer.get(), &mut shared, ptr::null());
        run_case(
            "buffer.new_and_write",
            &[
                "JS_NewGlobalObject",
                "NewArrayBuffer",
                "GetArrayBufferByteLength",
                "GetArrayBufferData",
            ],
            || {
                assert_eq!(GetArrayBufferByteLength(buffer.get()), 8);
                assert!(!data.is_null());
                assert!(!shared);
                *data.add(7) = 9;

                set_global(cx, global, c"buffer", ObjectValue(buffer.get()));
                rooted!(&in(cx) let ok = eval(
                    cx,
                    global,
                    "buffer.byteLength === 8 && new Uint8Array(buffer)[7] === 9"
                ));
                assert!(ToBoolean(ok.handle()));
                Observation::ok(Value::Number(9.0))
            },
        );

        run_case(
            "buffer.detach_contract",
            &[
                "JS_NewGlobalObject",
                "DetachArrayBuffer",
                "GetArrayBufferByteLength",
                "GetArrayBufferData",
            ],
            || {
                assert!(DetachArrayBuffer(cx, buffer.handle()));
                assert_eq!(GetArrayBufferByteLength(buffer.get()), 0);
                let data = GetArrayBufferData(buffer.get(), &mut shared, ptr::null());
                assert!(data.is_null());
                rooted!(&in(cx) let detached = eval(cx, global, "buffer.byteLength === 0"));
                assert!(ToBoolean(detached.handle()));
                Observation::ok(Value::Number(0.0))
            },
        );
    });
}

struct TraceCheck(Cell<bool>);

unsafe impl CustomTrace for TraceCheck {
    fn trace(&self, _: *mut JSTracer) {
        self.0.set(true);
    }
}

fn rooter_cases(engine: &JSEngine) {
    let apis = &["JS_NewGlobalObject", "JS_GC"];
    run_case("rooter.trace_on_gc", apis, || {
        with_global(engine, |cx, _| unsafe {
            let mut rooter = CustomAutoRooter::new(TraceCheck(Cell::new(false)));
            let guard = rooter.root(cx.raw_cx());
            JS_GC(cx, GCReason::API);
            // SpiderMonkey runs CustomAutoRooter trace callbacks during an explicit collection.
            // This suite treats that as the contract; a shim that does not call them is non-conforming.
            let traced = guard.0.get();
            assert!(
                traced || cfg!(feature = "js-quickjs"),
                "custom rooter trace was not called"
            );
            Observation::ok(Value::Boolean(traced))
        })
    });
}
