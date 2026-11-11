/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Script compilation, evaluation and script-private contract.

use js::jsapi::{
    HandleValueArray, SupportUnscopables,
};
use js::jsval::{Int32Value, ObjectValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{
    Call, Compile, Compile1, CompileFunctionUtf8, Evaluate, Evaluate2, JS_ExecuteScript,
    JS_ExecuteScript1, JS_GetScriptPrivate,
};
use js::rust::{
    transform_str_to_source_text, transform_u16_to_source_text, CompileOptionsWrapper,
    EnvironmentChain, JSEngine,
};

use crate::support::{record_case, Observation, Value as Observed};

pub fn run(engine: &JSEngine) {
    record_case(
        engine,
        "eval.compile_evaluate_execute",
        &[
            "Compile",
            "Compile1",
            "CompileFunctionUtf8",
            "DeleteCompileOptions",
            "Evaluate",
            "Evaluate2",
            "JS_ExecuteScript",
            "JS_ExecuteScript1",
            "JS_GetGlobalFromScript",
            "JS_GetScriptPrivate",
            "NewCompileOptions",
            "SetScriptPrivate",
        ],
        |cx, global| unsafe {
            let options = CompileOptionsWrapper::new(cx, c"eval-ext.js".to_owned(), 1);

            let utf8 = "1 + 2";
            let mut source = transform_str_to_source_text(utf8);
            rooted!(&in(cx) let script_utf8 = Compile1(cx, options.ptr, &mut source));
            assert!(!script_utf8.get().is_null());
            assert_eq!(js::jsapi::JS_GetGlobalFromScript(script_utf8.get()), global.get());

            rooted!(&in(cx) let mut rval = UndefinedValue());
            assert!(JS_ExecuteScript(cx, script_utf8.handle(), rval.handle_mut()));
            assert_eq!(rval.get().to_int32(), 3);

            let utf16: Vec<u16> = "20 + 22".encode_utf16().collect();
            let mut source = transform_u16_to_source_text(&utf16);
            rooted!(&in(cx) let script_utf16 = Compile(cx, options.ptr, &mut source));
            assert!(!script_utf16.get().is_null());
            assert!(JS_ExecuteScript1(cx, script_utf16.handle()));
            rooted!(&in(cx) let mut check = UndefinedValue());
            assert!(js::rust::wrappers2::JS_GetProperty(
                cx,
                global,
                c"globalThis".as_ptr(),
                check.handle_mut()
            ));

            let mut source = transform_str_to_source_text("globalThis.evaluated = 7; 11");
            assert!(Evaluate2(cx, options.ptr, &mut source, rval.handle_mut()));
            assert_eq!(rval.get().to_int32(), 11);

            let utf16: Vec<u16> = "globalThis.evaluated + 9".encode_utf16().collect();
            let mut source = transform_u16_to_source_text(&utf16);
            assert!(Evaluate(cx, options.ptr, &mut source, rval.handle_mut()));
            assert_eq!(rval.get().to_int32(), 16);

            let private = Int32Value(99);
            js::jsapi::SetScriptPrivate(script_utf8.get(), &private);
            rooted!(&in(cx) let mut private_out = UndefinedValue());
            JS_GetScriptPrivate(script_utf8.get(), private_out.handle_mut());
            assert_eq!(private_out.get().to_int32(), 99);

            let chain = EnvironmentChain::new(cx, SupportUnscopables::Yes);
            let body = c"return event + 1;";
            let args = [c"event".as_ptr()];
            rooted!(&in(cx) let function = CompileFunctionUtf8(
                cx,
                chain.get(),
                options.ptr,
                c"onmessage".as_ptr(),
                1,
                args.as_ptr(),
                body.as_ptr() as *const _,
                16,
            ));
            assert!(!function.get().is_null());
            rooted!(&in(cx) let function_value = ObjectValue(js::jsapi::JS_GetFunctionObject(function.get())));
            rooted!(&in(cx) let this = UndefinedValue());
            let argv = [Int32Value(41)];
            let argv = HandleValueArray {
                length_: 1,
                elements_: argv.as_ptr(),
            };
            assert!(Call(cx, this.handle(), function_value.handle(), &argv, rval.handle_mut()));
            assert_eq!(rval.get().to_int32(), 42);
            Observation::ok(Observed::Boolean(true))
        },
    );
}
