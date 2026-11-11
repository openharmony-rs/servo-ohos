/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![allow(unsafe_op_in_unsafe_fn)]

//! Object entry points that are mostly about identity, rooted vectors, iteration and the
//! remaining property/collection helpers.

use std::cell::Cell;
use std::ffi::c_void;
use std::ptr::{self, NonNull};
use std::sync::{LazyLock, Mutex};

use js::context::JSContext;
use js::conversions::jsstr_to_string;
use js::gc::StackGCVector;
use js::jsapi::{
    CompilationType, GetStaticPrototype, Handle, HandleString, HandleValue, JSContext as RawJSContext,
    JSFunction, JSJitGetterCallArgs, JSJitInfo, JSJitSetterCallArgs, JSObject, JSString,
    JS_ForOfIteratorValueIsIterable, JS_GetFunctionArity, JS_IsGlobalObject, JSSecurityCallbacks,
    JSPROP_ENUMERATE, JSPROP_PERMANENT, JSPROP_READONLY, ObjectOpResult, RuntimeCode, Value,
    jsid, PropertyDescriptor,
};
use js::jsval::{Int32Value, ObjectValue, StringValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{
    CallJitGetterOp, CallJitSetterOp, Construct1, DefineFunctionWithReserved,
    FromPropertyDescriptor, GetCurrentRealmOrNull, GetFunctionRealm, GetObjectProto,
    GetRealmErrorPrototype, GetRealmFunctionPrototype, GetRealmIteratorPrototype,
    GetRealmObjectPrototype, IsArgumentsObject, IsMapObject, IsSetObject, JS_DefineProperty,
    JS_DefineProperty2, JS_DefineUCProperty2, JS_ForwardGetPropertyTo, JS_ForwardSetPropertyTo,
    JS_GetFunctionDisplayId, JS_InitializePropertiesFromCompatibleNativeObject,
    JS_LinkConstructorAndPrototype, JS_NewGlobalObject,
    JS_NewObjectWithoutMetadata, JS_NewPlainObject, JS_ResolveStandardClass,
    JS_SetImmutablePrototype, JS_SetSecurityCallbacks, JS_ValueToFunction, MapClear, MapForEach,
    MapKeys, MapValues, NewMapObject, NewSetObject, SetAdd, SetDataPropertyDescriptor, SetDelete,
    SetEntries, SetForEach, SetHas, SetKeys,
    SetPropertyIgnoringNamedGetter, SetValues,
};
use js::rust::wrappers2::JS_NewStringCopyN;
use js::rust::{
    CompileOptionsWrapper, HandleObject, JSEngine, RealmOptions, RootedObjectVectorWrapper, Runtime,
    SIMPLE_GLOBAL_CLASS, evaluate_script, for_of,
};

use crate::support::{Observation, Value as Observed, eval, record_case, set_global};

const ATTRS: u32 = (JSPROP_ENUMERATE | JSPROP_PERMANENT) as u32;

fn string_ptr_of(cx: &mut JSContext, string: *mut JSString) -> String {
    unsafe {
        jsstr_to_string(
            cx,
            NonNull::new(string).expect("non-null JS string"),
        )
    }
}

fn string_of(cx: &mut JSContext, value: js::jsval::JSVal) -> String {
    assert!(value.is_string(), "expected a string value");
    string_ptr_of(cx, value.to_string())
}

fn object_of(cx: &mut JSContext, global: HandleObject, source: &str) -> *mut JSObject {
    unsafe { eval(cx, global, source).to_object() }
}

unsafe fn id_for(cx: &mut JSContext, global: HandleObject, source: &str) -> jsid {
    rooted!(&in(cx) let value = eval(cx, global, source));
    rooted!(&in(cx) let mut id = jsid::default());
    assert!(js::rust::wrappers2::JS_ValueToId(cx, value.handle(), id.handle_mut()));
    id.get()
}

fn rooted_vectors(engine: &JSEngine) {
    record_case(
        engine,
        "objects.extra_rooted_vectors",
        &[
            "AppendToIdVector",
            "AppendToRootedObjectVector",
            "CreateRootedIdVector",
            "CreateRootedObjectVector",
            "DeleteRootedObjectVector",
            "DestroyRootedIdVector",
            "GetIdVectorAddress",
            "GetObjectVectorAddress",
            "SliceRootedIdVector",
        ],
        |cx, global| unsafe {
            {
                let mut ids = js::rust::IdVector::new(cx);
                rooted!(&in(cx) let id = id_for(cx, global, "'rooted-id'"));
                assert!(js::rust::wrappers2::AppendToIdVector(ids.handle_mut(), id.handle()));
                assert_eq!(ids.len(), 1);
                assert!(ids[0].is_string());
                assert_eq!(string_ptr_of(cx, ids[0].to_string()), "rooted-id");
            }

            {
                let objects = RootedObjectVectorWrapper::new(cx);
                let first = object_of(cx, global, "({ first: 1 })");
                let second = object_of(cx, global, "({ second: 2 })");
                assert!(objects.append(first));
                assert!(!objects.handle().ptr.is_null());
                assert!(objects.append(second));
            }

            Observation::ok(Observed::Boolean(true))
        },
    );
}

fn identity_and_functions(engine: &JSEngine) {
    record_case(
        engine,
        "objects.extra_identity_and_functions",
        &[
            "Construct1",
            "DefineFunctionWithReserved",
            "GetFunctionRealm",
            "GetObjectProto",
            "GetRealmErrorPrototype",
            "GetRealmFunctionPrototype",
            "GetRealmIteratorPrototype",
            "GetStaticPrototype",
            "IsArgumentsObject",
            "JS_GetFunctionArity",
            "JS_GetFunctionDisplayId",
            "JS_InitializePropertiesFromCompatibleNativeObject",
            "JS_IsGlobalObject",
            "JS_LinkConstructorAndPrototype",
            "JS_NewObjectWithoutMetadata",
            "JS_SetImmutablePrototype",
            "JS_ValueToFunction",
            "NewFunctionWithReserved",
            "SetFunctionNativeReserved",
            "GetFunctionNativeReserved",
        ],
        |cx, global| unsafe {
            rooted!(&in(cx) let proto = GetRealmObjectPrototype(cx));
            rooted!(&in(cx) let mut actual_proto = ptr::null_mut::<JSObject>());
            assert!(GetObjectProto(cx, global, actual_proto.handle_mut()));
            assert_eq!(actual_proto.get(), proto.get());
            rooted!(&in(cx) let plain = JS_NewPlainObject(cx));
            assert_eq!(GetStaticPrototype(plain.get()), proto.get());
            assert!(JS_IsGlobalObject(global.get()));
            assert!(!JS_IsGlobalObject(plain.get()));

            rooted!(&in(cx) let function_proto = GetRealmFunctionPrototype(cx));
            rooted!(&in(cx) let error_proto = GetRealmErrorPrototype(cx));
            rooted!(&in(cx) let iterator_proto = GetRealmIteratorPrototype(cx));
            assert_eq!(
                function_proto.get(),
                object_of(cx, global, "Function.prototype")
            );
            assert_eq!(error_proto.get(), object_of(cx, global, "Error.prototype"));
            assert_eq!(
                iterator_proto.get(),
                object_of(
                    cx,
                    global,
                    "Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]()))"
                )
            );

            let arguments = object_of(cx, global, "(function () { return arguments; })()");
            assert!(IsArgumentsObject(js::rust::Handle::from_marked_location(&arguments).into()));
            assert!(!IsArgumentsObject(plain.handle().into()));
            let map = NewMapObject(cx);
            let set = NewSetObject(cx);
            let mut is_map = false;
            assert!(IsMapObject(cx, js::rust::Handle::from_marked_location(&map).into(), &mut is_map));
            assert!(is_map);
            let mut is_set = false;
            assert!(IsSetObject(cx, js::rust::Handle::from_marked_location(&set).into(), &mut is_set));
            assert!(is_set);

            rooted!(&in(cx) let ctor = object_of(cx, global, "(function C(v) { this.v = v; })"));
            rooted!(&in(cx) let ctor_value = ObjectValue(ctor.get()));
            rooted!(&in(cx) let args = vec![Int32Value(31)]);
            let args = js::jsapi::HandleValueArray::from(&args);
            rooted!(&in(cx) let mut instance = ptr::null_mut::<JSObject>());
            assert!(Construct1(cx, ctor_value.handle(), &args, instance.handle_mut()));
            set_global(cx, global, c"instance", ObjectValue(instance.get()));
            set_global(cx, global, c"C", ctor_value.get());
            assert!(eval(cx, global, "instance.v === 31 && instance instanceof C").to_boolean());

            assert_eq!(
                JS_ValueToFunction(cx, ctor_value.handle().into()),
                ctor.get().cast::<JSFunction>()
            );
            assert_eq!(JS_GetFunctionArity(ctor.get().cast::<JSFunction>()), 1);
            rooted!(&in(cx) let mut display = ptr::null_mut::<JSString>());
            rooted!(&in(cx) let constructor = ctor.get().cast::<JSFunction>());
            assert!(JS_GetFunctionDisplayId(
                cx,
                constructor.handle(),
                display.handle_mut(),
            ));
            assert_eq!(string_ptr_of(cx, display.get()), "C");
            let realm = GetFunctionRealm(cx, ctor.handle().into());
            assert!(!realm.is_null());
            assert_eq!(realm, GetCurrentRealmOrNull(cx));
            assert!(JS_LinkConstructorAndPrototype(cx, ctor.handle(), plain.handle()));
            set_global(cx, global, c"linked_ctor", ObjectValue(ctor.get()));
            set_global(cx, global, c"linked_proto", ObjectValue(plain.get()));
            assert!(eval(
                cx,
                global,
                "linked_ctor.prototype === linked_proto && linked_proto.constructor === linked_ctor"
            )
            .to_boolean());

            rooted!(&in(cx) let without_metadata =
                JS_NewObjectWithoutMetadata(cx, ptr::null(), proto.handle()));
            rooted!(&in(cx) let mut copied = JS_NewObjectWithoutMetadata(cx, ptr::null(), proto.handle()));
            let uc_name: Vec<u16> = "copied".encode_utf16().collect();
            rooted!(&in(cx) let copied_value = Int32Value(55));
            assert!(JS_DefineUCProperty2(
                cx,
                without_metadata.handle(),
                uc_name.as_ptr(),
                uc_name.len(),
                copied_value.handle(),
                ATTRS,
            ));
            assert!(JS_InitializePropertiesFromCompatibleNativeObject(
                cx,
                copied.handle(),
                without_metadata.handle(),
            ));
            set_global(cx, global, c"copied", ObjectValue(copied.get()));
            assert!(eval(cx, global, "copied.copied === 55").to_boolean());

            rooted!(&in(cx) let immutable = JS_NewPlainObject(cx));
            let mut succeeded = false;
            assert!(JS_SetImmutablePrototype(cx, immutable.handle(), &mut succeeded));
            assert!(succeeded);
            set_global(cx, global, c"immutable", ObjectValue(immutable.get()));
            assert!(eval(
                cx,
                global,
                "Reflect.setPrototypeOf(immutable, null) === false && Object.getPrototypeOf(immutable) !== null"
            )
            .to_boolean());

            let native = DefineFunctionWithReserved(
                cx,
                plain.get(),
                c"reservedNative".as_ptr(),
                Some(read_reserved),
                2,
                JSPROP_ENUMERATE as u32,
            );
            assert!(!native.is_null());
            js::jsapi::SetFunctionNativeReserved(
                native.cast(),
                0,
                &Int32Value(73),
            );
            set_global(cx, global, c"reservedNative", ObjectValue(native.cast()));
            assert!(eval(cx, global, "reservedNative() === 73").to_boolean());

            Observation::ok(Observed::Boolean(true))
        },
    );
}

unsafe extern "C" fn read_reserved(_cx: *mut RawJSContext, argc: u32, vp: *mut Value) -> bool {
    let args = js::jsapi::CallArgs::from_vp(vp, argc);
    args.rval().set(*js::jsapi::GetFunctionNativeReserved(
        args.callee(),
        0,
    ));
    true
}

fn collections(engine: &JSEngine) {
    record_case(
        engine,
        "objects.extra_collections",
        &[
            "IsMapObject",
            "IsSetObject",
            "MapClear",
            "MapForEach",
            "MapKeys",
            "MapSize",
            "MapValues",
            "NewMapObject",
            "JS_NewPlainObject",
            "NewSetObject",
            "SetAdd",
            "SetDelete",
            "SetEntries",
            "SetForEach",
            "SetHas",
            "SetKeys",
            "SetValues",
        ],
        |cx, global| unsafe {
            rooted!(&in(cx) let map = NewMapObject(cx));
            rooted!(&in(cx) let one = Int32Value(1));
            rooted!(&in(cx) let two = Int32Value(2));
            rooted!(&in(cx) let forty_two = Int32Value(42));
            assert!(js::rust::wrappers2::MapSet(cx, map.handle(), one.handle(), forty_two.handle()));
            assert!(js::rust::wrappers2::MapSet(cx, map.handle(), two.handle(), one.handle()));
            let mut is_map = false;
            assert!(IsMapObject(cx, map.handle().into(), &mut is_map));
            assert!(is_map);
            let mut map_is_set = false;
            assert!(IsSetObject(cx, map.handle().into(), &mut map_is_set));
            assert!(!map_is_set);

            rooted!(&in(cx) let callback = eval(
                cx,
                global,
                "(function (v, k) { globalThis.result = (globalThis.result || 0) + v * 10 + k; })",
            ));
            rooted!(&in(cx) let receiver = UndefinedValue());
            assert!(MapForEach(cx, map.handle(), callback.handle(), receiver.handle()));
            assert_eq!(eval(cx, global, "result").to_int32(), 433);

            rooted!(&in(cx) let mut keys = UndefinedValue());
            assert!(MapKeys(cx, map.handle(), keys.handle_mut()));
            set_global(cx, global, c"map_keys", keys.get());
            assert!(eval(cx, global, "Array.from(map_keys).join(',') === '1,2'").to_boolean());
            rooted!(&in(cx) let mut values = UndefinedValue());
            assert!(MapValues(cx, map.handle(), values.handle_mut()));
            set_global(cx, global, c"map_values", values.get());
            assert!(eval(cx, global, "Array.from(map_values).join(',') === '42,1'").to_boolean());
            assert!(MapClear(cx, map.handle()));
            assert_eq!(js::rust::wrappers2::MapSize(cx, map.handle()), 0);

            rooted!(&in(cx) let set = NewSetObject(cx));
            assert!(SetAdd(cx, set.handle(), one.handle()));
            assert!(SetAdd(cx, set.handle(), two.handle()));
            let mut has = false;
            assert!(SetHas(cx, set.handle(), one.handle(), &mut has));
            assert!(has);
            let mut deleted = false;
            assert!(SetDelete(cx, set.handle(), one.handle(), &mut deleted));
            assert!(deleted);
            assert!(SetAdd(cx, set.handle(), one.handle()));
            let mut set_is_map = false;
            assert!(IsMapObject(cx, set.handle().into(), &mut set_is_map));
            assert!(!set_is_map);
            let mut is_set = false;
            assert!(IsSetObject(cx, set.handle().into(), &mut is_set));
            assert!(is_set);

            rooted!(&in(cx) let mut set_keys = UndefinedValue());
            assert!(SetKeys(cx, set.handle(), set_keys.handle_mut()));
            set_global(cx, global, c"set_keys", set_keys.get());
            assert!(eval(cx, global, "Array.from(set_keys).join(',') === '2,1'").to_boolean());
            rooted!(&in(cx) let mut set_values = UndefinedValue());
            assert!(SetValues(cx, set.handle(), set_values.handle_mut()));
            set_global(cx, global, c"set_values", set_values.get());
            assert!(eval(cx, global, "Array.from(set_values).join(',') === '2,1'").to_boolean());
            rooted!(&in(cx) let mut set_entries = UndefinedValue());
            assert!(SetEntries(cx, set.handle(), set_entries.handle_mut()));
            set_global(cx, global, c"set_entries", set_entries.get());
            assert!(eval(cx, global, "JSON.stringify(Array.from(set_entries)) === '[[2,2],[1,1]]'").to_boolean());
            rooted!(&in(cx) let set_callback = eval(
                cx,
                global,
                "(function (v) { globalThis.set_result = (globalThis.set_result || 0) + v * 2; })",
            ));
            assert!(SetForEach(cx, set.handle(), set_callback.handle(), receiver.handle()));
            assert_eq!(eval(cx, global, "set_result").to_int32(), 6);

            Observation::ok(Observed::Boolean(true))
        },
    );
}

fn descriptors_and_forwarding(engine: &JSEngine) {
    record_case(
        engine,
        "objects.extra_descriptors_and_forwarding",
        &[
            "FromPropertyDescriptor",
            "JS_DefineProperty2",
            "JS_DefineUCProperty2",
            "JS_ForwardGetPropertyTo",
            "JS_ForwardSetPropertyTo",
            "JS_NewPlainObject",
            "JS_ResolveStandardClass",
            "JS_SetProperty",
            "SetDataPropertyDescriptor",
            "SetPropertyIgnoringNamedGetter",
            "JS_ValueToId",
        ],
        |cx, global| unsafe {
            rooted!(&in(cx) let mut desc = PropertyDescriptor::default());
            rooted!(&in(cx) let value = Int32Value(90));
            SetDataPropertyDescriptor(desc.handle_mut(), value.handle(), ATTRS);
            assert!(desc.hasValue_() && desc.hasWritable_() && desc.hasEnumerable_());
            assert!(desc.writable_() && desc.enumerable_() && !desc.configurable_());
            rooted!(&in(cx) let mut desc_object = UndefinedValue());
            assert!(FromPropertyDescriptor(cx, desc.handle(), desc_object.handle_mut()));
            set_global(cx, global, c"desc_object", desc_object.get());
            assert!(eval(
                cx,
                global,
                "desc_object.value === 90 && desc_object.writable === true && desc_object.enumerable === true && desc_object.configurable === false"
            )
            .to_boolean());

            rooted!(&in(cx) let target = JS_NewPlainObject(cx));
            rooted!(&in(cx) let receiver = JS_NewPlainObject(cx));
            let uc_name: Vec<u16> = "unicode".encode_utf16().collect();
            rooted!(&in(cx) let uc_value = Int32Value(12));
            assert!(JS_DefineUCProperty2(
                cx,
                target.handle(),
                uc_name.as_ptr(),
                uc_name.len(),
                uc_value.handle(),
                ATTRS,
            ));
            set_global(cx, global, c"target", ObjectValue(target.get()));
            set_global(cx, global, c"receiver", ObjectValue(receiver.get()));
            assert!(eval(cx, global, "target.unicode === 12").to_boolean());

            rooted!(&in(cx) let getter = object_of(
                cx,
                global,
                "(function () { return this.marker; })",
            ));
            rooted!(&in(cx) let setter = object_of(
                cx,
                global,
                "(function (v) { this.seen = v; })",
            ));
            assert!(JS_DefineProperty2(
                cx,
                target.handle(),
                c"x".as_ptr(),
                getter.handle(),
                setter.handle(),
                JSPROP_ENUMERATE as u32,
            ));
            rooted!(&in(cx) let marker = Int32Value(17));
            assert!(JS_DefineProperty(
                cx,
                receiver.handle(),
                c"marker".as_ptr(),
                marker.handle(),
                ATTRS,
            ));
            rooted!(&in(cx) let x_id = id_for(cx, global, "'x'"));
            rooted!(&in(cx) let receiver_value = ObjectValue(receiver.get()));
            rooted!(&in(cx) let mut forwarded = UndefinedValue());
            assert!(JS_ForwardGetPropertyTo(
                cx,
                target.handle(),
                x_id.handle(),
                receiver_value.handle(),
                forwarded.handle_mut(),
            ));
            assert_eq!(forwarded.get().to_int32(), 17);
            rooted!(&in(cx) let twenty_three = Int32Value(23));
            let mut result = ObjectOpResult::default();
            assert!(JS_ForwardSetPropertyTo(
                cx,
                target.handle(),
                x_id.handle(),
                twenty_three.handle(),
                receiver_value.handle(),
                &mut result,
            ));
            assert!(result.ok());
            assert!(eval(cx, global, "receiver.seen === 23").to_boolean());

            rooted!(&in(cx) let ignored = JS_NewPlainObject(cx));
            rooted!(&in(cx) let ignored_value = Int32Value(5));
            assert!(JS_DefineProperty(
                cx,
                ignored.handle(),
                c"readonly".as_ptr(),
                ignored_value.handle(),
                (JSPROP_PERMANENT | JSPROP_READONLY) as u32,
            ));
            let readonly_id = id_for(cx, global, "'readonly'");
            rooted!(&in(cx) let readonly_id = readonly_id);
            rooted!(&in(cx) let nine = Int32Value(9));
            rooted!(&in(cx) let ignored_value_root = ObjectValue(ignored.get()));
            let mut own_desc = PropertyDescriptor::default();
            own_desc.set_hasValue_(true);
            own_desc.value_ = Int32Value(5);
            own_desc.set_hasWritable_(true);
            own_desc.set_writable_(false);
            rooted!(&in(cx) let own_desc = own_desc);
            let mut ignored_result = ObjectOpResult::default();
            assert!(SetPropertyIgnoringNamedGetter(
                cx,
                ignored.handle(),
                readonly_id.handle(),
                nine.handle(),
                ignored_value_root.handle(),
                Some(own_desc.handle()),
                &mut ignored_result,
            ));
            assert!(!ignored_result.ok());

            let array_id = id_for(cx, global, "'Array'");
            rooted!(&in(cx) let array_id = array_id);
            let mut resolved = false;
            assert!(JS_ResolveStandardClass(
                cx,
                global,
                array_id.handle(),
                &mut resolved,
            ));
            assert!(resolved);

            Observation::ok(Observed::Boolean(true))
        },
    );
}

fn jit_getters_and_setters(engine: &JSEngine) {
    record_case(
        engine,
        "objects.extra_jit_accessors",
        &["CallJitGetterOp", "CallJitSetterOp"],
        |cx, global| unsafe {
            rooted!(&in(cx) let this = JS_NewPlainObject(cx));
            let mut info: JSJitInfo = std::mem::zeroed();
            info.__bindgen_anon_1.getter = Some(jit_getter);
            let mut getter_vp = [UndefinedValue(); 3];
            assert!(js::rust::wrappers2::CallJitGetterOp(
                &info,
                cx,
                this.handle(),
                0x1234 as *mut c_void,
                0,
                getter_vp.as_mut_ptr(),
            ));
            assert_eq!(getter_vp[0].to_int32(), 77);

            let mut info: JSJitInfo = std::mem::zeroed();
            info.__bindgen_anon_1.setter = Some(jit_setter);
            JIT_SETTER_VALUE.with(|cell| cell.set(0));
            let mut setter_vp = [UndefinedValue(); 3];
            setter_vp[2] = Int32Value(33);
            assert!(js::rust::wrappers2::CallJitSetterOp(
                &info,
                cx,
                this.handle(),
                0x5678 as *mut c_void,
                1,
                setter_vp.as_mut_ptr(),
            ));
            assert_eq!(JIT_SETTER_VALUE.with(Cell::get), 33);
            let _ = global;
            Observation::ok(Observed::Boolean(true))
        },
    );
}

thread_local! {
    static JIT_SETTER_VALUE: Cell<i32> = const { Cell::new(0) };
}

unsafe extern "C" fn jit_getter(
    _cx: *mut RawJSContext,
    _this: Handle<*mut JSObject>,
    specialized: *mut c_void,
    args: JSJitGetterCallArgs,
) -> bool {
    assert_eq!(specialized as usize, 0x1234);
    args.rval().set(Int32Value(77));
    true
}

unsafe extern "C" fn jit_setter(
    _cx: *mut RawJSContext,
    _this: Handle<*mut JSObject>,
    specialized: *mut c_void,
    args: JSJitSetterCallArgs,
) -> bool {
    assert_eq!(specialized as usize, 0x5678);
    JIT_SETTER_VALUE.with(|cell| cell.set(args.get(0).to_int32()));
    true
}

static SECURITY_CALLBACKS: JSSecurityCallbacks = JSSecurityCallbacks {
    contentSecurityPolicyAllows: Some(content_security_policy_allows),
    codeForEvalGets: None,
    subsumes: None,
};

static RAN_CSP_CALLBACK: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

unsafe extern "C" fn content_security_policy_allows(
    cx: *mut RawJSContext,
    _runtime_code: RuntimeCode,
    _code_string: HandleString,
    _compilation_type: CompilationType,
    parameter_strings: Handle<StackGCVector<*mut JSString>>,
    _body_string: HandleString,
    parameter_args: Handle<StackGCVector<js::jsval::JSVal>>,
    _body_arg: HandleValue,
    can_compile_strings: *mut bool,
) -> bool {
    let mut cx = unsafe { JSContext::from_ptr(NonNull::new(cx).unwrap()) };
    let cx = &mut cx;
    let parameter_strings = js::rust::Handle::from_raw(parameter_strings);
    assert_eq!(parameter_strings.len(), 2);
    assert_eq!(
        string_ptr_of(cx, parameter_strings.at(0).unwrap().get()),
        "a"
    );
    assert_eq!(
        string_ptr_of(cx, parameter_strings.at(1).unwrap().get()),
        "b"
    );
    let parameter_args = js::rust::Handle::from_raw(parameter_args);
    assert_eq!(parameter_args.len(), 2);
    assert_eq!(
        string_of(cx, parameter_args.at(0).unwrap().get()),
        "a"
    );
    assert_eq!(
        string_of(cx, parameter_args.at(1).unwrap().get()),
        "b"
    );
    *RAN_CSP_CALLBACK.lock().unwrap() = true;
    *can_compile_strings = true;
    true
}

fn stack_gc_vectors_and_for_of(engine: &JSEngine) {
    with_global_with_callbacks(engine, |cx, global| unsafe {
        let source = "Function(\"a\", \"b\", \"return a + b\")";
        rooted!(&in(cx) let mut result = UndefinedValue());
        let options = CompileOptionsWrapper::new(cx, c"csp.js".to_owned(), 1);
        assert!(evaluate_script(cx, global, source, result.handle_mut(), options).is_ok());
        assert!(*RAN_CSP_CALLBACK.lock().unwrap());

        let array = eval(cx, global, "[1, 2, 3]");
        rooted!(&in(cx) let array = array.to_object());
        let value = ObjectValue(array.get());
        rooted!(&in(cx) let value = value);
        let mut sum = 0;
        assert!(for_of::<_, _, ()>(cx, value.handle(), |_, item| {
            sum += item.get().to_int32();
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .is_ok());
        assert_eq!(sum, 6);
        rooted!(&in(cx) let not_iterable = Int32Value(1));
        assert!(for_of::<_, _, ()>(cx, not_iterable.handle(), |_, _| {
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .is_err());
        let _ = SetDataPropertyDescriptor;
    });
    crate::support::record(
        "objects.extra_stack_vectors_for_of",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "JS_ForOfIteratorInit",
            "JS_ForOfIteratorNext",
            "JS_ForOfIteratorValueIsIterable",
            "StackGCVectorStringAtIndex",
            "StackGCVectorStringLength",
            "StackGCVectorValueAtIndex",
            "StackGCVectorValueLength",
        ],
    );
}

fn with_global_with_callbacks(
    engine: &JSEngine,
    f: impl FnOnce(&mut JSContext, HandleObject),
) {
    let mut runtime = Runtime::new(engine.handle());
    let cx = runtime.cx();
    unsafe {
        JS_SetSecurityCallbacks(cx, &SECURITY_CALLBACKS);
        rooted!(&in(cx) let global = js::rust::wrappers2::JS_NewGlobalObject(
            cx,
            &SIMPLE_GLOBAL_CLASS,
            ptr::null_mut(),
            js::jsapi::OnNewGlobalHookOption::FireOnNewGlobalHook,
            &*js::rust::RealmOptions::default(),
        ));
        let mut realm = js::realm::AutoRealm::new_from_handle(cx, global.handle());
        f(&mut realm, global.handle());
    }
}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("objects.extra_rooted_vectors", || rooted_vectors(engine));
    crate::support::isolate_case("objects.extra_identity_and_functions", || {
        identity_and_functions(engine)
    });
    crate::support::isolate_case("objects.extra_collections", || collections(engine));
    crate::support::isolate_case("objects.extra_descriptors_and_forwarding", || {
        descriptors_and_forwarding(engine)
    });
    crate::support::isolate_case("objects.extra_jit_accessors", || {
        jit_getters_and_setters(engine)
    });
    crate::support::isolate_case("objects.extra_stack_vectors_for_of", || {
        stack_gc_vectors_and_for_of(engine)
    });
}
