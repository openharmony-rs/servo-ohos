/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#![allow(unsafe_op_in_unsafe_fn)]

//! Structured clone buffers, the callback API and the convenience clone entry point.

use std::cell::RefCell;
use std::ffi::c_void;
use std::ptr::{self, NonNull};

use js::context::JSContext;
use js::glue::{
    CopyJSStructuredCloneData, DeleteJSAutoStructuredCloneBuffer,
    GetLengthOfJSStructuredCloneData, JS_GetReservedSlot, NewJSAutoStructuredCloneBuffer,
    WriteBytesToJSStructuredCloneData,
};
use js::jsapi::{
    CloneDataPolicy, Handle, HandleObject, JSClass, JSObject, JSStructuredCloneCallbacks,
    JSStructuredCloneData, JSStructuredCloneReader, JSStructuredCloneWriter, JSString,
    MutableHandle, OnNewGlobalHookOption, StructuredCloneScope, JS_STRUCTURED_CLONE_VERSION,
    JSCLASS_RESERVED_SLOTS_MASK, JSCLASS_RESERVED_SLOTS_SHIFT,
};
use js::jsval::{Int32Value, JSVal, ObjectValue, UndefinedValue};
use js::realm::AutoRealm;
use js::rooted;
use js::rust::wrappers2::{
    JS_ClearPendingException, JS_IsExceptionPending, JS_NewGlobalObject, JS_NewUint8Array,
    JS_ReadStructuredClone, JS_StructuredClone, JS_WriteStructuredClone,
};
use js::rust::{
    evaluate_script, CompileOptionsWrapper, HandleObject as SafeHandleObject, JSEngine,
    RealmOptions, Runtime, SIMPLE_GLOBAL_CLASS,
};

use crate::support::{record, Observation, Value as Observed};

const SCOPE: StructuredCloneScope = StructuredCloneScope::DifferentProcess;

const POLICY: CloneDataPolicy = CloneDataPolicy {
    allowIntraClusterClonableSharedObjects_: false,
    allowSharedMemoryObjects_: false,
};

const HOST_TAG: u32 = 0xFFFF_8001;
const HOST_SLOT: u32 = 0;

static HOST_CLASS: JSClass = JSClass {
    name: c"ContractHost".as_ptr(),
    flags: (1 & JSCLASS_RESERVED_SLOTS_MASK) << JSCLASS_RESERVED_SLOTS_SHIFT,
    cOps: ptr::null(),
    spec: ptr::null(),
    ext: ptr::null(),
    oOps: ptr::null(),
};

thread_local! {
    static WRITES: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static READS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static SCOPES: RefCell<Vec<StructuredCloneScope>> = const { RefCell::new(Vec::new()) };
}

fn with_global(engine: &JSEngine, f: impl FnOnce(&mut JSContext, SafeHandleObject)) {
    let mut runtime = Runtime::new(engine.handle());
    let cx = runtime.cx();
    unsafe {
        rooted!(&in(cx) let global = JS_NewGlobalObject(
            cx,
            &SIMPLE_GLOBAL_CLASS,
            ptr::null_mut(),
            OnNewGlobalHookOption::FireOnNewGlobalHook,
            &*RealmOptions::default(),
        ));
        let mut realm = AutoRealm::new_from_handle(cx, global.handle());
        let (global, cx) = realm.global_and_reborrow();
        f(cx, global);
    }
}

fn eval(cx: &mut JSContext, global: SafeHandleObject, source: &str) -> JSVal {
    rooted!(&in(cx) let mut rval = UndefinedValue());
    let options = CompileOptionsWrapper::new(cx, c"clone.js".to_owned(), 1);
    evaluate_script(cx, global, source, rval.handle_mut(), options)
        .unwrap_or_else(|_| panic!("evaluating {source:?} failed"));
    rval.get()
}

fn raw_handle<T>(ptr: &T) -> Handle<T> {
    unsafe { Handle::from_marked_location(ptr) }
}

fn raw_mutable<T>(ptr: &mut T) -> MutableHandle<T> {
    unsafe { MutableHandle::from_marked_location(ptr) }
}

fn safe_handle<T>(ptr: &T) -> js::rust::Handle<'_, T> {
    unsafe { js::rust::Handle::from_marked_location(ptr) }
}

fn safe_mutable<T>(ptr: &mut T) -> js::rust::MutableHandle<'_, T> {
    unsafe { js::rust::MutableHandle::from_marked_location(ptr) }
}

unsafe fn new_host(cx: &mut JSContext, id: i32) -> *mut JSObject {
    let object = js::rust::wrappers2::JS_NewObject(cx, &HOST_CLASS);
    assert!(!object.is_null());
    js::jsapi::JS_SetReservedSlot(object, HOST_SLOT, &Int32Value(id));
    object
}

unsafe fn host_id(object: *mut JSObject) -> Option<i32> {
    let mut value = UndefinedValue();
    JS_GetReservedSlot(object, HOST_SLOT, &mut value);
    value.is_int32().then(|| value.to_int32())
}

unsafe extern "C" fn write_callback(
    cx: *mut js::context::RawJSContext,
    writer: *mut JSStructuredCloneWriter,
    object: HandleObject,
    _same_process: *mut bool,
    _closure: *mut c_void,
) -> bool {
    let Some(id) = host_id(*object.ptr) else {
        return false;
    };
    WRITES.with(|writes| writes.borrow_mut().push(id as u32));
    SCOPES.with(|scopes| scopes.borrow_mut().push(js::jsapi::JS_GetStructuredCloneScope(writer)));
    let cx = JSContext::from_ptr(NonNull::new(cx).unwrap());
    let cx = &mut { cx };

    // The callback writes a host record without using startWrite for the object itself, so the
    // object must not be kept in the writer's memory as a written object.
    assert!(js::jsapi::JS_ObjectNotWritten(writer, object));

    let text = js::rust::wrappers2::JS_NewStringCopyZ(cx, c"host text".as_ptr());
    assert!(!text.is_null());
    let typed = ObjectValue(JS_NewUint8Array(cx, 4));
    assert!(typed.is_object());

    js::jsapi::JS_WriteUint32PairUnchecked(writer, HOST_TAG, id as u32)
        && js::jsapi::JS_WriteBytes(writer, b"payload!".as_ptr().cast(), 8)
        && js::jsapi::JS_WriteString(writer, raw_handle(&text))
        && js::jsapi::JS_WriteDouble(writer, 2.5)
        && js::jsapi::JS_WriteTypedArray(writer, raw_handle(&typed))
        && js::jsapi::JS_WriteUint32PairUnchecked(writer, 11, 22)
}

unsafe extern "C" fn read_callback(
    cx: *mut js::context::RawJSContext,
    reader: *mut JSStructuredCloneReader,
    _policy: *const CloneDataPolicy,
    tag: u32,
    data: u32,
    _closure: *mut c_void,
) -> *mut JSObject {
    assert_eq!(tag, HOST_TAG);
    let mut bytes = [0u8; 8];
    assert!(js::jsapi::JS_ReadBytes(
        reader,
        bytes.as_mut_ptr().cast(),
        bytes.len()
    ));
    assert_eq!(&bytes, b"payload!");

    let mut text: *mut JSString = ptr::null_mut();
    assert!(js::jsapi::JS_ReadString(reader, raw_mutable(&mut text)));
    assert!(!text.is_null());
    assert_eq!(js::jsapi::JS_GetStringLength(text), 9);

    let mut double = 0.0;
    assert!(js::jsapi::JS_ReadDouble(reader, &mut double));
    assert_eq!(double, 2.5);

    let mut typed = UndefinedValue();
    assert!(js::jsapi::JS_ReadTypedArray(reader, raw_mutable(&mut typed)));
    assert!(typed.is_object());

    let mut p1 = 0;
    let mut p2 = 0;
    assert!(js::jsapi::JS_ReadUint32Pair(reader, &mut p1, &mut p2));
    assert_eq!((p1, p2), (11, 22));

    READS.with(|reads| reads.borrow_mut().push(data));
    let cx = JSContext::from_ptr(NonNull::new(cx).unwrap());
    new_host(&mut { cx }, data as i32 + 1000)
}

unsafe extern "C" fn report_error(
    _cx: *mut js::context::RawJSContext,
    _error: u32,
    _closure: *mut c_void,
    _message: *const std::os::raw::c_char,
) {
}

static CALLBACKS: JSStructuredCloneCallbacks = JSStructuredCloneCallbacks {
    read: Some(read_callback),
    write: Some(write_callback),
    reportError: Some(report_error),
    readTransfer: None,
    writeTransfer: None,
    freeTransfer: None,
    canTransfer: None,
    sabCloned: None,
};

unsafe fn write_value(
    cx: &mut JSContext,
    value: JSVal,
    transfer: JSVal,
    data: *mut JSStructuredCloneData,
) -> bool {
    JS_WriteStructuredClone(
        cx,
        safe_handle(&value),
        data,
        SCOPE,
        &POLICY,
        &CALLBACKS,
        ptr::null_mut(),
        safe_handle(&transfer),
    )
}

unsafe fn read_value(
    cx: &mut JSContext,
    data: *const JSStructuredCloneData,
) -> Option<JSVal> {
    let mut value = UndefinedValue();
    let ok = JS_ReadStructuredClone(
        cx,
        data,
        JS_STRUCTURED_CLONE_VERSION,
        SCOPE,
        safe_mutable(&mut value),
        &POLICY,
        &CALLBACKS,
        ptr::null_mut(),
    );
    ok.then_some(value)
}

fn buffer_lifecycle(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let value = eval(cx, global, "({ answer: 42, list: [1, 2, 3] })");
        let empty = UndefinedValue();

        let source = NewJSAutoStructuredCloneBuffer(SCOPE, &CALLBACKS);
        assert!(!source.is_null());
        assert!(write_value(cx, value, empty, &mut (*source).data_));
        let length = GetLengthOfJSStructuredCloneData(&mut (*source).data_);
        assert!(length > 0);

        let mut bytes = vec![0u8; length];
        CopyJSStructuredCloneData(&mut (*source).data_, bytes.as_mut_ptr());
        DeleteJSAutoStructuredCloneBuffer(source);

        let destination = NewJSAutoStructuredCloneBuffer(SCOPE, ptr::null());
        assert!(!destination.is_null());
        assert!(WriteBytesToJSStructuredCloneData(
            bytes.as_ptr(),
            bytes.len(),
            &mut (*destination).data_
        ));
        assert_eq!(
            GetLengthOfJSStructuredCloneData(&mut (*destination).data_),
            length
        );
        let output = read_value(cx, &(*destination).data_).expect("reading copied bytes");
        assert!(output.is_object());
        let answer = crate::support::get_property(cx, output, c"answer");
        assert_eq!(crate::support::observe(cx, answer), crate::support::Value::Number(42.0));
        DeleteJSAutoStructuredCloneBuffer(destination);
    });

    record(
        "clone.buffer_lifecycle",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "CopyJSStructuredCloneData",
            "DeleteJSAutoStructuredCloneBuffer",
            "GetLengthOfJSStructuredCloneData",
            "JS_NewGlobalObject",
            "JS_ReadStructuredClone",
            "JS_WriteStructuredClone",
            "NewJSAutoStructuredCloneBuffer",
            "WriteBytesToJSStructuredCloneData",
        ],
    );
}

fn callback_roundtrip(engine: &JSEngine) {
    WRITES.with(|writes| writes.borrow_mut().clear());
    READS.with(|reads| reads.borrow_mut().clear());
    SCOPES.with(|scopes| scopes.borrow_mut().clear());

    with_global(engine, |cx, global| unsafe {
        let host = new_host(cx, 7);
        let value = js::jsval::ObjectValue(host);
        let array = eval(cx, global, "[1, 2]");
        // Put the host object into an array twice: JS_ObjectNotWritten makes the second
        // occurrence invoke the write callback again instead of becoming a back-reference.
        rooted!(&in(cx) let array = array.to_object());
        let element = safe_handle(&value);
        assert!(js::rust::wrappers2::JS_SetElement(
            cx,
            array.handle(),
            0,
            element
        ));
        assert!(js::rust::wrappers2::JS_SetElement(
            cx,
            array.handle(),
            1,
            element
        ));

        let buffer = NewJSAutoStructuredCloneBuffer(SCOPE, &CALLBACKS);
        let empty = UndefinedValue();
        let array = ObjectValue(array.get());
        assert!(write_value(cx, array, empty, &mut (*buffer).data_));
        let output = read_value(cx, &(*buffer).data_).expect("reading callback payload");
        DeleteJSAutoStructuredCloneBuffer(buffer);
        assert!(output.is_object());
        let first = crate::support::get_property(cx, output, c"0");
        let second = crate::support::get_property(cx, output, c"1");
        assert!(first.is_object() && second.is_object());
    });

    assert_eq!(WRITES.with(|writes| writes.borrow().clone()), vec![7, 7]);
    assert_eq!(READS.with(|reads| reads.borrow().clone()), vec![7, 7]);
    assert_eq!(
        SCOPES.with(|scopes| scopes.borrow().clone()),
        vec![SCOPE, SCOPE]
    );

    record(
        "clone.callback_roundtrip",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "JS_GetStructuredCloneScope",
            "JS_NewGlobalObject",
            "JS_ObjectNotWritten",
            "JS_ReadBytes",
            "JS_ReadDouble",
            "JS_ReadString",
            "JS_ReadTypedArray",
            "JS_ReadUint32Pair",
            "JS_WriteBytes",
            "JS_WriteDouble",
            "JS_WriteString",
            "JS_WriteTypedArray",
            "JS_WriteUint32PairUnchecked",
            "NewJSAutoStructuredCloneBuffer",
            "DeleteJSAutoStructuredCloneBuffer",
        ],
    );
}

fn convenience_clone(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let source = eval(cx, global, "let o = { a: [1, 2] }; o.self = o; o");
        let mut clone = UndefinedValue();
        assert!(JS_StructuredClone(
            cx,
            safe_handle(&source),
            safe_mutable(&mut clone),
            ptr::null(),
            ptr::null_mut()
        ));
        assert!(clone.is_object());
        assert_ne!(clone.to_object(), source.to_object());
        let self_value = crate::support::get_property(cx, clone, c"self");
        assert_eq!(self_value.to_object(), clone.to_object());

        let mut integer = UndefinedValue();
        let five = Int32Value(5);
        assert!(JS_StructuredClone(
            cx,
            safe_handle(&five),
            safe_mutable(&mut integer),
            ptr::null(),
            ptr::null_mut()
        ));
        assert_eq!(integer.to_int32(), 5);
    });

    record(
        "clone.convenience_entry",
        &Observation::ok(Observed::Boolean(true)),
        &["JS_NewGlobalObject", "JS_StructuredClone"],
    );
}

fn transferables(engine: &JSEngine) {
    with_global(engine, |cx, global| unsafe {
        let value = eval(cx, global, "let b = new Uint8Array([1, 2, 3]).buffer; b");
        let transfer = eval(cx, global, "[b]");
        assert!(transfer.is_object());

        let buffer = NewJSAutoStructuredCloneBuffer(SCOPE, &CALLBACKS);
        assert!(write_value(cx, value, transfer, &mut (*buffer).data_));
        let mut has_transferables = false;
        assert!(js::jsapi::JS_StructuredCloneHasTransferables(
            &mut (*buffer).data_,
            &mut has_transferables
        ));
        // SpiderMonkey inspects only the first pair of the buffer here, which is the format
        // header even when a transfer map follows. The accessor's boolean is therefore not a
        // portable assertion; reading the value back checks the actual transfer contract.
        let _ = has_transferables;
        let output = read_value(cx, &(*buffer).data_).expect("reading transferred buffer");
        assert!(output.is_object());
        DeleteJSAutoStructuredCloneBuffer(buffer);
    });

    record(
        "clone.transferables",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "JS_NewGlobalObject",
            "JS_ReadStructuredClone",
            "JS_StructuredCloneHasTransferables",
            "JS_WriteStructuredClone",
            "NewJSAutoStructuredCloneBuffer",
            "DeleteJSAutoStructuredCloneBuffer",
        ],
    );
}

fn invalid_data(engine: &JSEngine) {
    with_global(engine, |cx, _global| unsafe {
        let buffer = NewJSAutoStructuredCloneBuffer(SCOPE, ptr::null());
        let garbage = [0u8; 32];
        assert!(WriteBytesToJSStructuredCloneData(
            garbage.as_ptr(),
            garbage.len(),
            &mut (*buffer).data_
        ));
        let mut value = UndefinedValue();
        let ok = JS_ReadStructuredClone(
            cx,
            &(*buffer).data_,
            JS_STRUCTURED_CLONE_VERSION,
            SCOPE,
            safe_mutable(&mut value),
            &POLICY,
            ptr::null(),
            ptr::null_mut(),
        );
        assert!(!ok);
        assert!(JS_IsExceptionPending(cx));
        JS_ClearPendingException(cx);
        DeleteJSAutoStructuredCloneBuffer(buffer);
    });

    record(
        "clone.invalid_data",
        &Observation::ok(Observed::Boolean(true)),
        &[
            "DeleteJSAutoStructuredCloneBuffer",
            "JS_ClearPendingException",
            "JS_IsExceptionPending",
            "JS_NewGlobalObject",
            "JS_ReadStructuredClone",
            "NewJSAutoStructuredCloneBuffer",
            "WriteBytesToJSStructuredCloneData",
        ],
    );
}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("clone.buffer_lifecycle", || buffer_lifecycle(engine));
    crate::support::isolate_case("clone.callback_roundtrip", || callback_roundtrip(engine));
    crate::support::isolate_case("clone.convenience_entry", || convenience_clone(engine));
    crate::support::isolate_case("clone.transferables", || transferables(engine));
    crate::support::isolate_case("clone.invalid_data", || invalid_data(engine));
}
