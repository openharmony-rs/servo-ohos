/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! ArrayBuffers, typed arrays and DataViews.

#![allow(unsafe_op_in_unsafe_fn)]

use std::ffi::{c_void, CStr};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use js::context::JSContext;
use js::glue::{
    GetFloat32ArrayLengthAndData, GetFloat64ArrayLengthAndData, GetInt16ArrayLengthAndData,
    GetInt32ArrayLengthAndData, GetInt8ArrayLengthAndData, GetUint16ArrayLengthAndData,
    GetUint32ArrayLengthAndData, GetUint8ArrayLengthAndData, GetUint8ClampedArrayLengthAndData,
};
use js::jsapi::{
    ArrayBufferHasData, GetArrayBufferByteLength, GetArrayBufferData, GetArrayBufferLengthAndData,
    GetArrayBufferMaybeSharedData, GetArrayBufferMaybeSharedLengthAndData,
    GetArrayBufferViewLengthAndData, GetObjectAsArrayBuffer, GetSharedArrayBufferByteLength,
    GetSharedArrayBufferData, GetSharedArrayBufferLengthAndData, IsArrayBufferObject,
    IsArrayBufferObjectMaybeShared, IsArrayBufferViewShared, IsDetachedArrayBufferObject,
    IsResizableArrayBufferMaybeShared, IsResizableArrayBufferView, IsSharedArrayBufferObject,
    JSObject, JS_GetArrayBufferViewByteLength,
    JS_GetArrayBufferViewByteOffset, JS_GetArrayBufferViewData, JS_GetArrayBufferViewFixedData,
    JS_GetArrayBufferViewType, JS_GetBigInt64ArrayData, JS_GetBigUint64ArrayData,
    JS_GetFloat16ArrayData,
    JS_GetFloat32ArrayData, JS_GetFloat64ArrayData, JS_GetInt16ArrayData, JS_GetInt32ArrayData,
    JS_GetInt8ArrayData, JS_GetObjectAsArrayBufferView, JS_GetObjectAsBigInt64Array,
    JS_GetObjectAsBigUint64Array, JS_GetObjectAsFloat16Array, JS_GetObjectAsFloat32Array,
    JS_GetObjectAsFloat64Array, JS_GetObjectAsInt16Array, JS_GetObjectAsInt32Array,
    JS_GetObjectAsInt8Array, JS_GetObjectAsUint16Array, JS_GetObjectAsUint32Array,
    JS_GetObjectAsUint8Array, JS_GetObjectAsUint8ClampedArray, JS_GetTypedArrayByteLength,
    JS_GetTypedArrayByteOffset, JS_GetTypedArrayLength, JS_GetTypedArraySharedness,
    JS_GetUint16ArrayData, JS_GetUint32ArrayData, JS_GetUint8ArrayData,
    JS_GetUint8ClampedArrayData, JS_IsArrayBufferViewObject, JS_IsTypedArrayObject,
    NewSharedArrayBuffer, OnNewGlobalHookOption, Type, UnwrapArrayBuffer,
    UnwrapArrayBufferMaybeShared, UnwrapArrayBufferView, UnwrapBigInt64Array, UnwrapBigUint64Array,
    UnwrapFloat16Array, UnwrapFloat32Array, UnwrapFloat64Array, UnwrapInt16Array, UnwrapInt32Array,
    UnwrapInt8Array, UnwrapSharedArrayBuffer, UnwrapUint16Array, UnwrapUint32Array,
    UnwrapUint8Array, UnwrapUint8ClampedArray,
};
use js::jsval::{JSVal, ObjectValue, UndefinedValue};
use js::realm::AutoRealm;
use js::rooted;
use js::rust::wrappers2::{
    ArrayBufferClone, ArrayBufferCopyData, CopyArrayBuffer, DetachArrayBuffer,
    HasDefinedArrayBufferDetachKey, JS_ClearPendingException, JS_GetArrayBufferViewBuffer,
    JS_IsExceptionPending, JS_NewBigInt64Array, JS_NewBigInt64ArrayFromArray,
    JS_NewBigInt64ArrayWithBuffer, JS_NewBigUint64Array, JS_NewBigUint64ArrayFromArray,
    JS_NewBigUint64ArrayWithBuffer, JS_NewDataView, JS_NewFloat16Array,
    JS_NewFloat16ArrayFromArray, JS_NewFloat16ArrayWithBuffer, JS_NewFloat32Array,
    JS_NewFloat32ArrayFromArray, JS_NewFloat32ArrayWithBuffer, JS_NewFloat64Array,
    JS_NewFloat64ArrayFromArray, JS_NewFloat64ArrayWithBuffer, JS_NewGlobalObject,
    JS_NewInt16Array, JS_NewInt16ArrayFromArray, JS_NewInt16ArrayWithBuffer, JS_NewInt32Array,
    JS_NewInt32ArrayFromArray, JS_NewInt32ArrayWithBuffer, JS_NewInt8Array,
    JS_NewInt8ArrayFromArray, JS_NewInt8ArrayWithBuffer, JS_NewUint16Array,
    JS_NewUint16ArrayFromArray, JS_NewUint16ArrayWithBuffer, JS_NewUint32Array,
    JS_NewUint32ArrayFromArray, JS_NewUint32ArrayWithBuffer, JS_NewUint8Array,
    JS_NewUint8ArrayFromArray, JS_NewUint8ArrayWithBuffer, JS_NewUint8ClampedArray,
    JS_NewUint8ClampedArrayFromArray, JS_NewUint8ClampedArrayWithBuffer, JS_SetProperty,
    NewArrayBuffer, NewArrayBufferWithContents, NewArrayBufferWithUserOwnedContents,
    NewExternalArrayBuffer, StealArrayBufferContents,
};
use js::rust::{
    evaluate_script, CompileOptionsWrapper, HandleObject, JSEngine, RealmOptions, Runtime,
    SIMPLE_GLOBAL_CLASS,
};
use js::typedarray::{CreateWith, Int16Array, Uint8Array};

/// Runs `f` in a new runtime and global; the runtime is destroyed (finalizing everything) after.
fn with_global(engine: &JSEngine, f: impl FnOnce(&mut JSContext, HandleObject)) {
    let mut runtime = Runtime::new(engine.handle());
    let context = runtime.cx();
    rooted!(&in(context) let global = unsafe {
        JS_NewGlobalObject(
            context,
            &SIMPLE_GLOBAL_CLASS,
            ptr::null_mut(),
            OnNewGlobalHookOption::FireOnNewGlobalHook,
            &*RealmOptions::default(),
        )
    });
    let mut realm = AutoRealm::new_from_handle(context, global.handle());
    f(&mut realm, global.handle());
}

/// Evaluates `src`; the result is kept alive by the shim's pool until the runtime is destroyed.
fn eval(cx: &mut JSContext, global: HandleObject, src: &str) -> JSVal {
    rooted!(&in(cx) let mut rval = UndefinedValue());
    let options = CompileOptionsWrapper::new(cx, c"qjs_buffers".to_owned(), 1);
    assert!(
        evaluate_script(cx, global, src, rval.handle_mut(), options).is_ok(),
        "{src}"
    );
    rval.get()
}

fn eval_obj(cx: &mut JSContext, global: HandleObject, src: &str) -> *mut JSObject {
    let v = eval(cx, global, src);
    assert!(v.is_object(), "{src}");
    v.to_object()
}

#[track_caller]
fn assert_js(cx: &mut JSContext, global: HandleObject, src: &str) {
    let v = eval(cx, global, src);
    assert!(v.is_boolean() && v.to_boolean(), "{src}");
}

fn set_global(cx: &mut JSContext, global: HandleObject, name: &CStr, obj: *mut JSObject) {
    rooted!(&in(cx) let v = ObjectValue(obj));
    assert!(unsafe { JS_SetProperty(cx, global, name.as_ptr(), v.handle()) });
}

#[track_caller]
fn take_exception(cx: &mut JSContext) {
    assert!(unsafe { JS_IsExceptionPending(cx) });
    unsafe { JS_ClearPendingException(cx) };
}

macro_rules! check_type {
    ($cx:expr, $global:expr, $new:ident, $from_array:ident, $with_buffer:ident, $unwrap:ident,
     $ty:expr, $ctor:literal, $size:expr, $init:literal) => {{
        let (cx, global): (&mut JSContext, HandleObject) = (&mut *$cx, $global);
        rooted!(&in(cx) let a = unsafe { $new(cx, 4) });
        assert!(!a.get().is_null());
        unsafe {
            assert!(JS_IsTypedArrayObject(a.get()));
            assert!(JS_IsArrayBufferViewObject(a.get()));
            assert!(!IsArrayBufferObject(a.get()));
            assert_eq!(JS_GetArrayBufferViewType(a.get()), $ty);
            assert_eq!(JS_GetTypedArrayLength(a.get()), 4);
            assert_eq!(JS_GetTypedArrayByteLength(a.get()), 4 * $size);
            assert!(!JS_GetTypedArraySharedness(a.get()));
            assert_eq!($unwrap(a.get()), a.get());
            assert_eq!(UnwrapArrayBufferView(a.get()), a.get());
            if $ty != Type::Int8 {
                assert!(UnwrapInt8Array(a.get()).is_null());
            }
        }
        set_global(cx, global, c"a", a.get());
        assert_js(cx, global, concat!("a instanceof ", $ctor, " && a.length === 4"));

        rooted!(&in(cx) let src = eval_obj(cx, global, $init));
        rooted!(&in(cx) let b = unsafe { $from_array(cx, src.handle()) });
        assert!(!b.get().is_null());
        assert_eq!(unsafe { JS_GetTypedArrayLength(b.get()) }, 3);
        set_global(cx, global, c"b", b.get());
        assert_js(cx, global, concat!("b instanceof ", $ctor, " && b[2] == 3"));

        rooted!(&in(cx) let buf = unsafe { NewArrayBuffer(cx, 8 * $size) });
        rooted!(&in(cx) let c = unsafe { $with_buffer(cx, buf.handle(), 2 * $size, -1) });
        assert!(!c.get().is_null());
        unsafe {
            assert_eq!(JS_GetTypedArrayLength(c.get()), 6);
            assert_eq!(JS_GetArrayBufferViewByteOffset(c.get()), 2 * $size);
        }
        rooted!(&in(cx) let d = unsafe { $with_buffer(cx, buf.handle(), 0, 3) });
        assert_eq!(unsafe { JS_GetTypedArrayLength(d.get()) }, 3);
    }};
}

fn each_type(cx: &mut JSContext, global: HandleObject) {
    check_type!(
        cx,
        global,
        JS_NewInt8Array,
        JS_NewInt8ArrayFromArray,
        JS_NewInt8ArrayWithBuffer,
        UnwrapInt8Array,
        Type::Int8,
        "Int8Array",
        1,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewUint8Array,
        JS_NewUint8ArrayFromArray,
        JS_NewUint8ArrayWithBuffer,
        UnwrapUint8Array,
        Type::Uint8,
        "Uint8Array",
        1,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewUint8ClampedArray,
        JS_NewUint8ClampedArrayFromArray,
        JS_NewUint8ClampedArrayWithBuffer,
        UnwrapUint8ClampedArray,
        Type::Uint8Clamped,
        "Uint8ClampedArray",
        1,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewInt16Array,
        JS_NewInt16ArrayFromArray,
        JS_NewInt16ArrayWithBuffer,
        UnwrapInt16Array,
        Type::Int16,
        "Int16Array",
        2,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewUint16Array,
        JS_NewUint16ArrayFromArray,
        JS_NewUint16ArrayWithBuffer,
        UnwrapUint16Array,
        Type::Uint16,
        "Uint16Array",
        2,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewInt32Array,
        JS_NewInt32ArrayFromArray,
        JS_NewInt32ArrayWithBuffer,
        UnwrapInt32Array,
        Type::Int32,
        "Int32Array",
        4,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewUint32Array,
        JS_NewUint32ArrayFromArray,
        JS_NewUint32ArrayWithBuffer,
        UnwrapUint32Array,
        Type::Uint32,
        "Uint32Array",
        4,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewFloat16Array,
        JS_NewFloat16ArrayFromArray,
        JS_NewFloat16ArrayWithBuffer,
        UnwrapFloat16Array,
        Type::Float16,
        "Float16Array",
        2,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewFloat32Array,
        JS_NewFloat32ArrayFromArray,
        JS_NewFloat32ArrayWithBuffer,
        UnwrapFloat32Array,
        Type::Float32,
        "Float32Array",
        4,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewFloat64Array,
        JS_NewFloat64ArrayFromArray,
        JS_NewFloat64ArrayWithBuffer,
        UnwrapFloat64Array,
        Type::Float64,
        "Float64Array",
        8,
        "[1, 2, 3]"
    );
    check_type!(
        cx,
        global,
        JS_NewBigInt64Array,
        JS_NewBigInt64ArrayFromArray,
        JS_NewBigInt64ArrayWithBuffer,
        UnwrapBigInt64Array,
        Type::BigInt64,
        "BigInt64Array",
        8,
        "[1n, 2n, 3n]"
    );
    check_type!(
        cx,
        global,
        JS_NewBigUint64Array,
        JS_NewBigUint64ArrayFromArray,
        JS_NewBigUint64ArrayWithBuffer,
        UnwrapBigUint64Array,
        Type::BigUint64,
        "BigUint64Array",
        8,
        "[1n, 2n, 3n]"
    );

    // From another typed array, and from an iterable.
    rooted!(&in(cx) let src = eval_obj(cx, global, "new Uint8Array([7, 8])"));
    rooted!(&in(cx) let a = unsafe { JS_NewInt32ArrayFromArray(cx, src.handle()) });
    set_global(cx, global, c"a", a.get());
    assert_js(cx, global, "a instanceof Int32Array && a.join() === '7,8'");
    rooted!(&in(cx) let src = eval_obj(cx, global, "new Set([4, 5, 6])"));
    rooted!(&in(cx) let a = unsafe { JS_NewUint16ArrayFromArray(cx, src.handle()) });
    set_global(cx, global, c"a", a.get());
    assert_js(cx, global, "a.join() === '4,5,6'");

    // Plain objects and buffers are neither typed arrays nor views.
    rooted!(&in(cx) let o = eval_obj(cx, global, "({ length: 2 })"));
    rooted!(&in(cx) let buf = unsafe { NewArrayBuffer(cx, 4) });
    unsafe {
        assert!(!JS_IsArrayBufferViewObject(o.get()));
        assert!(!JS_IsTypedArrayObject(buf.get()));
        assert!(UnwrapArrayBufferView(buf.get()).is_null());
        assert!(UnwrapArrayBuffer(o.get()).is_null());
        assert_eq!(UnwrapArrayBuffer(buf.get()), buf.get());
        // JS_GetArrayBufferViewType has a checked contract: its input must be a view.
        // Passing an ArrayBuffer asserts in SpiderMonkey, so the DataView case below covers it.
    }

    // Creating a view over something that is not a buffer, or out of bounds, throws.
    rooted!(&in(cx) let bad = unsafe { JS_NewUint8ArrayWithBuffer(cx, o.handle(), 0, -1) });
    assert!(bad.get().is_null());
    take_exception(cx);
    rooted!(&in(cx) let bad = unsafe { JS_NewUint32ArrayWithBuffer(cx, buf.handle(), 1, -1) });
    assert!(bad.get().is_null());
    take_exception(cx);
    rooted!(&in(cx) let bad = unsafe { JS_NewUint8ArrayWithBuffer(cx, buf.handle(), 2, 3) });
    assert!(bad.get().is_null());
    take_exception(cx);
}

fn data_pointers(cx: &mut JSContext, global: HandleObject) {
    // Rust writes are visible to script.
    rooted!(&in(cx) let a = unsafe { JS_NewUint8Array(cx, 4) });
    let mut shared = true;
    let data = unsafe { JS_GetUint8ArrayData(a.get(), &mut shared, ptr::null()) };
    assert!(!shared);
    unsafe { ptr::copy_nonoverlapping([1u8, 2, 3, 4].as_ptr(), data, 4) };
    set_global(cx, global, c"a", a.get());
    assert_js(cx, global, "a.join() === '1,2,3,4'");

    // Script writes are visible to Rust, through the same pointer.
    eval(cx, global, "a[3] = 200");
    assert_eq!(unsafe { *data.add(3) }, 200);

    rooted!(&in(cx) let f = eval_obj(cx, global, "var f = new Float64Array([1.5, -2]); f"));
    let (mut len, mut data) = (0, ptr::null_mut());
    let obj = unsafe { JS_GetObjectAsFloat64Array(f.get(), &mut len, &mut shared, &mut data) };
    assert_eq!(obj, f.get());
    assert_eq!(len, 2);
    assert_eq!(
        unsafe { std::slice::from_raw_parts(data, len) },
        &[1.5, -2.0]
    );
    unsafe { *data.add(1) = 8.25 };
    assert_js(cx, global, "f[1] === 8.25");

    rooted!(&in(cx) let b = unsafe { JS_NewBigInt64Array(cx, 2) });
    unsafe { *JS_GetBigInt64ArrayData(b.get(), &mut shared, ptr::null()) = i64::MIN };
    set_global(cx, global, c"b", b.get());
    assert_js(cx, global, "b[0] === -(2n ** 63n) && b[1] === 0n");

    rooted!(&in(cx) let h = unsafe { JS_NewFloat16Array(cx, 1) });
    unsafe { *JS_GetFloat16ArrayData(h.get(), &mut shared, ptr::null()) = 0x3c00 };
    set_global(cx, global, c"h", h.get());
    assert_js(cx, global, "h[0] === 1");

    // Through the typed array wrappers of the mozjs crate.
    rooted!(&in(cx) let mut t = ptr::null_mut::<JSObject>());
    unsafe { Int16Array::create(cx, CreateWith::Slice(&[1, -2, -3]), t.handle_mut()) }.unwrap();
    set_global(cx, global, c"t", t.get());
    assert_js(cx, global, "t instanceof Int16Array && t[2] === -3");

    // ArrayBuffer storage.
    rooted!(&in(cx) let buf = unsafe { NewArrayBuffer(cx, 8) });
    let data = unsafe { GetArrayBufferData(buf.get(), &mut shared, ptr::null()) };
    assert!(!data.is_null() && !shared);
    unsafe { *data.add(7) = 9 };
    set_global(cx, global, c"buf", buf.get());
    assert_js(
        cx,
        global,
        "buf.byteLength === 8 && new Uint8Array(buf)[7] === 9",
    );
    let (mut len, mut data2) = (0, ptr::null_mut());
    unsafe { GetArrayBufferLengthAndData(buf.get(), &mut len, &mut shared, &mut data2) };
    assert_eq!((len, data2), (8, data));

    // Zero-length buffers and arrays have data (only detached ones have none).
    rooted!(&in(cx) let empty = unsafe { JS_NewUint8Array(cx, 0) });
    let empty = Uint8Array::from(empty.get()).unwrap();
    assert_eq!(empty.to_vec(), Some(vec![]));
}

fn views_with_offsets(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let buf = eval_obj(cx, global, "var buf = new ArrayBuffer(16); new Uint8Array(buf).forEach((_, i, a) => a[i] = i); buf"));
    let mut shared = false;
    let base = unsafe { GetArrayBufferData(buf.get(), &mut shared, ptr::null()) };

    rooted!(&in(cx) let v = eval_obj(cx, global, "new Uint16Array(buf, 4, 3)"));
    unsafe {
        assert_eq!(JS_GetArrayBufferViewByteOffset(v.get()), 4);
        assert_eq!(JS_GetArrayBufferViewByteLength(v.get()), 6);
        assert_eq!(JS_GetTypedArrayLength(v.get()), 3);
        let (mut len, mut data) = (0, ptr::null_mut());
        GetArrayBufferViewLengthAndData(v.get(), &mut len, &mut shared, &mut data);
        assert_eq!((len, data), (6, base.add(4)));
    }
    rooted!(&in(cx) let vbuf = unsafe { js::rust::wrappers2::JS_GetArrayBufferViewBuffer(cx, v.handle(), &mut shared) });
    assert_eq!(vbuf.get(), buf.get());
    assert!(!shared);

    rooted!(&in(cx) let s = eval_obj(cx, global, "new Int32Array(buf).subarray(1, 3)"));
    unsafe {
        assert_eq!(JS_GetArrayBufferViewByteOffset(s.get()), 4);
        assert_eq!(JS_GetTypedArrayLength(s.get()), 2);
    }

    // DataViews are views but not typed arrays.
    rooted!(&in(cx) let dv = unsafe { JS_NewDataView(cx, buf.handle(), 2, 4) });
    assert!(!dv.get().is_null());
    unsafe {
        assert!(JS_IsArrayBufferViewObject(dv.get()));
        assert!(!JS_IsTypedArrayObject(dv.get()));
        assert_eq!(
            JS_GetArrayBufferViewType(dv.get()),
            Type::MaxTypedArrayViewType
        );
        assert_eq!(JS_GetArrayBufferViewByteOffset(dv.get()), 2);
        assert_eq!(JS_GetArrayBufferViewByteLength(dv.get()), 4);
        let (mut len, mut data) = (0, ptr::null_mut());
        GetArrayBufferViewLengthAndData(dv.get(), &mut len, &mut shared, &mut data);
        assert_eq!((len, data), (4, base.add(2)));
        *data.add(1) = 0xaa;
    }
    set_global(cx, global, c"dv", dv.get());
    assert_js(cx, global, "dv instanceof DataView && dv.getUint8(0) === 2 && dv.getUint8(1) === 0xaa && new Uint8Array(buf)[3] === 0xaa");

    rooted!(&in(cx) let dv2 = eval_obj(cx, global, "new DataView(buf, 12)"));
    unsafe {
        assert_eq!(JS_GetArrayBufferViewByteOffset(dv2.get()), 12);
        assert_eq!(JS_GetArrayBufferViewByteLength(dv2.get()), 4);
    }
    rooted!(&in(cx) let bad = unsafe { JS_NewDataView(cx, buf.handle(), 14, 4) });
    assert!(bad.get().is_null());
    take_exception(cx);

    // Views of resizable buffers track the length, and have no data when out of bounds.
    rooted!(&in(cx) let rv = eval_obj(cx, global, "var r = new ArrayBuffer(8, { maxByteLength: 16 }); var rdv = new DataView(r, 2); var rv = new Uint16Array(r, 2); r.resize(12); rv"));
    rooted!(&in(cx) let rdv = eval_obj(cx, global, "rdv"));
    unsafe {
        assert!(IsResizableArrayBufferView(rv.get()));
        assert!(!IsResizableArrayBufferView(v.get()));
        assert_eq!(JS_GetTypedArrayLength(rv.get()), 5);
        assert_eq!(JS_GetArrayBufferViewByteLength(rdv.get()), 10);
    }
    eval(cx, global, "r.resize(1)");
    unsafe {
        assert_eq!(JS_GetTypedArrayLength(rv.get()), 0);
        assert_eq!(JS_GetArrayBufferViewByteLength(rdv.get()), 0);
        let (mut len, mut data) = (1, ptr::null_mut());
        GetArrayBufferViewLengthAndData(rv.get(), &mut len, &mut shared, &mut data);
        assert_eq!(len, 0);
    }
}

fn detach(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let d = eval_obj(cx, global, "var d = new ArrayBuffer(8); var dv = new Uint8Array(d, 2); d"));
    rooted!(&in(cx) let view = eval_obj(cx, global, "dv"));
    let mut defined = true;
    assert!(unsafe { HasDefinedArrayBufferDetachKey(cx, d.handle(), &mut defined) });
    assert!(!defined);
    assert!(unsafe { ArrayBufferHasData(d.get()) });

    assert!(unsafe { DetachArrayBuffer(cx, d.handle()) });
    unsafe {
        assert!(IsDetachedArrayBufferObject(d.get()));
        assert!(!ArrayBufferHasData(d.get()));
        assert_eq!(GetArrayBufferByteLength(d.get()), 0);
        let (mut len, mut shared, mut data) = (1, true, ptr::null_mut());
        GetArrayBufferLengthAndData(d.get(), &mut len, &mut shared, &mut data);
        assert_eq!((len, shared, data), (0, false, ptr::null_mut()));
        assert_eq!(JS_GetTypedArrayLength(view.get()), 0);
        assert_eq!(JS_GetArrayBufferViewByteOffset(view.get()), 0);
        GetArrayBufferViewLengthAndData(view.get(), &mut len, &mut shared, &mut data);
        assert_eq!(len, 0);
    }
    assert_js(
        cx,
        global,
        "d.byteLength === 0 && d.detached && dv.length === 0",
    );
    assert_eq!(Uint8Array::from(view.get()).unwrap().to_vec(), None);

    // Detaching again is a no-op; detaching a view throws.
    assert!(unsafe { DetachArrayBuffer(cx, d.handle()) });
    assert!(!unsafe { DetachArrayBuffer(cx, view.handle()) });
    take_exception(cx);
    assert!(!unsafe { HasDefinedArrayBufferDetachKey(cx, view.handle(), &mut defined) });
    take_exception(cx);
}

fn steal_contents(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let buf = eval_obj(cx, global, "var s = new Uint8Array([1, 2, 3]); s.buffer"));
    let data = unsafe { StealArrayBufferContents(cx, buf.handle()) } as *mut u8;
    assert!(!data.is_null());
    assert_eq!(unsafe { std::slice::from_raw_parts(data, 3) }, &[1, 2, 3]);
    assert!(unsafe { IsDetachedArrayBufferObject(buf.get()) });
    assert_js(cx, global, "s.length === 0 && s.buffer.byteLength === 0");

    // Stealing a detached buffer or a non-buffer throws.
    assert!(unsafe { StealArrayBufferContents(cx, buf.handle()) }.is_null());
    take_exception(cx);
    rooted!(&in(cx) let view = eval_obj(cx, global, "s"));
    assert!(unsafe { StealArrayBufferContents(cx, view.handle()) }.is_null());
    take_exception(cx);

    // The contents move into a new buffer without a copy.
    rooted!(&in(cx) let n = unsafe { NewArrayBufferWithContents(cx, 3, data as *mut c_void) });
    let mut shared = false;
    assert_eq!(
        unsafe { GetArrayBufferData(n.get(), &mut shared, ptr::null()) },
        data
    );
    set_global(cx, global, c"n", n.get());
    assert_js(cx, global, "new Uint8Array(n).join() === '1,2,3'");

    // And can be stolen again, also when empty.
    let again = unsafe { StealArrayBufferContents(cx, n.handle()) };
    assert_eq!(again as *mut u8, data);
    rooted!(&in(cx) let n2 = unsafe { NewArrayBufferWithContents(cx, 3, again) });
    rooted!(&in(cx) let e = unsafe { NewArrayBuffer(cx, 0) });
    let empty = unsafe { StealArrayBufferContents(cx, e.handle()) };
    assert!(!empty.is_null());
    rooted!(&in(cx) let e2 = unsafe { NewArrayBufferWithContents(cx, 0, empty) });
    assert!(unsafe { ArrayBufferHasData(e2.get()) && ArrayBufferHasData(n2.get()) });
    rooted!(&in(cx) let e3 = unsafe { NewArrayBufferWithContents(cx, 0, ptr::null_mut()) });
    assert!(unsafe { ArrayBufferHasData(e3.get()) });
}

fn clone_and_copy(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let buf = eval_obj(cx, global, "var c = new Uint8Array([0, 1, 2, 3, 4, 5]); c.buffer"));
    rooted!(&in(cx) let clone = unsafe { ArrayBufferClone(cx, buf.handle(), 1, 3) });
    set_global(cx, global, c"cl", clone.get());
    assert_js(
        cx,
        global,
        "cl !== c.buffer && new Uint8Array(cl).join() === '1,2,3'",
    );
    rooted!(&in(cx) let bad = unsafe { ArrayBufferClone(cx, buf.handle(), 4, 3) });
    assert!(bad.get().is_null());
    take_exception(cx);

    // Overlapping copy within one buffer.
    assert!(unsafe { ArrayBufferCopyData(cx, buf.handle(), 2, buf.handle(), 0, 4) });
    assert_js(cx, global, "c.join() === '0,1,0,1,2,3'");
    assert!(unsafe { ArrayBufferCopyData(cx, clone.handle(), 0, buf.handle(), 5, 1) });
    assert_js(cx, global, "new Uint8Array(cl).join() === '3,2,3'");
    assert!(!unsafe { ArrayBufferCopyData(cx, clone.handle(), 1, buf.handle(), 0, 3) });
    take_exception(cx);

    rooted!(&in(cx) let copy = unsafe { CopyArrayBuffer(cx, buf.handle()) });
    set_global(cx, global, c"cp", copy.get());
    assert_js(
        cx,
        global,
        "cp !== c.buffer && new Uint8Array(cp).join() === c.join()",
    );
    assert!(unsafe { DetachArrayBuffer(cx, buf.handle()) });
    rooted!(&in(cx) let bad = unsafe { CopyArrayBuffer(cx, buf.handle()) });
    assert!(bad.get().is_null());
    take_exception(cx);
    rooted!(&in(cx) let bad = unsafe { ArrayBufferClone(cx, buf.handle(), 0, 0) });
    assert!(bad.get().is_null());
    take_exception(cx);
}

/// Counts calls of an external buffer's free function.
struct Freed {
    calls: AtomicUsize,
    contents: AtomicPtr<c_void>,
}

impl Freed {
    fn new() -> Freed {
        Freed {
            calls: AtomicUsize::new(0),
            contents: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

unsafe extern "C" fn count_free(contents: *mut c_void, user_data: *mut c_void) {
    let freed = &*(user_data as *const Freed);
    freed.calls.fetch_add(1, Ordering::SeqCst);
    freed.contents.store(contents, Ordering::SeqCst);
}

fn new_external(cx: &mut JSContext, data: &mut [u8], freed: &Freed) -> *mut JSObject {
    let obj = unsafe {
        NewExternalArrayBuffer(
            cx,
            data.len(),
            data.as_mut_ptr() as *mut c_void,
            Some(count_free),
            freed as *const Freed as *mut c_void,
        )
    };
    assert!(!obj.is_null());
    obj
}

fn external_buffers(cx: &mut JSContext, global: HandleObject) {
    // Freed once when the runtime is destroyed.
    let mut data = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let contents = data.as_mut_ptr() as *mut c_void;
    let freed = Freed::new();
    rooted!(&in(cx) let ext = new_external(cx, &mut data, &freed));
    let mut shared = true;
    assert_eq!(
        unsafe { GetArrayBufferData(ext.get(), &mut shared, ptr::null()) },
        contents as *mut u8
    );
    assert!(!shared);
    set_global(cx, global, c"ext", ext.get());
    assert_js(cx, global, "new Uint8Array(ext)[7] === 8");
    eval(cx, global, "new Uint8Array(ext)[0] = 42");
    assert_eq!(freed.calls(), 0);

    rooted!(&in(cx) let detached = new_external(cx, &mut data, &freed));
    assert!(unsafe { DetachArrayBuffer(cx, detached.handle()) });

    rooted!(&in(cx) let stolen_from = new_external(cx, &mut data, &freed));
    let stolen = unsafe { StealArrayBufferContents(cx, stolen_from.handle()) };
    assert!(!stolen.is_null() && stolen != contents);
    rooted!(&in(cx) let n = unsafe { NewArrayBufferWithContents(cx, 8, stolen) });
    set_global(cx, global, c"n", n.get());
    assert_js(
        cx,
        global,
        "new Uint8Array(n).join() === '42,2,3,4,5,6,7,8'",
    );

    rooted!(&in(cx) let user = unsafe { NewArrayBufferWithUserOwnedContents(cx, 8, data.as_mut_ptr() as *mut c_void) });
    set_global(cx, global, c"user", user.get());
    assert_js(cx, global, "new Uint8Array(user)[1] === 2");
    assert!(unsafe { DetachArrayBuffer(cx, user.handle()) });
}

pub fn run(engine: &JSEngine) {
    run_group(engine, "buffer.types", each_type);
    run_group(engine, "buffer.data_pointers", data_pointers);
    run_group(engine, "buffer.views_with_offsets", views_with_offsets);
    run_group(engine, "buffer.detach", detach);
    run_group(engine, "buffer.steal_contents", steal_contents);
    run_group(engine, "buffer.clone_and_copy", clone_and_copy);
    run_group(engine, "buffer.external", external_buffers);
    run_group(engine, "buffer.get_object_as", get_object_as_family);
    run_group_with_options(engine, "buffer.shared", shared_array_buffer, shared_options());
    run_group(engine, "buffer.view_data", view_data);
}

fn shared_options() -> RealmOptions {
    let mut options = RealmOptions::default();
    options.creationOptions_.sharedMemoryAndAtomics_ = true;
    options.creationOptions_.defineSharedArrayBufferConstructor_ = true;
    options
}

fn run_group_with_options(
    engine: &JSEngine,
    case: &str,
    f: impl FnOnce(&mut JSContext, HandleObject),
    options: RealmOptions,
) {
    let apis = apis_for_case(case);
    crate::support::record_case_with_options(
        engine,
        case,
        apis,
        options,
        |cx, global| {
            f(cx, global);
            crate::support::Observation::ok(crate::support::Value::Boolean(true))
        },
    );
}

fn run_group(
    engine: &JSEngine,
    case: &str,
    f: impl FnOnce(&mut JSContext, HandleObject),
) {
    let apis = apis_for_case(case);
    crate::support::run_case(case, apis, || {
        with_global(engine, f);
        crate::support::Observation::ok(crate::support::Value::Boolean(true))
    });
}

fn get_object_as_family(cx: &mut JSContext, _global: HandleObject) {
    let buffer = unsafe { NewArrayBuffer(cx, 64) };
    rooted!(&in(cx) let buffer = buffer);
    let mut shared = false;
    let base = unsafe { GetArrayBufferData(buffer.get(), &mut shared, ptr::null()) };
    assert!(!base.is_null());
    assert!(!shared);

    macro_rules! check_array {
        ($new:ident, $object_as:ident, $data:ident, $length_and_data:ident, $ty:ty, $count:expr) => {{
            let array = unsafe { $new(cx, buffer.handle(), 0, $count) };
            rooted!(&in(cx) let array = array);
            assert!(!array.get().is_null());
            let mut length = 0;
            let (mut data, mut is_shared) = (ptr::null_mut::<$ty>(), false);
            let object = unsafe { $object_as(array.get(), &mut length, &mut is_shared, &mut data) };
            assert_eq!(object, array.get());
            assert_eq!(length, $count);
            assert!(!is_shared);
            assert_eq!(data as *mut u8, base);
            assert_eq!(unsafe { $data(array.get(), &mut is_shared, ptr::null()) }, data);
            assert!(!is_shared);
            let (mut glen, mut gshared, mut gdata) = (0, true, ptr::null_mut());
            unsafe { $length_and_data(array.get(), &mut glen, &mut gshared, &mut gdata) };
            assert_eq!(glen, $count);
            assert!(!gshared);
            assert_eq!(gdata.cast::<u8>(), (data as *mut u8).cast::<u8>());
        }};
    }

    check_array!(JS_NewInt8ArrayWithBuffer, JS_GetObjectAsInt8Array, JS_GetInt8ArrayData, GetInt8ArrayLengthAndData, i8, 4);
    check_array!(JS_NewUint8ArrayWithBuffer, JS_GetObjectAsUint8Array, JS_GetUint8ArrayData, GetUint8ArrayLengthAndData, u8, 4);
    check_array!(JS_NewUint8ClampedArrayWithBuffer, JS_GetObjectAsUint8ClampedArray, JS_GetUint8ClampedArrayData, GetUint8ClampedArrayLengthAndData, u8, 4);
    check_array!(JS_NewInt16ArrayWithBuffer, JS_GetObjectAsInt16Array, JS_GetInt16ArrayData, GetInt16ArrayLengthAndData, i16, 2);
    check_array!(JS_NewUint16ArrayWithBuffer, JS_GetObjectAsUint16Array, JS_GetUint16ArrayData, GetUint16ArrayLengthAndData, u16, 2);
    check_array!(JS_NewInt32ArrayWithBuffer, JS_GetObjectAsInt32Array, JS_GetInt32ArrayData, GetInt32ArrayLengthAndData, i32, 2);
    check_array!(JS_NewUint32ArrayWithBuffer, JS_GetObjectAsUint32Array, JS_GetUint32ArrayData, GetUint32ArrayLengthAndData, u32, 2);
    check_array!(JS_NewFloat32ArrayWithBuffer, JS_GetObjectAsFloat32Array, JS_GetFloat32ArrayData, GetFloat32ArrayLengthAndData, f32, 2);
    check_array!(JS_NewFloat64ArrayWithBuffer, JS_GetObjectAsFloat64Array, JS_GetFloat64ArrayData, GetFloat64ArrayLengthAndData, f64, 1);
    check_array!(JS_NewFloat16ArrayWithBuffer, JS_GetObjectAsFloat16Array, JS_GetFloat16ArrayData, GetFloat32ArrayLengthAndData, u16, 2);
    check_array!(JS_NewBigInt64ArrayWithBuffer, JS_GetObjectAsBigInt64Array, JS_GetBigInt64ArrayData, GetFloat64ArrayLengthAndData, i64, 1);
    check_array!(JS_NewBigUint64ArrayWithBuffer, JS_GetObjectAsBigUint64Array, JS_GetBigUint64ArrayData, GetFloat64ArrayLengthAndData, u64, 1);

    let view = unsafe { JS_NewUint8ArrayWithBuffer(cx, buffer.handle(), 8, 4) };
    rooted!(&in(cx) let view = view);
    let (mut length, mut is_shared, mut data) = (0, true, ptr::null_mut());
    assert_eq!(
        unsafe { JS_GetObjectAsArrayBufferView(view.get(), &mut length, &mut is_shared, &mut data) },
        view.get()
    );
    assert_eq!((length, is_shared), (4, false));
    assert_eq!(data, unsafe { base.add(8) });

    let mut length = 0;
    let mut data = ptr::null_mut();
    assert_eq!(
        unsafe { GetObjectAsArrayBuffer(buffer.get(), &mut length, &mut data) },
        buffer.get()
    );
    assert_eq!((length, data), (64, base));
}

fn shared_array_buffer(cx: &mut JSContext, _global: HandleObject) {
    let shared = unsafe { NewSharedArrayBuffer(cx.raw_cx(), 16) };
    rooted!(&in(cx) let shared = shared);
    assert!(!shared.get().is_null());
    assert!(unsafe { IsArrayBufferObjectMaybeShared(shared.get()) });
    assert!(unsafe { IsSharedArrayBufferObject(shared.get()) });
    assert!(!unsafe { IsResizableArrayBufferMaybeShared(shared.get()) });
    assert_eq!(unsafe { UnwrapArrayBufferMaybeShared(shared.get()) }, shared.get());
    assert_eq!(unsafe { UnwrapSharedArrayBuffer(shared.get()) }, shared.get());
    assert_eq!(unsafe { GetSharedArrayBufferByteLength(shared.get()) }, 16);

    let mut is_shared = false;
    let data = unsafe { GetSharedArrayBufferData(shared.get(), &mut is_shared, ptr::null()) };
    assert!(is_shared);
    assert!(!data.is_null());
    let (mut length, mut is_shared, mut data2) = (0, false, ptr::null_mut());
    unsafe { GetSharedArrayBufferLengthAndData(shared.get(), &mut length, &mut is_shared, &mut data2) };
    assert_eq!((length, is_shared, data2), (16, true, data));

    let view = unsafe { JS_NewUint8ArrayWithBuffer(cx, shared.handle(), 0, 4) };
    rooted!(&in(cx) let view = view);
    assert!(unsafe { IsArrayBufferViewShared(view.get()) });
    assert!(unsafe { JS_GetTypedArraySharedness(view.get()) });
    let mut is_shared = false;
    assert_eq!(
        unsafe { JS_GetUint8ArrayData(view.get(), &mut is_shared, ptr::null()) },
        data
    );
    assert!(is_shared);
    let (mut length, mut is_shared, mut view_data) = (0, false, ptr::null_mut());
    unsafe { GetUint8ArrayLengthAndData(view.get(), &mut length, &mut is_shared, &mut view_data) };
    assert_eq!((length, is_shared, view_data), (4, true, data));
}

fn view_data(cx: &mut JSContext, _global: HandleObject) {
    let buffer = unsafe { NewArrayBuffer(cx, 16) };
    rooted!(&in(cx) let buffer = buffer);
    assert!(unsafe { js::jsapi::EnsureNonInlineArrayBufferOrView(cx.raw_cx(), buffer.get()) });
    let mut is_shared = false;
    let base = unsafe { GetArrayBufferData(buffer.get(), &mut is_shared, ptr::null()) };
    assert!(!base.is_null());

    let view = unsafe { JS_NewUint8ArrayWithBuffer(cx, buffer.handle(), 4, 8) };
    rooted!(&in(cx) let view = view);
    let mut is_shared = false;
    assert_eq!(
        unsafe { JS_GetArrayBufferViewData(view.get(), &mut is_shared, ptr::null()) },
        unsafe { base.add(4) } as *mut c_void
    );
    assert!(!is_shared);
    let mut fixed = [0u8; 16];
    assert_eq!(
        unsafe { JS_GetArrayBufferViewFixedData(view.get(), fixed.as_mut_ptr(), fixed.len()) },
        unsafe { base.add(4) }
    );

    let mut is_shared = false;
    assert_eq!(
        unsafe { GetArrayBufferMaybeSharedData(buffer.get(), &mut is_shared, ptr::null()) },
        base
    );
    assert!(!is_shared);
    let (mut length, mut is_shared, mut data) = (0, true, ptr::null_mut());
    unsafe { GetArrayBufferMaybeSharedLengthAndData(buffer.get(), &mut length, &mut is_shared, &mut data) };
    assert_eq!((length, is_shared, data), (16, false, base));
}


fn apis_for_case(case: &str) -> &'static [&'static str] {
    match case {
        "buffer.types" => &[
            "JS_NewInt8Array",
            "JS_NewInt8ArrayFromArray",
            "JS_NewInt8ArrayWithBuffer",
            "UnwrapInt8Array",
            "JS_NewUint8Array",
            "JS_NewUint8ArrayFromArray",
            "JS_NewUint8ArrayWithBuffer",
            "UnwrapUint8Array",
            "JS_NewUint8ClampedArray",
            "JS_NewUint8ClampedArrayFromArray",
            "JS_NewUint8ClampedArrayWithBuffer",
            "UnwrapUint8ClampedArray",
            "JS_NewInt16Array",
            "JS_NewInt16ArrayFromArray",
            "JS_NewInt16ArrayWithBuffer",
            "UnwrapInt16Array",
            "JS_NewUint16Array",
            "JS_NewUint16ArrayFromArray",
            "JS_NewUint16ArrayWithBuffer",
            "UnwrapUint16Array",
            "JS_NewInt32Array",
            "JS_NewInt32ArrayFromArray",
            "JS_NewInt32ArrayWithBuffer",
            "UnwrapInt32Array",
            "JS_NewUint32Array",
            "JS_NewUint32ArrayFromArray",
            "JS_NewUint32ArrayWithBuffer",
            "UnwrapUint32Array",
            "JS_NewFloat16Array",
            "JS_NewFloat16ArrayFromArray",
            "JS_NewFloat16ArrayWithBuffer",
            "UnwrapFloat16Array",
            "JS_NewFloat32Array",
            "JS_NewFloat32ArrayFromArray",
            "JS_NewFloat32ArrayWithBuffer",
            "UnwrapFloat32Array",
            "JS_NewFloat64Array",
            "JS_NewFloat64ArrayFromArray",
            "JS_NewFloat64ArrayWithBuffer",
            "UnwrapFloat64Array",
            "JS_NewBigInt64Array",
            "JS_NewBigInt64ArrayFromArray",
            "JS_NewBigInt64ArrayWithBuffer",
            "UnwrapBigInt64Array",
            "JS_NewBigUint64Array",
            "JS_NewBigUint64ArrayFromArray",
            "JS_NewBigUint64ArrayWithBuffer",
            "UnwrapBigUint64Array",
            "JS_IsTypedArrayObject",
            "JS_IsArrayBufferViewObject",
            "IsArrayBufferObject",
            "JS_GetArrayBufferViewType",
            "JS_GetTypedArrayLength",
            "JS_GetTypedArrayByteLength",
            "JS_GetTypedArraySharedness",
            "UnwrapArrayBufferView",
            "NewArrayBuffer",
            "JS_GetArrayBufferViewByteOffset",
            "UnwrapArrayBuffer",
        ],
        "buffer.data_pointers" => &[
            "JS_NewUint8Array",
            "JS_GetUint8ArrayData",
            "JS_GetObjectAsFloat64Array",
            "JS_NewBigInt64Array",
            "JS_GetBigInt64ArrayData",
            "JS_NewFloat16Array",
            "JS_GetFloat16ArrayData",
            "NewArrayBuffer",
            "GetArrayBufferData",
            "GetArrayBufferLengthAndData",
        ],
        "buffer.views_with_offsets" => &[
            "GetArrayBufferData",
            "JS_GetArrayBufferViewByteOffset",
            "JS_GetArrayBufferViewByteLength",
            "JS_GetTypedArrayLength",
            "GetArrayBufferViewLengthAndData",
            "JS_GetArrayBufferViewBuffer",
            "JS_NewDataView",
            "JS_IsArrayBufferViewObject",
            "JS_IsTypedArrayObject",
            "JS_GetArrayBufferViewType",
            "IsResizableArrayBufferView",
        ],
        "buffer.detach" => &[
            "HasDefinedArrayBufferDetachKey",
            "ArrayBufferHasData",
            "DetachArrayBuffer",
            "IsDetachedArrayBufferObject",
            "GetArrayBufferByteLength",
            "GetArrayBufferLengthAndData",
            "JS_GetTypedArrayLength",
            "JS_GetArrayBufferViewByteOffset",
            "GetArrayBufferViewLengthAndData",
        ],
        "buffer.steal_contents" => &[
            "StealArrayBufferContents",
            "IsDetachedArrayBufferObject",
            "NewArrayBufferWithContents",
            "GetArrayBufferData",
            "NewArrayBuffer",
            "ArrayBufferHasData",
        ],
        "buffer.clone_and_copy" => &[
            "ArrayBufferClone",
            "ArrayBufferCopyData",
            "CopyArrayBuffer",
            "DetachArrayBuffer",
        ],
        "buffer.external" => &[
            "NewExternalArrayBuffer",
            "GetArrayBufferData",
            "StealArrayBufferContents",
            "NewArrayBufferWithContents",
            "NewArrayBufferWithUserOwnedContents",
            "DetachArrayBuffer",
        ],
        "buffer.get_object_as" => &[
            "NewArrayBuffer", "GetArrayBufferData", "JS_NewInt8ArrayWithBuffer",
            "JS_GetObjectAsInt8Array", "JS_GetInt8ArrayData", "GetInt8ArrayLengthAndData",
            "JS_NewUint8ArrayWithBuffer", "JS_GetObjectAsUint8Array", "JS_GetUint8ArrayData",
            "GetUint8ArrayLengthAndData", "JS_NewUint8ClampedArrayWithBuffer",
            "JS_GetObjectAsUint8ClampedArray", "JS_GetUint8ClampedArrayData",
            "GetUint8ClampedArrayLengthAndData", "JS_NewInt16ArrayWithBuffer",
            "JS_GetObjectAsInt16Array", "JS_GetInt16ArrayData", "GetInt16ArrayLengthAndData",
            "JS_NewUint16ArrayWithBuffer", "JS_GetObjectAsUint16Array", "JS_GetUint16ArrayData",
            "GetUint16ArrayLengthAndData", "JS_NewInt32ArrayWithBuffer",
            "JS_GetObjectAsInt32Array", "JS_GetInt32ArrayData", "GetInt32ArrayLengthAndData",
            "JS_NewUint32ArrayWithBuffer", "JS_GetObjectAsUint32Array", "JS_GetUint32ArrayData",
            "GetUint32ArrayLengthAndData", "JS_NewFloat32ArrayWithBuffer",
            "JS_GetObjectAsFloat32Array", "JS_GetFloat32ArrayData", "GetFloat32ArrayLengthAndData",
            "JS_NewFloat64ArrayWithBuffer", "JS_GetObjectAsFloat64Array", "JS_GetFloat64ArrayData",
            "GetFloat64ArrayLengthAndData", "JS_NewFloat16ArrayWithBuffer",
            "JS_GetObjectAsFloat16Array", "JS_GetFloat16ArrayData", "JS_NewBigInt64ArrayWithBuffer",
            "JS_GetObjectAsBigInt64Array", "JS_GetBigInt64ArrayData",
            "JS_NewBigUint64ArrayWithBuffer", "JS_GetObjectAsBigUint64Array",
            "JS_GetBigUint64ArrayData", "JS_GetObjectAsArrayBufferView", "GetObjectAsArrayBuffer",
        ],
        "buffer.shared" => &[
            "NewSharedArrayBuffer", "IsArrayBufferObjectMaybeShared", "IsSharedArrayBufferObject",
            "IsResizableArrayBufferMaybeShared", "UnwrapArrayBufferMaybeShared",
            "UnwrapSharedArrayBuffer", "GetSharedArrayBufferByteLength",
            "GetSharedArrayBufferData", "GetSharedArrayBufferLengthAndData",
            "JS_NewUint8ArrayWithBuffer", "IsArrayBufferViewShared", "JS_GetTypedArraySharedness",
            "JS_GetUint8ArrayData", "GetUint8ArrayLengthAndData",
        ],
        "buffer.view_data" => &[
            "NewArrayBuffer", "EnsureNonInlineArrayBufferOrView", "GetArrayBufferData",
            "JS_NewUint8ArrayWithBuffer", "JS_GetArrayBufferViewData",
            "JS_GetArrayBufferViewFixedData", "GetArrayBufferMaybeSharedData",
            "GetArrayBufferMaybeSharedLengthAndData",
        ],
        _ => panic!("unknown buffer case {case}"),
    }
}
