/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! ArrayBuffer, DataView and typed-array entry points.

use std::ptr;

use js::glue::GetUint8ArrayLengthAndData;
use js::jsapi::{
    IsArrayBufferObject,
    IsArrayBufferObjectMaybeShared, IsArrayBufferViewShared, IsDetachedArrayBufferObject,
    IsResizableArrayBufferMaybeShared, IsResizableArrayBufferView, IsSharedArrayBufferObject,
    JS_GetArrayBufferViewBuffer, JS_GetArrayBufferViewByteLength, JS_GetArrayBufferViewByteOffset,
    JS_GetArrayBufferViewData, JS_GetArrayBufferViewFixedData, JS_GetArrayBufferViewType,
    JS_GetObjectAsUint8Array, JS_GetTypedArrayByteLength, JS_GetTypedArrayByteOffset,
    JS_GetTypedArrayLength, JS_GetTypedArraySharedness, JS_GetUint8ArrayData,
    JS_IsArrayBufferViewObject, JS_IsTypedArrayObject, JS_NewBigInt64Array,
    JS_NewBigInt64ArrayFromArray, JS_NewBigInt64ArrayWithBuffer, JS_NewBigUint64Array,
    JS_NewBigUint64ArrayFromArray, JS_NewBigUint64ArrayWithBuffer, JS_NewDataView,
    JS_NewFloat16Array, JS_NewFloat16ArrayFromArray, JS_NewFloat16ArrayWithBuffer,
    JS_NewFloat32Array, JS_NewFloat32ArrayFromArray, JS_NewFloat32ArrayWithBuffer,
    JS_NewFloat64Array, JS_NewFloat64ArrayFromArray, JS_NewFloat64ArrayWithBuffer,
    JS_NewInt16Array, JS_NewInt16ArrayFromArray, JS_NewInt16ArrayWithBuffer,
    JS_NewInt32Array, JS_NewInt32ArrayFromArray, JS_NewInt32ArrayWithBuffer,
    JS_NewInt8Array, JS_NewInt8ArrayFromArray, JS_NewInt8ArrayWithBuffer,
    JS_NewUint16Array, JS_NewUint16ArrayFromArray, JS_NewUint16ArrayWithBuffer,
    JS_NewUint32Array, JS_NewUint32ArrayFromArray, JS_NewUint32ArrayWithBuffer,
    JS_NewUint8Array, JS_NewUint8ClampedArray, JS_NewUint8ClampedArrayFromArray,
    JS_NewUint8ClampedArrayWithBuffer, JS_NewUint8ArrayFromArray, JS_NewUint8ArrayWithBuffer,
    NewArrayBuffer, NewSharedArrayBuffer, Type,
    UnwrapArrayBuffer, UnwrapArrayBufferMaybeShared, UnwrapArrayBufferView,
    UnwrapBigInt64Array, UnwrapBigUint64Array, UnwrapFloat16Array, UnwrapFloat32Array,
    UnwrapFloat64Array, UnwrapInt16Array, UnwrapInt32Array, UnwrapInt8Array,
    UnwrapSharedArrayBuffer, UnwrapUint16Array, UnwrapUint32Array, UnwrapUint8Array,
    UnwrapUint8ClampedArray,
};
use js::jsval::ObjectValue;
use js::rooted;
use js::rust::{JSEngine, RealmOptions, ToBoolean};

use crate::support::{Observation, Value, eval, record_case, record_case_with_options, set_global};

pub fn run(engine: &JSEngine) {
    typed_array_family(engine);
    array_buffer_queries(engine);
    data_views_and_offsets(engine);
}

macro_rules! typed_array_case {
    ($engine:expr, $case:literal, $new:ident, $from_array:ident, $with_buffer:ident,
     $unwrap:ident, $ty:expr, $ctor:literal, $size:expr, $init:literal, $apis:expr) => {{
        record_case($engine, $case, $apis, |cx, global| unsafe {
            let array = $new(cx.raw_cx(), 4);
            rooted!(&in(cx) let array = array);
            assert!(!array.get().is_null());
            assert!(JS_IsTypedArrayObject(array.get()));
            assert!(JS_IsArrayBufferViewObject(array.get()));
            assert!(!IsArrayBufferObject(array.get()));
            assert_eq!(JS_GetArrayBufferViewType(array.get()), $ty);
            assert_eq!(JS_GetTypedArrayLength(array.get()), 4);
            assert_eq!(JS_GetTypedArrayByteLength(array.get()), 4 * $size);
            assert!(!JS_GetTypedArraySharedness(array.get()));
            assert_eq!($unwrap(array.get()), array.get());
            assert_eq!(UnwrapArrayBufferView(array.get()), array.get());

            set_global(cx, global, c"a", ObjectValue(array.get()));
            let is_ctor = eval(cx, global, concat!("a instanceof ", $ctor, " && a.length === 4"));
            rooted!(&in(cx) let is_ctor = is_ctor);
            assert!(ToBoolean(is_ctor.handle()));

            let source = eval(cx, global, $init).to_object();
            rooted!(&in(cx) let source = source);
            let from_array = $from_array(cx.raw_cx(), source.handle().into());
            rooted!(&in(cx) let from_array = from_array);
            assert!(!from_array.get().is_null());
            assert_eq!(JS_GetTypedArrayLength(from_array.get()), 3);
            set_global(cx, global, c"b", ObjectValue(from_array.get()));
            let is_ctor = eval(cx, global, concat!("b instanceof ", $ctor, " && b[2] == 3"));
            rooted!(&in(cx) let is_ctor = is_ctor);
            assert!(ToBoolean(is_ctor.handle()));

            let buffer = NewArrayBuffer(cx.raw_cx(), 8 * $size);
            rooted!(&in(cx) let buffer = buffer);
            let view = $with_buffer(cx.raw_cx(), buffer.handle().into(), 2 * $size, -1);
            rooted!(&in(cx) let view = view);
            assert!(!view.get().is_null());
            assert_eq!(JS_GetTypedArrayLength(view.get()), 6);
            assert_eq!(JS_GetArrayBufferViewByteOffset(view.get()), 2 * $size);

            let sized = $with_buffer(cx.raw_cx(), buffer.handle().into(), 0, 3);
            rooted!(&in(cx) let sized = sized);
            assert_eq!(JS_GetTypedArrayLength(sized.get()), 3);
            Observation::ok(Value::Boolean(true))
        })
    }};
}

fn typed_array_family(engine: &JSEngine) {
    typed_array_case!(
        engine,
        "typed_array.int8",
        JS_NewInt8Array,
        JS_NewInt8ArrayFromArray,
        JS_NewInt8ArrayWithBuffer,
        UnwrapInt8Array,
        Type::Int8,
        "Int8Array",
        1,
        "[1, 2, 3]",
        &[
            "JS_NewInt8Array",
            "JS_NewInt8ArrayFromArray",
            "JS_NewInt8ArrayWithBuffer",
            "UnwrapInt8Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.uint8",
        JS_NewUint8Array,
        JS_NewUint8ArrayFromArray,
        JS_NewUint8ArrayWithBuffer,
        UnwrapUint8Array,
        Type::Uint8,
        "Uint8Array",
        1,
        "[1, 2, 3]",
        &[
            "JS_NewUint8Array",
            "JS_NewUint8ArrayFromArray",
            "JS_NewUint8ArrayWithBuffer",
            "UnwrapUint8Array",
            "JS_IsTypedArrayObject",
            "JS_IsArrayBufferViewObject",
            "JS_GetArrayBufferViewType",
            "JS_GetTypedArrayLength",
            "JS_GetTypedArrayByteLength",
            "JS_GetTypedArraySharedness",
            "UnwrapArrayBufferView",
            "NewArrayBuffer",
            "JS_GetArrayBufferViewByteOffset",
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.uint8_clamped",
        JS_NewUint8ClampedArray,
        JS_NewUint8ClampedArrayFromArray,
        JS_NewUint8ClampedArrayWithBuffer,
        UnwrapUint8ClampedArray,
        Type::Uint8Clamped,
        "Uint8ClampedArray",
        1,
        "[1, 2, 3]",
        &[
            "JS_NewUint8ClampedArray",
            "JS_NewUint8ClampedArrayFromArray",
            "JS_NewUint8ClampedArrayWithBuffer",
            "UnwrapUint8ClampedArray",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.int16",
        JS_NewInt16Array,
        JS_NewInt16ArrayFromArray,
        JS_NewInt16ArrayWithBuffer,
        UnwrapInt16Array,
        Type::Int16,
        "Int16Array",
        2,
        "[1, 2, 3]",
        &[
            "JS_NewInt16Array",
            "JS_NewInt16ArrayFromArray",
            "JS_NewInt16ArrayWithBuffer",
            "UnwrapInt16Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.uint16",
        JS_NewUint16Array,
        JS_NewUint16ArrayFromArray,
        JS_NewUint16ArrayWithBuffer,
        UnwrapUint16Array,
        Type::Uint16,
        "Uint16Array",
        2,
        "[1, 2, 3]",
        &[
            "JS_NewUint16Array",
            "JS_NewUint16ArrayFromArray",
            "JS_NewUint16ArrayWithBuffer",
            "UnwrapUint16Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.int32",
        JS_NewInt32Array,
        JS_NewInt32ArrayFromArray,
        JS_NewInt32ArrayWithBuffer,
        UnwrapInt32Array,
        Type::Int32,
        "Int32Array",
        4,
        "[1, 2, 3]",
        &[
            "JS_NewInt32Array",
            "JS_NewInt32ArrayFromArray",
            "JS_NewInt32ArrayWithBuffer",
            "UnwrapInt32Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.uint32",
        JS_NewUint32Array,
        JS_NewUint32ArrayFromArray,
        JS_NewUint32ArrayWithBuffer,
        UnwrapUint32Array,
        Type::Uint32,
        "Uint32Array",
        4,
        "[1, 2, 3]",
        &[
            "JS_NewUint32Array",
            "JS_NewUint32ArrayFromArray",
            "JS_NewUint32ArrayWithBuffer",
            "UnwrapUint32Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.float16",
        JS_NewFloat16Array,
        JS_NewFloat16ArrayFromArray,
        JS_NewFloat16ArrayWithBuffer,
        UnwrapFloat16Array,
        Type::Float16,
        "Float16Array",
        2,
        "[1, 2, 3]",
        &[
            "JS_NewFloat16Array",
            "JS_NewFloat16ArrayFromArray",
            "JS_NewFloat16ArrayWithBuffer",
            "UnwrapFloat16Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.float32",
        JS_NewFloat32Array,
        JS_NewFloat32ArrayFromArray,
        JS_NewFloat32ArrayWithBuffer,
        UnwrapFloat32Array,
        Type::Float32,
        "Float32Array",
        4,
        "[1, 2, 3]",
        &[
            "JS_NewFloat32Array",
            "JS_NewFloat32ArrayFromArray",
            "JS_NewFloat32ArrayWithBuffer",
            "UnwrapFloat32Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.float64",
        JS_NewFloat64Array,
        JS_NewFloat64ArrayFromArray,
        JS_NewFloat64ArrayWithBuffer,
        UnwrapFloat64Array,
        Type::Float64,
        "Float64Array",
        8,
        "[1, 2, 3]",
        &[
            "JS_NewFloat64Array",
            "JS_NewFloat64ArrayFromArray",
            "JS_NewFloat64ArrayWithBuffer",
            "UnwrapFloat64Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.bigint64",
        JS_NewBigInt64Array,
        JS_NewBigInt64ArrayFromArray,
        JS_NewBigInt64ArrayWithBuffer,
        UnwrapBigInt64Array,
        Type::BigInt64,
        "BigInt64Array",
        8,
        "[1n, 2n, 3n]",
        &[
            "JS_NewBigInt64Array",
            "JS_NewBigInt64ArrayFromArray",
            "JS_NewBigInt64ArrayWithBuffer",
            "UnwrapBigInt64Array",
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
        ]
    );
    typed_array_case!(
        engine,
        "typed_array.biguint64",
        JS_NewBigUint64Array,
        JS_NewBigUint64ArrayFromArray,
        JS_NewBigUint64ArrayWithBuffer,
        UnwrapBigUint64Array,
        Type::BigUint64,
        "BigUint64Array",
        8,
        "[1n, 2n, 3n]",
        &[
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
        ]
    );
}

fn array_buffer_queries(engine: &JSEngine) {
    let mut options = RealmOptions::default();
    options.creationOptions_.sharedMemoryAndAtomics_ = true;
    options.creationOptions_.defineSharedArrayBufferConstructor_ = true;
    record_case_with_options(
        engine,
        "array_buffer.view_queries",
        &[
            "NewArrayBuffer",
            "NewSharedArrayBuffer",
            "UnwrapArrayBuffer",
            "UnwrapArrayBufferMaybeShared",
            "UnwrapSharedArrayBuffer",
            "IsArrayBufferObjectMaybeShared",
            "IsResizableArrayBufferMaybeShared",
            "IsResizableArrayBufferView",
            "IsArrayBufferViewShared",
            "IsSharedArrayBufferObject",
            "GetUint8ArrayLengthAndData",
        ],
        options,
        |cx, _| unsafe {
            let buffer = NewArrayBuffer(cx.raw_cx(), 8);
            rooted!(&in(cx) let buffer = buffer);
            assert_eq!(UnwrapArrayBuffer(buffer.get()), buffer.get());
            assert_eq!(UnwrapArrayBufferMaybeShared(buffer.get()), buffer.get());
            assert!(UnwrapSharedArrayBuffer(buffer.get()).is_null());
            assert!(IsArrayBufferObjectMaybeShared(buffer.get()));
            assert!(!IsSharedArrayBufferObject(buffer.get()));
            assert!(!IsResizableArrayBufferMaybeShared(buffer.get()));

            let shared = NewSharedArrayBuffer(cx.raw_cx(), 8);
            rooted!(&in(cx) let shared = shared);
            assert!(!shared.get().is_null());
            assert!(IsSharedArrayBufferObject(shared.get()));
            assert_eq!(UnwrapSharedArrayBuffer(shared.get()), shared.get());

            let array = JS_NewUint8Array(cx.raw_cx(), 4);
            rooted!(&in(cx) let array = array);
            assert!(!IsArrayBufferViewShared(array.get()));
            assert!(!IsResizableArrayBufferView(array.get()));
            let mut length = 0;
            let mut is_shared = true;
            let mut data = ptr::null_mut();
            GetUint8ArrayLengthAndData(array.get(), &mut length, &mut is_shared, &mut data);
            assert_eq!(length, 4);
            assert!(!is_shared);
            assert!(!data.is_null());
            Observation::ok(Value::Boolean(true))
        },
    );
}

fn data_views_and_offsets(engine: &JSEngine) {
    record_case(
        engine,
        "array_buffer.views_and_offsets",
        &[
            "NewArrayBuffer",
            "JS_NewDataView",
            "JS_NewUint8Array",
            "JS_NewUint8ArrayWithBuffer",
            "JS_GetArrayBufferViewBuffer",
            "JS_GetArrayBufferViewByteLength",
            "JS_GetArrayBufferViewByteOffset",
            "JS_GetArrayBufferViewData",
            "JS_GetArrayBufferViewFixedData",
            "JS_GetArrayBufferViewType",
            "JS_GetObjectAsUint8Array",
            "JS_GetTypedArrayByteLength",
            "JS_GetTypedArrayByteOffset",
            "JS_GetTypedArrayLength",
            "JS_GetTypedArraySharedness",
            "JS_GetUint8ArrayData",
            "JS_IsArrayBufferViewObject",
            "JS_IsTypedArrayObject",
            "IsDetachedArrayBufferObject",
            "IsArrayBufferObject",
        ],
        |cx, _| unsafe {
            let buffer = NewArrayBuffer(cx.raw_cx(), 16);
            rooted!(&in(cx) let buffer = buffer);
            let view = JS_NewUint8ArrayWithBuffer(cx.raw_cx(), buffer.handle().into(), 4, 4);
            rooted!(&in(cx) let view = view);
            assert!(!view.get().is_null());
            assert!(JS_IsArrayBufferViewObject(view.get()));
            assert!(JS_IsTypedArrayObject(view.get()));
            assert!(!IsDetachedArrayBufferObject(buffer.get()));
            assert_eq!(JS_GetTypedArrayLength(view.get()), 4);
            assert_eq!(JS_GetTypedArrayByteLength(view.get()), 4);
            assert_eq!(JS_GetTypedArrayByteOffset(view.get()), 4);
            assert!(!JS_GetTypedArraySharedness(view.get()));
            assert_eq!(JS_GetArrayBufferViewByteOffset(view.get()), 4);
            assert_eq!(JS_GetArrayBufferViewByteLength(view.get()), 4);
            assert_eq!(JS_GetArrayBufferViewType(view.get()), Type::Uint8);
            let mut shared = false;
            let view_buffer =
                JS_GetArrayBufferViewBuffer(cx.raw_cx(), view.handle().into(), &mut shared);
            rooted!(&in(cx) let view_buffer = view_buffer);
            assert_eq!(view_buffer.get(), buffer.get());
            assert!(!shared);

            let data = JS_GetArrayBufferViewData(view.get(), &mut shared, ptr::null());
            assert!(!data.is_null());
            let mut fixed_buffer = [0u8; 64];
            let fixed = JS_GetArrayBufferViewFixedData(
                view.get(),
                fixed_buffer.as_mut_ptr(),
                fixed_buffer.len(),
            );
            assert!(!fixed.is_null());
            let typed_data = JS_GetUint8ArrayData(view.get(), &mut shared, ptr::null());
            assert!(!typed_data.is_null());
            let mut length = 0;
            let mut data = ptr::null_mut();
            let object = JS_GetObjectAsUint8Array(view.get(), &mut length, &mut shared, &mut data);
            assert_eq!(object, view.get());
            assert_eq!(length, 4);

            let data_view = JS_NewDataView(cx.raw_cx(), buffer.handle().into(), 2, 4);
            rooted!(&in(cx) let data_view = data_view);
            assert!(!data_view.get().is_null());
            assert!(JS_IsArrayBufferViewObject(data_view.get()));
            assert!(!JS_IsTypedArrayObject(data_view.get()));
            assert_eq!(
                JS_GetArrayBufferViewType(data_view.get()),
                Type::MaxTypedArrayViewType
            );
            Observation::ok(Value::Boolean(true))
        },
    );
}
