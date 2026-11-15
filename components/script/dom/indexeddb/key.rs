/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cmp::Ordering;
use std::ffi::CString;
use std::{ptr, slice};

use itertools::Itertools;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::jsapi::{
    ClippedTime, ESClass, IsArrayBufferObject, IsDetachedArrayBufferObject,
    JS_DeprecatedStringHasLatin1Chars, JS_GetArrayBufferViewBuffer, JS_GetStringLength,
    JS_IsArrayBufferViewObject, JSString, NewArrayObject1, PropertyKey,
};
use js::jsval::{DoubleValue, ObjectValue, StringValue, UndefinedValue};
use js::rust::wrappers2::{
    GetArrayLength, GetBuiltinClass, IsArrayObject, JS_AtomizeUCStringN, JS_ClearPendingException,
    JS_DefinePropertyById2, JS_GetLatin1StringCharsAndLength, JS_GetTwoByteStringCharsAndLength,
    JS_HasOwnPropertyById, JS_IndexToId, JS_IsIdentifier, JS_NewUCStringCopyN, JS_StringToId,
    NewDateObject, ObjectIsDate, SameValue,
};
use js::rust::{HandleId, HandleObject, HandleValue, MutableHandleId, MutableHandleValue};
use js::typedarray::{ArrayBuffer, ArrayBufferU8, ArrayBufferView};
use script_bindings::js::{JS_GetPropertyById, JS_IsExceptionPending, JS_NewObject, PropertyFlags};
use storage_traits::indexeddb::{
    BackendError, IndexedDBIndex, IndexedDBIndexKey, IndexedDBKeyRange, IndexedDBKeyType,
    Utf16String,
};

use crate::dom::bindings::buffer_source::create_buffer_source;
use crate::dom::bindings::codegen::Bindings::BlobBinding::BlobMethods;
use crate::dom::bindings::codegen::Bindings::FileBinding::FileMethods;
use crate::dom::bindings::codegen::UnionTypes::StringOrStringSequence as StrOrStringSequence;
use crate::dom::bindings::conversions::{get_property_jsval, root_from_handlevalue};
use crate::dom::bindings::error::Error;
use crate::dom::bindings::root::DomRoot;
use crate::dom::bindings::str::DOMString;
use crate::dom::bindings::utils::define_dictionary_property;
use crate::dom::blob::Blob;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::file::File;
use crate::dom::globalscope::GlobalScope;
use crate::dom::idbkeyrange::IDBKeyRange;
use crate::dom::idbobjectstore::KeyPath;

/// The code units of the JavaScript string `string`, which must not be null.
#[expect(unsafe_code)]
fn js_string_code_units(cx: &JSContext, string: *mut JSString) -> Vec<u16> {
    let mut length = 0;
    // SAFETY: `string` is a live string, and its characters are copied before anything can
    // garbage collect.
    unsafe {
        if JS_DeprecatedStringHasLatin1Chars(string) {
            let chars = JS_GetLatin1StringCharsAndLength(cx, string, &mut length);
            assert!(!chars.is_null());
            slice::from_raw_parts(chars, length)
                .iter()
                .map(|&char| char.into())
                .collect()
        } else {
            let chars = JS_GetTwoByteStringCharsAndLength(cx, string, &mut length);
            assert!(!chars.is_null());
            slice::from_raw_parts(chars, length).to_vec()
        }
    }
}

/// Sets `result` to a JavaScript string of the code units of `string`.
#[expect(unsafe_code)]
fn utf16_string_to_jsval(cx: &mut JSContext, string: &Utf16String, mut result: MutableHandleValue) {
    let units = string.units();
    // SAFETY: `units` is valid for `units.len()` code units, which the call copies.
    let js_string = unsafe { JS_NewUCStringCopyN(cx, units.as_ptr(), units.len()) };
    assert!(
        !js_string.is_null(),
        "Failed to convert an IndexedDB string into a JS string"
    );
    // SAFETY: `js_string` is a new, non-null string.
    result.set(StringValue(unsafe { &*js_string }));
}

/// Sets `id` to the property key of the key path identifier `identifier`, made from its code
/// units: the property functions that take a C string read it as Latin-1 on SpiderMonkey, which
/// changes a non-ASCII identifier.
#[expect(unsafe_code)]
fn identifier_id(cx: &mut JSContext, identifier: &str, id: MutableHandleId) -> Result<(), Error> {
    let units: Vec<u16> = identifier.encode_utf16().collect();
    // SAFETY: `units` is valid for `units.len()` code units, which the call copies.
    rooted!(&in(cx) let string = unsafe { JS_AtomizeUCStringN(cx, units.as_ptr(), units.len()) });
    // SAFETY: `string` is rooted and not null.
    if string.is_null() || !unsafe { JS_StringToId(cx, string.handle(), id) } {
        return Err(Error::JSFailed);
    }
    Ok(())
}

/// ? HasOwnProperty(object, id).
#[expect(unsafe_code)]
fn has_own_identifier(
    cx: &mut JSContext,
    object: HandleObject,
    id: HandleId,
) -> Result<bool, Error> {
    let mut found = false;
    // SAFETY: `object` and `id` are rooted.
    if !unsafe { JS_HasOwnPropertyById(cx, object, id, &mut found) } {
        return Err(Error::JSFailed);
    }
    Ok(found)
}

/// ? Get(object, id).
fn get_identifier(
    cx: &mut JSContext,
    object: HandleObject,
    id: HandleId,
    value: MutableHandleValue,
) -> Result<(), Error> {
    if !JS_GetPropertyById(cx, object, id, value) {
        return Err(Error::JSFailed);
    }
    Ok(())
}

/// CreateDataProperty(object, id, value).
#[expect(unsafe_code)]
fn create_identifier(
    cx: &mut JSContext,
    object: HandleObject,
    id: HandleId,
    value: HandleValue,
) -> Result<(), Error> {
    let attributes =
        (PropertyFlags::ENUMERABLE | PropertyFlags::WRITABLE | PropertyFlags::CONFIGURABLE)
            .as_jsprop_flags();
    // SAFETY: `object`, `id` and `value` are rooted.
    if !unsafe { JS_DefinePropertyById2(cx, object, id, value, attributes) } {
        return Err(Error::JSFailed);
    }
    Ok(())
}

/// The name (<https://w3c.github.io/IndexedDB/#name>) that the DOMString `name` gives.
pub(crate) fn name_from_dom_string(name: &DOMString) -> Utf16String {
    Utf16String::from(&*name.str())
}

/// The DOMString of the name `name`.
pub(crate) fn dom_string_from_name(name: &Utf16String) -> DOMString {
    DOMString::from(name.to_string_lossy())
}

/// <https://w3c.github.io/IndexedDB/#sorted-name-list> of `names`, sorted by code unit.
pub(crate) fn sorted_name_list(
    cx: &mut JSContext,
    global: &GlobalScope,
    names: impl IntoIterator<Item = Utf16String>,
) -> DomRoot<DOMStringList> {
    let names = names
        .into_iter()
        .sorted_unstable()
        .map(|name| dom_string_from_name(&name))
        .collect();
    DOMStringList::new(cx, global, names)
}

// https://www.w3.org/TR/IndexedDB-3/#convert-key-to-value
#[expect(unsafe_code)]
pub fn key_type_to_jsval(
    cx: &mut JSContext,
    key: &IndexedDBKeyType,
    mut result: MutableHandleValue,
) {
    // Step 1. Let type be key’s type.
    // Step 2. Let value be key’s value.
    // Step 3. Switch on type:
    match key {
        // Step 3. If type is number, return an ECMAScript Number value equal to value.
        IndexedDBKeyType::Number(n) => result.set(DoubleValue(*n)),

        // Step 3. If type is string, return an ECMAScript String value equal to value.
        IndexedDBKeyType::String(s) => utf16_string_to_jsval(cx, s, result),

        IndexedDBKeyType::Date(d) => unsafe {
            // Step 3.1. Let date be the result of executing the ECMAScript Date
            // constructor with the single argument value.
            let date = NewDateObject(cx, ClippedTime { t: *d });

            // Step 3.2. Assert: date is not an abrupt completion.
            assert!(
                !date.is_null(),
                "Failed to convert IndexedDB date key into a Date"
            );

            // Step 3.3. Return date.
            date.to_jsval(cx, result);
        },

        IndexedDBKeyType::Binary(b) => {
            // Step 3.1. Let len be value’s length.

            // Step 3.2. Let buffer be the result of executing the ECMAScript
            // ArrayBuffer constructor with len.
            rooted!(&in(cx) let mut buffer = ptr::null_mut::<js::jsapi::JSObject>());

            // Step 3.3. Assert: buffer is not an abrupt completion.

            // Step 3.4. Set the entries in buffer’s [[ArrayBufferData]] internal slot to the
            // entries in value.
            assert!(
                create_buffer_source::<ArrayBufferU8>(cx, b, buffer.handle_mut()).is_ok(),
                "Failed to convert IndexedDB binary key into an ArrayBuffer"
            );

            // Step 3.5. Return buffer.
            result.set(ObjectValue(buffer.get()));
        },

        IndexedDBKeyType::Array(a) => unsafe {
            // Step 3.1. Let array be the result of executing the ECMAScript Array
            // constructor with no arguments.
            rooted!(&in(cx) let array = NewArrayObject1(cx.raw_cx(), 0));

            // Step 3.2. Assert: array is not an abrupt completion.
            assert!(
                !array.get().is_null(),
                "Failed to convert IndexedDB array key into an Array"
            );

            // Step 3.3. Let len be value’s size.
            let len = a.len();

            // Step 3.4. Let index be 0.
            let mut index = 0;

            // Step 3.5. While index is less than len:
            while index < len {
                // Step 3.5.1. Let entry be the result of converting a key to a value with
                // value[index].
                rooted!(&in(cx) let mut entry = UndefinedValue());
                key_type_to_jsval(cx, &a[index], entry.handle_mut());

                // Step 3.5.2. Let status be CreateDataProperty(array, index, entry).
                let index_property = CString::new(index.to_string());
                assert!(
                    index_property.is_ok(),
                    "Failed to convert IndexedDB array index to CString"
                );
                let index_property = index_property.unwrap();
                let status = define_dictionary_property(
                    cx,
                    array.handle(),
                    index_property.as_c_str(),
                    entry.handle(),
                );

                // Step 3.5.3. Assert: status is true.
                assert!(
                    status.is_ok(),
                    "CreateDataProperty on a fresh JS array should not fail"
                );

                // Step 3.5.4. Increase index by 1.
                index += 1;
            }

            // Step 3.6. Return array.
            result.set(ObjectValue(array.get()));
        },
    }
}

/// <https://www.w3.org/TR/IndexedDB-3/#valid-key-path>
pub(crate) fn is_valid_key_path(
    cx: &mut JSContext,
    key_path: &StrOrStringSequence,
) -> Result<bool, Error> {
    // <https://tc39.es/ecma262/#prod-IdentifierName>
    #[expect(unsafe_code)]
    let is_identifier_name = |cx: &mut JSContext, name: &str| -> Result<bool, Error> {
        rooted!(&in(cx) let mut value = UndefinedValue());
        name.to_jsval(cx, value.handle_mut());
        rooted!(&in(cx) let string = value.to_string());

        unsafe {
            let mut is_identifier = false;
            if !JS_IsIdentifier(cx, string.handle(), &mut is_identifier) {
                return Err(Error::JSFailed);
            }
            Ok(is_identifier)
        }
    };

    // A valid key path is one of:
    let is_valid = |cx: &mut JSContext, path: &DOMString| -> Result<bool, Error> {
        // An empty string.
        let is_empty_string = path.is_empty();

        // An identifier, which is a string matching the IdentifierName production from the
        // ECMAScript Language Specification [ECMA-262].
        let is_identifier = is_identifier_name(cx, &path.str())?;

        // A string consisting of two or more identifiers separated by periods (U+002E FULL STOP).
        let is_identifier_list = path
            .str()
            .split('.')
            .map(|s| is_identifier_name(cx, s))
            .try_collect::<bool, Vec<bool>, Error>()?
            .iter()
            .all(|&value| value);

        Ok(is_empty_string || is_identifier || is_identifier_list)
    };

    match key_path {
        StrOrStringSequence::StringSequence(paths) => {
            // A non-empty list containing only strings conforming to the above requirements.
            if paths.is_empty() {
                Ok(false)
            } else {
                Ok(paths
                    .iter()
                    .map(|s| is_valid(cx, s))
                    .try_collect::<bool, Vec<bool>, Error>()?
                    .iter()
                    .all(|&value| value))
            }
        },
        StrOrStringSequence::String(path) => is_valid(cx, path),
    }
}

pub(crate) enum ConversionResult {
    Valid(IndexedDBKeyType),
    Invalid,
}

impl ConversionResult {
    pub fn into_result(self) -> Result<IndexedDBKeyType, Error> {
        match self {
            ConversionResult::Valid(key) => Ok(key),
            ConversionResult::Invalid => Err(Error::Data(None)),
        }
    }
}

// https://www.w3.org/TR/IndexedDB-3/#convert-value-to-key
#[expect(unsafe_code)]
pub fn convert_value_to_key(
    cx: &mut JSContext,
    input: HandleValue,
    seen: Option<Vec<HandleValue>>,
) -> Result<ConversionResult, Error> {
    // Step 1: If seen was not given, then let seen be a new empty set.
    let mut seen = seen.unwrap_or_default();

    // Step 2: If seen contains input, then return "invalid value".
    for seen_input in &seen {
        let mut same = false;
        if unsafe { !SameValue(cx, *seen_input, input, &mut same) } {
            return Err(Error::JSFailed);
        }
        if same {
            return Ok(ConversionResult::Invalid);
        }
    }

    // Step 3. Jump to the appropriate step below.

    // If Type(input) is Number:
    if input.is_number() {
        // 3.1. If input is NaN then return "invalid value".
        if input.to_number().is_nan() {
            return Ok(ConversionResult::Invalid);
        }
        // 3.2. Otherwise, return a new key with type number and value input.
        return Ok(ConversionResult::Valid(IndexedDBKeyType::Number(
            input.to_number(),
        )));
    }

    // If Type(input) is String:
    if input.is_string() {
        // 3.1. Return a new key with type string and value input.
        let units = js_string_code_units(cx, input.to_string());
        return Ok(ConversionResult::Valid(IndexedDBKeyType::String(
            Utf16String::from_units(units),
        )));
    }

    if input.is_object() {
        rooted!(&in(cx) let object = input.to_object());
        unsafe {
            let mut is_date = false;
            if !ObjectIsDate(cx, object.handle(), &mut is_date) {
                return Err(Error::JSFailed);
            }

            // If input is a Date (has a [[DateValue]] internal slot):
            if is_date {
                // 3.1. Let ms be the value of input's [[DateValue]] internal slot.
                let mut ms = f64::NAN;
                if !js::rust::wrappers2::DateGetMsecSinceEpoch(cx, object.handle(), &mut ms) {
                    return Err(Error::JSFailed);
                }
                // 3.2. If ms is NaN then return "invalid value".
                if ms.is_nan() {
                    return Ok(ConversionResult::Invalid);
                }
                // 3.3. Otherwise, return a new key with type date and value ms.
                return Ok(ConversionResult::Valid(IndexedDBKeyType::Date(ms)));
            }

            // If input is a buffer source type:
            if IsArrayBufferObject(*object) || JS_IsArrayBufferViewObject(*object) {
                let is_detached = if IsArrayBufferObject(*object) {
                    IsDetachedArrayBufferObject(*object)
                } else {
                    // Shared ArrayBuffers are not supported here, so this stays false.
                    let mut is_shared = false;
                    rooted!(
                        in (cx.raw_cx()) let view_buffer =
                            JS_GetArrayBufferViewBuffer(
                                cx.raw_cx(),
                                object.handle().into(),
                                &mut is_shared
                            )
                    );
                    !is_shared && IsDetachedArrayBufferObject(*view_buffer.handle())
                };
                // 3.1. If input is detached then return "invalid value".
                if is_detached {
                    return Ok(ConversionResult::Invalid);
                }
                // 3.2. Let bytes be the result of getting a copy of the bytes held
                // by the buffer source input.
                let bytes = if IsArrayBufferObject(*object) {
                    let array_buffer = ArrayBuffer::from(*object).map_err(|()| Error::JSFailed)?;
                    array_buffer.to_vec()
                } else {
                    let array_buffer_view =
                        ArrayBufferView::from(*object).map_err(|()| Error::JSFailed)?;
                    array_buffer_view.to_vec()
                }
                .expect("Already checked for detached buffers");
                // 3.3. Return a new key with type binary and value bytes.
                return Ok(ConversionResult::Valid(IndexedDBKeyType::Binary(bytes)));
            }

            // If input is an Array exotic object:
            let mut is_array = false;
            if !IsArrayObject(cx, input, &mut is_array) {
                return Err(Error::JSFailed);
            }
            if is_array {
                // 3.1. Let len be ? ToLength( ? Get(input, "length")).
                let mut len = 0;
                if !GetArrayLength(cx, object.handle(), &mut len) {
                    return Err(Error::JSFailed);
                }
                // 3.2. Append input to seen.
                seen.push(input);
                // 3.3. Let keys be a new empty list.
                let mut keys = vec![];
                // 3.4. Let index be 0.
                let mut index: u32 = 0;
                // 3.5. While index is less than len:
                while index < len {
                    rooted!(&in(cx) let mut id: PropertyKey);
                    if !JS_IndexToId(cx, index, id.handle_mut()) {
                        return Err(Error::JSFailed);
                    }
                    // 3.5.1. Let hop be ? HasOwnProperty(input, index).
                    let mut hop = false;
                    if !JS_HasOwnPropertyById(cx, object.handle(), id.handle(), &mut hop) {
                        return Err(Error::JSFailed);
                    }
                    // 3.5.2. If hop is false, return "invalid value".
                    if !hop {
                        return Ok(ConversionResult::Invalid);
                    }
                    // 3.5.3. Let entry be ? Get(input, index).
                    rooted!(&in(cx) let mut entry = UndefinedValue());
                    if !JS_GetPropertyById(cx, object.handle(), id.handle(), entry.handle_mut()) {
                        return Err(Error::JSFailed);
                    }

                    // 3.5.4. Let key be the result of converting a value to a key
                    //        with arguments entry and seen.
                    // 3.5.5. ReturnIfAbrupt(key).
                    let key = match convert_value_to_key(cx, entry.handle(), Some(seen.clone()))? {
                        ConversionResult::Valid(key) => key,
                        // 3.5.6. If key is "invalid value" or "invalid type"
                        //        abort these steps and return "invalid value".
                        ConversionResult::Invalid => return Ok(ConversionResult::Invalid),
                    };
                    // 3.5.7. Append key to keys.
                    keys.push(key);
                    // 3.5.8. Increase index by 1.
                    index += 1;
                }
                // 3.6. Return a new array key with value keys.
                return Ok(ConversionResult::Valid(IndexedDBKeyType::Array(keys)));
            }
        }
    }

    // Otherwise, return "invalid type".
    Ok(ConversionResult::Invalid)
}

/// <https://w3c.github.io/IndexedDB/#convert-a-value-to-a-multientry-key>, except that the keys
/// of an array key are in key order rather than in the order of the array: each becomes a record
/// of a multiEntry index, so their order isn't observable.
#[expect(unsafe_code)]
pub(crate) fn convert_value_to_multientry_key(
    cx: &mut JSContext,
    input: HandleValue,
) -> Result<ConversionResult, Error> {
    // Step 1. If input is an Array exotic object, then:
    let mut is_array = false;
    // SAFETY: `input` is rooted.
    if !unsafe { IsArrayObject(cx, input, &mut is_array) } {
        return Err(Error::JSFailed);
    }
    if !is_array {
        // Step 2. Otherwise, return the result of converting a value to a key with argument
        // input. Rethrow any exceptions.
        return convert_value_to_key(cx, input, None);
    }
    rooted!(&in(cx) let object = input.to_object());

    // Step 1.1. Let len be ? ToLength( ? Get(input, "length")).
    let mut len = 0;
    // SAFETY: `object` is rooted.
    if !unsafe { GetArrayLength(cx, object.handle(), &mut len) } {
        return Err(Error::JSFailed);
    }
    // Step 1.2. Let seen be a new set containing only input.
    // Step 1.3. Let keys be a new empty list.
    let mut keys = vec![];
    // Step 1.4. Let index be 0.
    // Step 1.5. While index is less than len:
    for index in 0..len {
        // Step 1.5.1. Let entry be Get(input, index).
        rooted!(&in(cx) let mut id: PropertyKey);
        rooted!(&in(cx) let mut entry = UndefinedValue());
        // SAFETY: `id` is rooted.
        let got = unsafe { JS_IndexToId(cx, index, id.handle_mut()) } &&
            JS_GetPropertyById(cx, object.handle(), id.handle(), entry.handle_mut());
        // Step 1.5.2. If entry is not an abrupt completion, then:
        if !got {
            clear_pending_exception(cx);
            continue;
        }
        // Step 1.5.2.1. Let key be the result of converting a value to a key with arguments entry
        // and seen.
        // Step 1.5.2.2. If key is not "invalid value" or "invalid type" or an abrupt completion,
        // and there is no item in keys equal to key, then append key to keys.
        match convert_value_to_key(cx, entry.handle(), Some(vec![input])) {
            Ok(ConversionResult::Valid(key)) => keys.push(key),
            Ok(ConversionResult::Invalid) => {},
            Err(_) => clear_pending_exception(cx),
        }
        // Step 1.5.3. Increase index by 1.
    }
    // NOTE: Removing the duplicates after sorting takes O(n log n) comparisons rather than the
    // spec's O(n²). Valid keys never compare as unordered.
    keys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    keys.dedup();
    // Step 1.6. Return a new array key with value set to keys.
    Ok(ConversionResult::Valid(IndexedDBKeyType::Array(keys)))
}

/// Clears the exception of a failed JavaScript operation whose failure is ignored.
#[expect(unsafe_code)]
pub(crate) fn clear_pending_exception(cx: &mut JSContext) {
    if JS_IsExceptionPending(cx) {
        // SAFETY: `cx` is the context the exception is pending on.
        unsafe { JS_ClearPendingException(cx) };
    }
}

/// <https://www.w3.org/TR/IndexedDB-3/#convert-a-value-to-a-key-range>
pub fn convert_value_to_key_range(
    cx: &mut JSContext,
    input: HandleValue,
    null_disallowed: Option<bool>,
) -> Result<IndexedDBKeyRange, Error> {
    // Step 1. If value is a key range, return value.
    if let Ok(obj) = root_from_handlevalue::<IDBKeyRange>(cx, input) {
        let obj = obj.inner().clone();
        return Ok(obj);
    }

    // Step 2. If value is undefined or is null, then throw a "DataError" DOMException if null
    // disallowed flag is set, or return an unbounded key range otherwise.
    if input.get().is_undefined() || input.get().is_null() {
        if null_disallowed.is_some_and(|flag| flag) {
            return Err(Error::Data(None));
        } else {
            return Ok(IndexedDBKeyRange {
                lower: None,
                upper: None,
                lower_open: Default::default(),
                upper_open: Default::default(),
            });
        }
    }

    // Step 3. Let key be the result of running the steps to convert a value to a key with value.
    // Rethrow any exceptions.
    let key = convert_value_to_key(cx, input, None)?;

    // Step 4. If key is invalid, throw a "DataError" DOMException.
    let key = key.into_result()?;

    // Step 5. Return a key range containing only key.
    Ok(IndexedDBKeyRange::only(key))
}

pub(crate) fn map_backend_error_to_dom_error(error: BackendError) -> Error {
    match error {
        BackendError::QuotaExceeded => Error::QuotaExceeded {
            quota: None,
            requested: None,
        },
        BackendError::DbErr(details) => {
            Error::Operation(Some(format!("IndexedDB open failed: {details}")))
        },
        other => Error::Operation(Some(format!("IndexedDB open failed: {other:?}"))),
    }
}

/// The error that a transaction aborts with when the backend, or script on its behalf, failed
/// it: a "QuotaExceededError" for a full disk, a "ConstraintError" for a unique index that got
/// the same key for two records while it was populated, and otherwise an "UnknownError" with a
/// neutral message, as <https://w3c.github.io/IndexedDB/#commit-a-transaction> step 2.4 suggests;
/// the details only go to the log.
pub(crate) fn map_transaction_error_to_dom_error(error: BackendError) -> Error {
    match error {
        BackendError::QuotaExceeded => Error::QuotaExceeded {
            quota: None,
            requested: None,
        },
        BackendError::Constraint => Error::Constraint(Some(
            "A unique index has the same key for two records".to_owned(),
        )),
        BackendError::Abort => Error::Abort(Some("The transaction was aborted".to_owned())),
        other => {
            warn!("IndexedDB transaction failed: {other}");
            Error::Unknown(Some("The IndexedDB transaction failed".to_owned()))
        },
    }
}

/// The result of steps in
/// <https://www.w3.org/TR/IndexedDB-3/#evaluate-a-key-path-on-a-value>
pub(crate) enum EvaluationResult {
    Success,
    Failure,
}

/// <https://www.w3.org/TR/IndexedDB-3/#evaluate-a-key-path-on-a-value>
#[expect(unsafe_code)]
pub(crate) fn evaluate_key_path_on_value(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &KeyPath,
    mut return_val: MutableHandleValue,
) -> Result<EvaluationResult, Error> {
    match key_path {
        // Step 1. If keyPath is a list of strings, then:
        KeyPath::StringSequence(key_path) => {
            // Step 1.1. Let result be a new Array object created as if by the expression [].
            // SAFETY: `cx` is a live context; the new array is rooted right away.
            rooted!(&in(cx) let mut result = unsafe { NewArrayObject1(cx.raw_cx(), 0) });
            if result.is_null() {
                return Err(Error::JSFailed);
            }

            // Step 1.2. Let i be 0.
            // Step 1.3. For each item in keyPath:
            for (i, item) in key_path.iter().enumerate() {
                // Step 1.3.1. Let key be the result of recursively running the steps to evaluate a key
                // path on a value using item as keyPath and value as value.
                // Step 1.3.2. Assert: key is not an abrupt completion.
                // Step 1.3.3. If key is failure, abort the overall algorithm and return failure.
                rooted!(&in(cx) let mut key = UndefinedValue());
                if let EvaluationResult::Failure = evaluate_key_path_on_value(
                    cx,
                    value,
                    &KeyPath::String(item.clone()),
                    key.handle_mut(),
                )? {
                    return Ok(EvaluationResult::Failure);
                };

                // Step 1.3.4. Let p be ! ToString(i).
                // Step 1.3.5. Let status be CreateDataProperty(result, p, key).
                // Step 1.3.6. Assert: status is true.
                let i_cstr = std::ffi::CString::new(i.to_string()).unwrap();
                define_dictionary_property(cx, result.handle(), i_cstr.as_c_str(), key.handle())
                    .map_err(|_| Error::JSFailed)?;

                // Step 1.3.7. Increase i by 1.
                // Done by for loop with enumerate()
            }

            // Step 1.4. Return result.
            result.to_jsval(cx, return_val);
        },
        KeyPath::String(key_path) => {
            // Step 2. If keyPath is the empty string, return value and skip the remaining steps.
            if key_path.is_empty() {
                return_val.set(*value);
                return Ok(EvaluationResult::Success);
            }

            // NOTE: Use current_value, instead of value described in spec, in the following steps.
            rooted!(&in(cx) let mut current_value = *value);

            // Step 3. Let identifiers be the result of strictly splitting keyPath on U+002E
            // FULL STOP characters (.).
            // Step 4. For each identifier of identifiers, jump to the appropriate step below:
            for identifier in key_path.str().split('.') {
                // If Type(value) is String, and identifier is "length"
                if identifier == "length" && current_value.is_string() {
                    // Let value be a Number equal to the number of elements in value.
                    rooted!(&in(cx) let string_value = current_value.to_string());
                    unsafe {
                        let string_length = JS_GetStringLength(*string_value) as u64;
                        string_length.to_jsval(cx, current_value.handle_mut());
                    }
                    continue;
                }

                // If value is an Array and identifier is "length"
                if identifier == "length" {
                    let mut is_array = false;
                    if unsafe { !IsArrayObject(cx, current_value.handle(), &mut is_array) } {
                        return Err(Error::JSFailed);
                    }
                    if is_array {
                        // Let value be ! ToLength(! Get(value, "length")).
                        rooted!(&in(cx) let object = current_value.to_object());
                        get_property_jsval(
                            cx,
                            object.handle(),
                            c"length",
                            current_value.handle_mut(),
                        )?;

                        continue;
                    }
                }

                // If value is a Blob and identifier is "size"
                if identifier == "size" &&
                    let Ok(blob) = root_from_handlevalue::<Blob>(cx, current_value.handle())
                {
                    // Let value be a Number equal to value’s size.
                    blob.Size().to_jsval(cx, current_value.handle_mut());

                    continue;
                }

                // If value is a Blob and identifier is "type"
                if identifier == "type" &&
                    let Ok(blob) = root_from_handlevalue::<Blob>(cx, current_value.handle())
                {
                    // Let value be a String equal to value’s type.
                    blob.Type().to_jsval(cx, current_value.handle_mut());

                    continue;
                }

                // If value is a File and identifier is "name"
                if identifier == "name" &&
                    let Ok(file) = root_from_handlevalue::<File>(cx, current_value.handle())
                {
                    // Let value be a String equal to value’s name.
                    file.name()
                        .clone()
                        .root()
                        .to_jsval(cx, current_value.handle_mut());

                    continue;
                }

                // If value is a File and identifier is "lastModified"
                if identifier == "lastModified" &&
                    let Ok(file) = root_from_handlevalue::<File>(cx, current_value.handle())
                {
                    // Let value be a Number equal to value’s lastModified.
                    file.LastModified().to_jsval(cx, current_value.handle_mut());

                    continue;
                }

                // If value is a File and identifier is "lastModifiedDate"
                if identifier == "lastModifiedDate" &&
                    let Ok(file) = root_from_handlevalue::<File>(cx, current_value.handle())
                {
                    // Let value be a new Date object with [[DateValue]] internal slot equal to value’s lastModified.
                    let time = ClippedTime {
                        t: file.LastModified() as f64,
                    };
                    unsafe {
                        NewDateObject(cx, time).to_jsval(cx, current_value.handle_mut());
                    }

                    continue;
                }

                // Otherwise
                // If Type(value) is not Object, return failure.
                if !current_value.is_object() {
                    return Ok(EvaluationResult::Failure);
                }

                rooted!(&in(cx) let object = current_value.to_object());
                rooted!(&in(cx) let mut id: PropertyKey);
                identifier_id(cx, identifier, id.handle_mut())?;

                // Let hop be ! HasOwnProperty(value, identifier).
                let hop = has_own_identifier(cx, object.handle(), id.handle())?;

                // If hop is false, return failure.
                if !hop {
                    return Ok(EvaluationResult::Failure);
                }

                // Let value be ! Get(value, identifier).
                get_identifier(cx, object.handle(), id.handle(), current_value.handle_mut())?;

                // If value is undefined, return failure.
                if current_value.get().is_undefined() {
                    return Ok(EvaluationResult::Failure);
                }
            }

            // Step 5. Assert: value is not an abrupt completion.
            // Done within Step 4.

            // Step 6. Return value.
            return_val.set(*current_value);
        },
    }
    Ok(EvaluationResult::Success)
}

/// The result of steps in
/// <https://www.w3.org/TR/IndexedDB-3/#extract-a-key-from-a-value-using-a-key-path>
pub(crate) enum ExtractionResult {
    Key(IndexedDBKeyType),
    Invalid,
    Failure,
}

/// <https://w3c.github.io/IndexedDB/#check-that-a-key-could-be-injected-into-a-value>
pub(crate) fn can_inject_key_into_value(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &DOMString,
) -> Result<bool, Error> {
    // Step 1. Let identifiers be the result of strictly splitting keyPath on U+002E FULL STOP
    // characters (.).
    let key_path_string = key_path.str();
    let mut identifiers: Vec<&str> = key_path_string.split('.').collect();

    // Step 2. Assert: identifiers is not empty.
    let Some(_) = identifiers.pop() else {
        return Ok(false);
    };

    rooted!(&in(cx) let mut current_value = *value);

    // Step 3. For each remaining identifier of identifiers:
    for identifier in identifiers {
        // Step 3.1. If value is not an Object or an Array, return false.
        if !current_value.is_object() {
            return Ok(false);
        }

        rooted!(&in(cx) let current_object = current_value.to_object());
        rooted!(&in(cx) let mut id: PropertyKey);
        identifier_id(cx, identifier, id.handle_mut())?;

        // Step 3.2. Let hop be ? HasOwnProperty(value, identifier).
        let hop = has_own_identifier(cx, current_object.handle(), id.handle())?;

        // Step 3.3. If hop is false, set value to a new Object created as if by the expression
        // ({}).
        // We avoid mutating `value` during this check and can return true immediately because the
        // remaining path can be created from scratch.
        if !hop {
            return Ok(true);
        }

        // Step 3.4. Set value to ? Get(value, identifier).
        get_identifier(
            cx,
            current_object.handle(),
            id.handle(),
            current_value.handle_mut(),
        )?;
    }

    // Step 4. Return true if value is an Object or an Array, and false otherwise.
    Ok(current_value.is_object())
}

/// Whether a key that <https://w3c.github.io/IndexedDB/#inject-a-key-into-a-value-using-a-key-path>
/// puts into `value` at `key_path` is stored with the value. StructuredSerializeForStorage keeps
/// the own properties of ordinary objects and Arrays only, so a key that attaches to any other
/// object, such as a Date, a Map, a Blob or a typed array, or to an object below one, is dropped.
/// The objects that injecting creates are ordinary ones.
#[expect(unsafe_code)]
pub(crate) fn injected_key_is_stored(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &DOMString,
) -> Result<bool, Error> {
    let key_path = key_path.str();
    let mut identifiers = key_path.split('.').peekable();
    rooted!(&in(cx) let mut current_value = *value);
    while let Some(identifier) = identifiers.next() {
        if !current_value.is_object() {
            return Ok(false);
        }
        rooted!(&in(cx) let object = current_value.to_object());
        let mut class = ESClass::Other;
        if !unsafe { GetBuiltinClass(cx, object.handle(), &mut class) } {
            return Err(Error::JSFailed);
        }
        if class != ESClass::Object && class != ESClass::Array {
            return Ok(false);
        }
        if identifiers.peek().is_none() {
            break;
        }
        rooted!(&in(cx) let mut id: PropertyKey);
        identifier_id(cx, identifier, id.handle_mut())?;
        if !has_own_identifier(cx, object.handle(), id.handle())? {
            break;
        }
        get_identifier(cx, object.handle(), id.handle(), current_value.handle_mut())?;
    }
    Ok(true)
}

/// <https://w3c.github.io/IndexedDB/#inject-a-key-into-a-value-using-a-key-path>
pub(crate) fn inject_key_into_value(
    cx: &mut JSContext,
    value: HandleValue,
    key: &IndexedDBKeyType,
    key_path: &DOMString,
) -> Result<bool, Error> {
    // Step 1. Let identifiers be the result of strictly splitting keyPath on U+002E FULL STOP characters (.).
    let key_path_string = key_path.str();
    let mut identifiers: Vec<&str> = key_path_string.split('.').collect();

    // Step 2. Assert: identifiers is not empty.
    let Some(last) = identifiers.pop() else {
        return Ok(false);
    };

    // Step 3. Let last be the last item of identifiers and remove it from the list.
    // Done by `pop()` above.

    rooted!(&in(cx) let mut current_value = *value);

    // Step 4. For each remaining identifier of identifiers:
    for identifier in identifiers {
        // Step 4.1 Assert: value is an Object or an Array.
        if !current_value.is_object() {
            return Ok(false);
        }

        rooted!(&in(cx) let current_object = current_value.to_object());
        rooted!(&in(cx) let mut id: PropertyKey);
        identifier_id(cx, identifier, id.handle_mut())?;

        // Step 4.2 Let hop be ! HasOwnProperty(value, identifier).
        let hop = has_own_identifier(cx, current_object.handle(), id.handle())?;

        // Step 4.3 If hop is false, then:
        if !hop {
            // Step 4.3.1 Let o be a new Object created as if by the expression ({}).
            rooted!(&in(cx) let o = JS_NewObject(cx, None));
            rooted!(&in(cx) let mut o_value = UndefinedValue());
            o.to_jsval(cx, o_value.handle_mut());

            // Step 4.3.2 Let status be CreateDataProperty(value, identifier, o).
            create_identifier(cx, current_object.handle(), id.handle(), o_value.handle())?;

            // Step 4.3.3 Assert: status is true.
        }

        // Step 4.3 Let value be ! Get(value, identifier).
        get_identifier(
            cx,
            current_object.handle(),
            id.handle(),
            current_value.handle_mut(),
        )?;

        // Step 5 "Assert: value is an Object or an Array."
        if !current_value.is_object() {
            return Ok(false);
        }
    }

    // Step 6. Let keyValue be the result of converting a key to a value with key.
    rooted!(&in(cx) let mut key_value = UndefinedValue());
    key_type_to_jsval(cx, key, key_value.handle_mut());

    // `current_value` is the parent object where `last` will be defined.
    if !current_value.is_object() {
        return Ok(false);
    }
    rooted!(&in(cx) let parent_object = current_value.to_object());
    rooted!(&in(cx) let mut last_id: PropertyKey);
    identifier_id(cx, last, last_id.handle_mut())?;

    // Step 7. Let status be CreateDataProperty(value, last, keyValue).
    create_identifier(
        cx,
        parent_object.handle(),
        last_id.handle(),
        key_value.handle(),
    )?;

    // Step 8. Assert: status is true.
    // The JS_DefineProperty success check above enforces this assertion.
    // "NOTE: Assertions can be made in the above steps because this algorithm is only applied to values that are the output of StructuredDeserialize, and the steps to check that a key could be injected into a value have been run."
    Ok(true)
}

/// <https://www.w3.org/TR/IndexedDB-3/#extract-a-key-from-a-value-using-a-key-path>
pub(crate) fn extract_key(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &KeyPath,
    multi_entry: Option<bool>,
) -> Result<ExtractionResult, Error> {
    // Step 1. Let r be the result of running the steps to evaluate a key path on a value with
    // value and keyPath. Rethrow any exceptions.
    // Step 2. If r is failure, return failure.
    rooted!(&in(cx) let mut r = UndefinedValue());
    if let EvaluationResult::Failure =
        evaluate_key_path_on_value(cx, value, key_path, r.handle_mut())?
    {
        return Ok(ExtractionResult::Failure);
    }

    // Step 3. Let key be the result of running the steps to convert a value to a key with r if the
    // multiEntry flag is unset, and the result of running the steps to convert a value to a
    // multiEntry key with r otherwise. Rethrow any exceptions.
    let key = match multi_entry {
        Some(true) => convert_value_to_multientry_key(cx, r.handle())?,
        _ => convert_value_to_key(cx, r.handle(), None)?,
    };
    match key {
        // Step 4. If key is "invalid value" or "invalid type", return invalid.
        ConversionResult::Invalid => Ok(ExtractionResult::Invalid),
        // Step 5. Return key.
        ConversionResult::Valid(key) => Ok(ExtractionResult::Key(key)),
    }
}

/// <https://w3c.github.io/IndexedDB/#store-a-record-into-an-object-store> steps 5.1 and 5.2 for
/// `index`, where `value` is the clone being stored: the keys of the records that `index` gets,
/// without duplicates.
///
/// `generated_key_path` is the store's key path when the backend generates the record's key, which
/// step 1.1.3 injects into the value at that key path before step 5. Script doesn't know the key,
/// so the parts of the index key that come from it are placeholders, and the keys are those that
/// the key path gives on the value with the key injected.
pub(crate) fn index_keys(
    cx: &mut JSContext,
    value: HandleValue,
    index: &IndexedDBIndex,
    generated_key_path: Option<&str>,
) -> Vec<IndexedDBIndexKey> {
    // Step 5.1. Let index key be the result of extracting a key from a value using a key path
    // with value, index’s key path, and index’s multiEntry flag.
    let key_path = KeyPath::from(index.key_path.clone());
    let index_key = match generated_key_path {
        Some(store_key_path) => {
            index_key_with_generated_key(cx, value, &key_path, index.multi_entry, store_key_path)
        },
        None => extracted_index_key(cx, value, &key_path, index.multi_entry),
    };
    // Step 5.2. If index key is an exception, or invalid, or failure, take no further actions for
    // index, and continue these steps for the next index.
    // NOTE: An exception thrown in this step is not rethrown.
    let index_key = match index_key {
        Ok(Some(index_key)) => index_key,
        Ok(None) => return vec![],
        Err(_) => {
            clear_pending_exception(cx);
            return vec![];
        },
    };
    // Step 5.5 and 5.6: a multiEntry index gets a record for each subkey of an array key, any
    // other index one for the key.
    match index_key {
        IndexedDBIndexKey::Key(IndexedDBKeyType::Array(subkeys)) if index.multi_entry => {
            subkeys.into_iter().map(IndexedDBIndexKey::Key).collect()
        },
        index_key => vec![index_key],
    }
}

/// The index key that extracting a key from `value` with `key_path` gives, `None` for invalid or
/// failure.
fn extracted_index_key(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &KeyPath,
    multi_entry: bool,
) -> Result<Option<IndexedDBIndexKey>, Error> {
    Ok(match extract_key(cx, value, key_path, Some(multi_entry))? {
        ExtractionResult::Key(key) => Some(IndexedDBIndexKey::Key(key)),
        ExtractionResult::Invalid | ExtractionResult::Failure => None,
    })
}

/// What evaluating a key path item gives on a value into which a generated key was injected at
/// the store's key path.
enum OnInjectedKey {
    /// The key.
    Key,
    /// Failure: the item goes below the key, which is a number.
    Failure,
    /// The same index key as on the value without the key, or likewise none. The injection only
    /// adds the key's property and possibly ordinary objects on the way to it: an item that
    /// reaches such an object gets an invalid key instead of failure, and an existing object
    /// converts to the same key with the added property.
    Unchanged,
}

fn on_injected_key(item: &str, store_key_path: &str) -> OnInjectedKey {
    match item.strip_prefix(store_key_path) {
        Some("") => OnInjectedKey::Key,
        Some(rest) if rest.starts_with('.') => OnInjectedKey::Failure,
        _ => OnInjectedKey::Unchanged,
    }
}

/// The index key that extracting a key with `key_path` gives on `value` once a generated key is
/// injected into it at `store_key_path`, with the key as a placeholder.
fn index_key_with_generated_key(
    cx: &mut JSContext,
    value: HandleValue,
    key_path: &KeyPath,
    multi_entry: bool,
    store_key_path: &str,
) -> Result<Option<IndexedDBIndexKey>, Error> {
    match key_path {
        KeyPath::String(path) => match on_injected_key(&path.str(), store_key_path) {
            // The key is a number, which a multiEntry index also gets as it is.
            OnInjectedKey::Key => Ok(Some(IndexedDBIndexKey::PrimaryKey)),
            OnInjectedKey::Failure => Ok(None),
            OnInjectedKey::Unchanged => extracted_index_key(cx, value, key_path, multi_entry),
        },
        KeyPath::StringSequence(items) => {
            let on_key: Vec<OnInjectedKey> = items
                .iter()
                .map(|item| on_injected_key(&item.str(), store_key_path))
                .collect();
            if on_key
                .iter()
                .all(|on_key| matches!(on_key, OnInjectedKey::Unchanged))
            {
                return extracted_index_key(cx, value, key_path, multi_entry);
            }
            // Evaluating a sequence fails if an item fails, and converting the array of the items'
            // values gives invalid if one of them is invalid.
            let mut parts = Vec::with_capacity(items.len());
            for (item, on_key) in items.iter().zip(on_key) {
                match on_key {
                    OnInjectedKey::Key => parts.push(IndexedDBIndexKey::PrimaryKey),
                    OnInjectedKey::Failure => return Ok(None),
                    OnInjectedKey::Unchanged => {
                        let item = KeyPath::String(item.clone());
                        match extracted_index_key(cx, value, &item, false)? {
                            Some(part) => parts.push(part),
                            None => return Ok(None),
                        }
                    },
                }
            }
            Ok(Some(IndexedDBIndexKey::Array(parts)))
        },
    }
}
