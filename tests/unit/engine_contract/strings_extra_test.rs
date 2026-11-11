/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The remaining string, atom, id, JSON and memory entry points.

use std::ffi::{CStr, c_char, c_void};
use std::ptr::NonNull;
use std::slice;

use js::context::JSContext;
use js::conversions::jsstr_to_string;
use js::glue::{
    CreateJSExternalStringCallbacks, DeleteJSExternalStringCallbacks,
    JSExternalStringCallbacksTraps, JS_ForgetStringLinearness,
};
use js::jsapi::mozilla::{MallocSizeOf, Range};
use js::jsapi::{
    AtomToLinearString, GetLinearStringCharAt, GetLinearStringLength, JSExternalStringCallbacks,
    JSPropertySpec_Name, JS_GetStringLength, JS_IsIdentifier1, JS_LinearStringEqualsAscii,
    JS_LinearStringEqualsAscii1, JS_PutEscapedLinearString, jsid, JSAtom,
};
use js::jsapi::js::{StringIsArrayIndex, StringIsArrayIndex1};
use js::jsval::{BooleanValue, Int32Value, NullValue, StringValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{
    JS_AtomizeAndPinStringN, JS_AtomizeString, JS_AtomizeStringN, JS_AtomizeUCString,
    JS_AtomizeUCStringN, JS_CopyStringChars, JS_EncodeStringToBuffer,
    JS_GetEmptyString, JS_GetEmptyStringValue,
    JS_GetStringEncodingLength, JS_IsIdentifier, JS_NewExternalStringLatin1,
    JS_NewExternalUCString, JS_NewMaybeExternalStringLatin1, JS_NewMaybeExternalStringUTF8,
    JS_NewMaybeExternalUCString, JS_NewPlainObject, JS_NewStringCopyUTF8N, JS_NewStringCopyUTF8Z,
    JS_NewStringCopyZ, JS_NewUCStringCopyN, JS_NewUCStringCopyZ, JS_PutEscapedString,
    JS_StringEqualsAscii, JS_StringEqualsAscii1, JS_StringToId, JS_EnsureLinearString,
    JS_ParseJSON1, JS_ParseJSON2, JS_ParseJSONWithReviver1, PropertySpecNameEqualsId,
    PropertySpecNameToPermanentId, StringToLinearStringSlow, JS_StringifyWithLengthHint, JS_free,
    JS_malloc, JS_realloc,
    LossyTwoByteCharsToNewLatin1CharsZ, LossyUTF8CharsToNewTwoByteCharsZ,
    RUST_JSID_IS_INT, RUST_JSID_IS_STRING, RUST_JSID_IS_VOID, RUST_JSID_TO_INT,
    RUST_JSID_TO_STRING, ToBooleanSlow, ToInt32Slow, ToInt64Slow, ToJSONMaybeSafely,
    ToNumberSlow, ToObjectSlow, ToStringSlow, ToUint16Slow, ToUint32Slow, ToUint64Slow,
    UTF8CharsToNewTwoByteCharsZ, int_to_jsid,
};
use js::rust::{HandleObject, HandleValue, JSEngine, MutableHandle};

use crate::support::{Observation, Value, eval, record_case, set_global};

#[cfg(not(feature = "js-quickjs"))]
mod encode_abi {
    use std::ffi::{c_char, c_void};
    use std::ptr;

    use js::context::JSContext;
    use js::jsapi::mozilla::Range;
    use js::jsapi::{
        Handle, MutableHandle as RawMutableHandle, JSString, Latin1CharsZ, TwoByteChars,
        TwoByteCharsZ, UTF8Chars, jsid,
    };

    unsafe extern "C" {
        #[link_name = "_Z22JS_EncodeStringToASCIIP9JSContextP8JSString"]
        fn js_encode_string_to_ascii(
            out: *mut *mut c_char,
            cx: *mut c_void,
            string: *mut JSString,
        );
        #[link_name = "_Z23JS_EncodeStringToLatin1P9JSContextP8JSString"]
        fn js_encode_string_to_latin1(
            out: *mut *mut c_char,
            cx: *mut c_void,
            string: *mut JSString,
        );
        #[link_name = "_Z21JS_EncodeStringToUTF8P9JSContextN2JS6HandleIP8JSStringEE"]
        fn js_encode_string_to_utf8(
            out: *mut *mut c_char,
            cx: *mut c_void,
            string: Handle<*mut JSString>,
        );
        #[link_name = "_Z12JS_CharsToIdP9JSContextN2JS12TwoByteCharsENS1_13MutableHandleINS1_11PropertyKeyEEE"]
        fn js_chars_to_id(
            cx: *mut c_void,
            chars: *const TwoByteChars,
            id: RawMutableHandle<jsid>,
        ) -> bool;
        #[link_name = "_ZN2JS34LossyTwoByteCharsToNewLatin1CharsZEP9JSContextRKN7mozilla5RangeIKDsEE"]
        fn js_lossy_two_byte_to_latin1(
            out: *mut Latin1CharsZ,
            cx: *mut c_void,
            chars: *const Range<u16>,
        );
        #[link_name = "_ZN2JS27UTF8CharsToNewTwoByteCharsZEP9JSContextRKNS_9UTF8CharsEPmm"]
        fn js_utf8_to_two_byte(
            out: *mut TwoByteCharsZ,
            cx: *mut c_void,
            chars: *const UTF8Chars,
            out_len: *mut usize,
            arena: u32,
        );
        #[link_name = "_ZN2JS32LossyUTF8CharsToNewTwoByteCharsZEP9JSContextRKNS_9UTF8CharsEPmm"]
        fn js_lossy_utf8_to_two_byte(
            out: *mut TwoByteCharsZ,
            cx: *mut c_void,
            chars: *const UTF8Chars,
            out_len: *mut usize,
            arena: u32,
        );
    }

    pub unsafe fn encode_string_to_ascii(cx: &mut JSContext, string: *mut JSString) -> *mut c_char {
        let mut out = ptr::null_mut();
        js_encode_string_to_ascii(&mut out, cx.raw_cx().cast(), string);
        out
    }

    pub unsafe fn encode_string_to_latin1(cx: &mut JSContext, string: *mut JSString) -> *mut c_char {
        let mut out = ptr::null_mut();
        js_encode_string_to_latin1(&mut out, cx.raw_cx().cast(), string);
        out
    }

    pub unsafe fn encode_string_to_utf8(
        cx: &mut JSContext,
        string: Handle<*mut JSString>,
    ) -> *mut c_char {
        let mut out = ptr::null_mut();
        js_encode_string_to_utf8(&mut out, cx.raw_cx().cast(), string);
        out
    }

    pub unsafe fn chars_to_id(
        cx: &mut JSContext,
        chars: TwoByteChars,
        id: RawMutableHandle<jsid>,
    ) -> bool {
        js_chars_to_id(cx.raw_cx().cast(), &chars, id)
    }

    pub unsafe fn lossy_two_byte_to_latin1(
        cx: &mut JSContext,
        chars: &Range<u16>,
    ) -> Latin1CharsZ {
        let mut out = std::mem::MaybeUninit::uninit();
        js_lossy_two_byte_to_latin1(out.as_mut_ptr(), cx.raw_cx().cast(), chars);
        out.assume_init()
    }

    pub unsafe fn utf8_to_two_byte(
        cx: &mut JSContext,
        chars: &UTF8Chars,
        out_len: &mut usize,
    ) -> TwoByteCharsZ {
        let mut out = std::mem::MaybeUninit::uninit();
        js_utf8_to_two_byte(
            out.as_mut_ptr(),
            cx.raw_cx().cast(),
            chars,
            out_len,
            0,
        );
        out.assume_init()
    }

    pub unsafe fn lossy_utf8_to_two_byte(
        cx: &mut JSContext,
        chars: &UTF8Chars,
        out_len: &mut usize,
    ) -> TwoByteCharsZ {
        let mut out = std::mem::MaybeUninit::uninit();
        js_lossy_utf8_to_two_byte(
            out.as_mut_ptr(),
            cx.raw_cx().cast(),
            chars,
            out_len,
            0,
        );
        out.assume_init()
    }
}

#[cfg(feature = "js-quickjs")]
mod encode_abi {
    use std::ffi::c_char;

    use js::context::JSContext;
    use js::jsapi::mozilla::Range;
    use js::jsapi::{
        Handle, Latin1CharsZ, MutableHandle as RawMutableHandle, JSString, TwoByteChars,
        TwoByteCharsZ, UTF8Chars, jsid,
    };
    use js::rust::Handle as SafeHandle;
    use js::rust::MutableHandle as SafeMutableHandle;
    use js::rust::wrappers2::{
        JS_CharsToId, JS_EncodeStringToASCII, JS_EncodeStringToLatin1, JS_EncodeStringToUTF8,
        LossyTwoByteCharsToNewLatin1CharsZ, LossyUTF8CharsToNewTwoByteCharsZ,
        UTF8CharsToNewTwoByteCharsZ,
    };

    pub unsafe fn encode_string_to_ascii(cx: &mut JSContext, string: *mut JSString) -> *mut c_char {
        JS_EncodeStringToASCII(cx, string) as *mut c_char
    }

    pub unsafe fn encode_string_to_latin1(cx: &mut JSContext, string: *mut JSString) -> *mut c_char {
        JS_EncodeStringToLatin1(cx, string) as *mut c_char
    }

    pub unsafe fn encode_string_to_utf8(
        cx: &mut JSContext,
        string: Handle<*mut JSString>,
    ) -> *mut c_char {
        let string = SafeHandle::from_marked_location(string.ptr);
        JS_EncodeStringToUTF8(cx, string) as *mut c_char
    }

    pub unsafe fn chars_to_id(
        cx: &mut JSContext,
        chars: TwoByteChars,
        id: RawMutableHandle<jsid>,
    ) -> bool {
        let id = SafeMutableHandle::from_marked_location(id.ptr);
        JS_CharsToId(cx, chars, id)
    }

    pub unsafe fn lossy_two_byte_to_latin1(
        cx: &mut JSContext,
        chars: &Range<u16>,
    ) -> Latin1CharsZ {
        LossyTwoByteCharsToNewLatin1CharsZ(cx, chars)
    }

    pub unsafe fn utf8_to_two_byte(
        cx: &mut JSContext,
        chars: &UTF8Chars,
        out_len: &mut usize,
    ) -> TwoByteCharsZ {
        UTF8CharsToNewTwoByteCharsZ(cx, chars, out_len, 0)
    }

    pub unsafe fn lossy_utf8_to_two_byte(
        cx: &mut JSContext,
        chars: &UTF8Chars,
        out_len: &mut usize,
    ) -> TwoByteCharsZ {
        LossyUTF8CharsToNewTwoByteCharsZ(cx, chars, out_len, 0)
    }
}

const APIS: &[&str] = &[
    "AtomToLinearString",
    "CreateJSExternalStringCallbacks",
    "DeleteJSExternalStringCallbacks",
    "GetLinearStringCharAt",
    "GetLinearStringLength",
    "JS_AtomizeAndPinStringN",
    "JS_AtomizeString",
    "JS_AtomizeStringN",
    "JS_AtomizeUCString",
    "JS_AtomizeUCStringN",
    "JS_CharsToId",
    "JS_CopyStringChars",
    "JS_EncodeStringToASCII",
    "JS_EncodeStringToBuffer",
    "JS_EncodeStringToLatin1",
    "JS_EncodeStringToUTF8",
    "JS_EnsureLinearString",
    "JS_ForgetStringLinearness",
    "JS_GetEmptyString",
    "JS_GetEmptyStringValue",
    "JS_GetStringEncodingLength",
    "JS_GetStringLength",
    "JS_IsIdentifier",
    "JS_IsIdentifier1",
    "JS_LinearStringEqualsAscii",
    "JS_LinearStringEqualsAscii1",
    "JS_NewExternalStringLatin1",
    "JS_NewExternalUCString",
    "JS_NewMaybeExternalStringLatin1",
    "JS_NewMaybeExternalStringUTF8",
    "JS_NewMaybeExternalUCString",
    "JS_NewStringCopyUTF8N",
    "JS_NewStringCopyUTF8Z",
    "JS_NewStringCopyZ",
    "JS_NewUCStringCopyZ",
    "JS_ParseJSON1",
    "JS_ParseJSON2",
    "JS_ParseJSONWithReviver1",
    "JS_PutEscapedLinearString",
    "JS_PutEscapedString",
    "JS_StringEqualsAscii",
    "JS_StringEqualsAscii1",
    "JS_StringToId",
    "JS_StringifyWithLengthHint",
    "JS_free",
    "JS_malloc",
    "JS_realloc",
    "LossyTwoByteCharsToNewLatin1CharsZ",
    "LossyUTF8CharsToNewTwoByteCharsZ",
    "PropertySpecNameEqualsId",
    "PropertySpecNameToPermanentId",
    "RUST_JSID_IS_INT",
    "RUST_JSID_IS_STRING",
    "RUST_JSID_IS_VOID",
    "RUST_JSID_TO_INT",
    "RUST_JSID_TO_STRING",
    "StringIsArrayIndex",
    "StringIsArrayIndex1",
    "StringToLinearStringSlow",
    "ToBooleanSlow",
    "ToInt32Slow",
    "ToInt64Slow",
    "ToJSONMaybeSafely",
    "ToNumberSlow",
    "ToObjectSlow",
    "ToStringSlow",
    "ToUint16Slow",
    "ToUint32Slow",
    "ToUint64Slow",
    "UTF8CharsToNewTwoByteCharsZ",
    "int_to_jsid",
];

pub fn run(engine: &JSEngine) {
    record_case(engine, "strings.remaining", APIS, |cx, global| unsafe {
        let mut utf8 = *b"a\xe2\x82\xac";
        let utf8_chars = js::jsapi::JS::UTF8Chars {
            _base: range(utf8.as_mut_ptr(), utf8.len()),
        };

        rooted!(&in(cx) let ascii = JS_NewStringCopyZ(cx, c"abc".as_ptr()));
        rooted!(&in(cx) let wide = JS_NewUCStringCopyZ(cx, [0x20ac, 0].as_ptr()));
        rooted!(&in(cx) let utf8z = JS_NewStringCopyUTF8Z(
            cx,
            js::jsapi::JS::ConstUTF8CharsZ {
                data_: b"caf\xc3\xa9\0".as_ptr().cast(),
            },
        ));
        rooted!(&in(cx) let utf8n = JS_NewStringCopyUTF8N(cx, &utf8_chars));
        assert_eq!(to_rust(cx, ascii.get()), "abc");
        assert_eq!(to_rust(cx, wide.get()), "€");
        assert_eq!(to_rust(cx, utf8z.get()), "café");
        assert_eq!(to_rust(cx, utf8n.get()), "a€");
        assert_eq!(JS_GetStringLength(ascii.get()), 3);
        assert_eq!(JS_GetStringEncodingLength(cx, ascii.get()), 3);
        let encoded = encode_abi::encode_string_to_ascii(cx, ascii.get());
        assert_eq!(encode(cx, encoded), b"abc".to_vec());
        let encoded = encode_abi::encode_string_to_latin1(cx, wide.get());
        assert_eq!(encode(cx, encoded), [0xac]);
        let encoded = encode_abi::encode_string_to_utf8(cx, ascii.handle().into());
        assert_eq!(encode(cx, encoded), b"abc".to_vec());
        let mut buffer = [0i8; 4];
        assert!(JS_EncodeStringToBuffer(
            cx,
            ascii.get(),
            buffer.as_mut_ptr(),
            buffer.len()
        ));
        assert_eq!(&buffer[..3], &[b'a' as i8, b'b' as i8, b'c' as i8]);

        let mut escaped = [0i8; 8];
        let n = JS_PutEscapedString(
            cx,
            escaped.as_mut_ptr(),
            escaped.len(),
            wide.get(),
            b'"' as c_char,
        );
        assert_eq!(CStr::from_ptr(escaped.as_ptr()).to_bytes(), br#""\u20AC"#);
        assert_eq!(n, 8);
        let linear = StringToLinearStringSlow(cx, wide.get());
        let n = JS_PutEscapedLinearString(
            escaped.as_mut_ptr(),
            escaped.len(),
            linear,
            b'"' as c_char,
        );
        assert_eq!(n, 8);

        let empty = JS_GetEmptyString(cx);
        assert_eq!(JS_GetStringLength(empty), 0);
        let mut empty_value = UndefinedValue();
        JS_GetEmptyStringValue(cx, &mut empty_value);
        assert_eq!(empty_value.to_string(), empty);

        let ext1 = external_callbacks(3);
        let p = Box::into_raw(b"abc".to_vec().into_boxed_slice());
        let latin1 = JS_NewExternalStringLatin1(cx, p.cast(), 3, ext1);
        let ext2 = external_callbacks(1);
        let p = Box::into_raw(Box::new(0x20acu16));
        let utf16 = JS_NewExternalUCString(cx, p, 1, ext2);
        rooted!(&in(cx) let latin1 = latin1);
        rooted!(&in(cx) let utf16 = utf16);
        assert_eq!(to_rust(cx, latin1.get()), "abc");
        assert_eq!(to_rust(cx, utf16.get()), "€");

        let ext3 = external_callbacks(3);
        let p = Box::into_raw(b"xyz".to_vec().into_boxed_slice());
        let mut allocated = false;
        rooted!(&in(cx) let maybe_latin1 =
            JS_NewMaybeExternalStringLatin1(cx, p.cast(), 3, ext3, &mut allocated));
        let ext4 = external_callbacks(1);
        let p = Box::into_raw(Box::new(0x2603u16));
        let mut allocated_uc = false;
        rooted!(&in(cx) let maybe_utf16 =
            JS_NewMaybeExternalUCString(cx, p, 1, ext4, &mut allocated_uc));
        let ext5 = external_callbacks(4);
        let mut utf8_bytes = *b"rust";
        let utf8_chars = js::jsapi::JS::UTF8Chars {
            _base: range(utf8_bytes.as_mut_ptr(), utf8_bytes.len()),
        };
        let mut allocated_utf8 = false;
        rooted!(&in(cx) let maybe_utf8 =
            JS_NewMaybeExternalStringUTF8(cx, &utf8_chars, ext5, &mut allocated_utf8));
        assert_eq!(to_rust(cx, maybe_latin1.get()), "xyz");
        assert_eq!(to_rust(cx, maybe_utf16.get()), "☃");
        assert_eq!(to_rust(cx, maybe_utf8.get()), "rust");
        DeleteJSExternalStringCallbacks(external_callbacks(0));
        let _ = (allocated, allocated_uc, allocated_utf8);

        atoms_and_ids(cx, global, ascii.get());
        json(cx, global);
        conversions(cx);
        allocator(cx);
        set_global(cx, global, c"encoded", StringValue(&*utf8z.get()));
        assert!(eval(cx, global, "encoded === 'café'").to_boolean());
        Observation::ok(Value::Boolean(true))
    });
}

unsafe fn to_rust(cx: &mut JSContext, s: *mut js::jsapi::JSString) -> String {
    jsstr_to_string(cx, NonNull::new(s).unwrap())
}

unsafe fn encode(cx: &mut JSContext, p: *mut c_char) -> Vec<u8> {
    let bytes = CStr::from_ptr(p).to_bytes().to_vec();
    JS_free(cx, p as *mut c_void);
    bytes
}

static TRAPS: JSExternalStringCallbacksTraps = JSExternalStringCallbacksTraps {
    latin1Finalize: Some(latin1_finalize),
    utf16Finalize: Some(utf16_finalize),
    latin1SizeOfBuffer: Some(latin1_size),
    utf16SizeOfBuffer: Some(utf16_size),
};

unsafe extern "C" fn latin1_finalize(private: *const c_void, chars: *mut u8) {
    let _ = (private, chars);
}

unsafe extern "C" fn utf16_finalize(private: *const c_void, chars: *mut u16) {
    let _ = (private, chars);
}

unsafe extern "C" fn latin1_size(private: *const c_void, _: *const u8, _: MallocSizeOf) -> usize {
    private as usize
}

unsafe extern "C" fn utf16_size(private: *const c_void, _: *const u16, _: MallocSizeOf) -> usize {
    private as usize
}

unsafe fn external_callbacks(len: usize) -> *mut JSExternalStringCallbacks {
    CreateJSExternalStringCallbacks(&TRAPS, len as *mut c_void)
}

unsafe fn range<T>(ptr: *mut T, len: usize) -> Range<T> {
    let mut range: Range<T> = std::mem::zeroed();
    range.mStart.mPtr = ptr;
    range.mEnd.mPtr = ptr.add(len);
    range
}

unsafe fn atoms_and_ids(
    cx: &mut JSContext,
    global: HandleObject,
    ascii: *mut js::jsapi::JSString,
) {
    let a = JS_AtomizeString(cx, c"name".as_ptr());
    let b = JS_AtomizeStringN(cx, c"name".as_ptr(), 4);
    let c = JS_AtomizeAndPinStringN(cx, c"pinned".as_ptr(), 6);
    let d = JS_AtomizeUCString(cx, [0x20ac, 0].as_ptr());
    let e = JS_AtomizeUCStringN(cx, [0x20ac].as_ptr(), 1);
    assert_eq!(to_rust(cx, a), "name");
    assert_eq!(to_rust(cx, b), "name");
    assert_eq!(to_rust(cx, c), "pinned");
    assert_eq!(to_rust(cx, d), "€");
    assert_eq!(to_rust(cx, e), "€");

    let linear = AtomToLinearString(a as *mut JSAtom);
    assert_eq!(GetLinearStringLength(linear), 4);
    assert_eq!(GetLinearStringCharAt(linear, 1), b'a' as u16);
    let ensured = JS_EnsureLinearString(cx, b);
    assert_eq!(GetLinearStringLength(ensured), 4);
    assert!(JS_LinearStringEqualsAscii(ensured, c"name".as_ptr()));
    assert!(JS_LinearStringEqualsAscii1(ensured, c"name!".as_ptr(), 4));
    assert!(!JS_LinearStringEqualsAscii1(ensured, c"name".as_ptr(), 3));
    let forgotten = JS_ForgetStringLinearness(ensured);
    assert_eq!(to_rust(cx, forgotten), "name");

    let mut matched = false;
    assert!(JS_StringEqualsAscii(cx, a, c"name".as_ptr(), &mut matched));
    assert!(matched);
    assert!(JS_StringEqualsAscii1(cx, a, c"name".as_ptr(), 4, &mut matched));
    assert!(matched);

    rooted!(&in(cx) let a = a);
    rooted!(&in(cx) let mut string_id = jsid::default());
    assert!(JS_StringToId(cx, a.handle(), string_id.handle_mut()));
    assert!(RUST_JSID_IS_STRING(string_id.handle()));
    assert!(!RUST_JSID_IS_INT(string_id.handle()));
    assert!(!RUST_JSID_IS_VOID(string_id.handle()));
    let back = RUST_JSID_TO_STRING(string_id.handle());
    assert_eq!(to_rust(cx, back), "name");

    rooted!(&in(cx) let mut int_id = jsid::default());
    int_to_jsid(17, int_id.handle_mut());
    assert!(RUST_JSID_IS_INT(int_id.handle()));
    assert_eq!(RUST_JSID_TO_INT(int_id.handle()), 17);

    let mut chars = [b'n' as u16, b'a' as u16, b'm' as u16, b'e' as u16, 0];
    let chars = js::jsapi::JS::TwoByteChars {
        _base: range(chars.as_mut_ptr(), 4),
    };
    rooted!(&in(cx) let mut chars_id = jsid::default());
    assert!(!js::jsapi::JS_IsExceptionPending(cx.raw_cx()));
    let ok = encode_abi::chars_to_id(cx, chars, chars_id.handle_mut().into());
    let pending = js::jsapi::JS_IsExceptionPending(cx.raw_cx());
    rooted!(&in(cx) let mut exception = UndefinedValue());
    if pending {
        js::rust::wrappers2::JS_GetPendingException(cx, exception.handle_mut());
        let _ = exception;
    }
    js::jsapi::JS_ClearPendingException(cx.raw_cx());
    assert!(ok, "JS_CharsToId failed; pending={pending}");
    assert!(RUST_JSID_IS_STRING(chars_id.handle()));

    let spec_name = JSPropertySpec_Name {
        string_: c"name".as_ptr(),
    };
    assert!(PropertySpecNameEqualsId(spec_name, string_id.handle()));
    rooted!(&in(cx) let mut permanent = jsid::default());
    assert!(PropertySpecNameToPermanentId(
        cx,
        spec_name,
        permanent.as_ptr()
    ));
    assert!(RUST_JSID_IS_STRING(permanent.handle()));
    assert_eq!(to_rust(cx, RUST_JSID_TO_STRING(permanent.handle())), "name");

    let mut ident = false;
    rooted!(&in(cx) let ident_string = JS_AtomizeString(cx, c"identifier".as_ptr()));
    assert!(JS_IsIdentifier(cx, ident_string.handle(), &mut ident));
    assert!(ident);
    let ident_chars: Vec<u16> = "identifier".encode_utf16().collect();
    assert!(JS_IsIdentifier1(ident_chars.as_ptr(), ident_chars.len()));
    let not_ident: Vec<u16> = "1a".encode_utf16().collect();
    assert!(!JS_IsIdentifier1(not_ident.as_ptr(), not_ident.len()));

    let mut index = 0;
    let index_chars: Vec<u16> = "123".encode_utf16().collect();
    rooted!(&in(cx) let index_string = JS_NewUCStringCopyN(
        cx,
        index_chars.as_ptr(),
        index_chars.len()
    ));
    let index_linear = JS_EnsureLinearString(cx, index_string.get());
    assert!(StringIsArrayIndex(index_linear, &mut index));
    assert_eq!(index, 123);
    assert!(StringIsArrayIndex1(index_chars.as_ptr(), 3, &mut index));
    assert_eq!(index, 123);

    let _ = ascii;
    set_global(cx, global, c"atom", StringValue(&*ident_string.get()));
    assert!(eval(cx, global, "atom === 'identifier'").to_boolean());
}

fn mutable<T>(value: &mut T) -> MutableHandle<'_, T> {
    unsafe { MutableHandle::from_marked_location(value) }
}

unsafe extern "C" fn json_write(buf: *const u16, len: u32, data: *mut c_void) -> bool {
    let out = &mut *(data as *mut String);
    out.push_str(&String::from_utf16_lossy(slice::from_raw_parts(
        buf,
        len as usize,
    )));
    true
}

unsafe fn json(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let source = JS_NewStringCopyZ(cx, c"{\"a\":[1,2]}".as_ptr()));
    rooted!(&in(cx) let mut parsed = UndefinedValue());
    assert!(JS_ParseJSON1(cx, source.handle(), parsed.handle_mut()));
    assert!(parsed.get().is_object());

    let latin1 = b"{\"b\":3}";
    let mut parsed2 = UndefinedValue();
    assert!(JS_ParseJSON2(
        cx,
        latin1.as_ptr(),
        latin1.len() as u32,
        mutable(&mut parsed2)
    ));
    assert!(parsed2.is_object());

    rooted!(&in(cx) let reviver = eval(
        cx,
        global,
        "(key, value) => typeof value === 'number' ? value * 2 : value"
    ));
    let mut revived = UndefinedValue();
    assert!(JS_ParseJSONWithReviver1(
        cx,
        source.handle(),
        reviver.handle(),
        mutable(&mut revived)
    ));
    set_global(cx, global, c"revived", revived);
    assert!(eval(cx, global, "revived.a[1] === 4").to_boolean());

    let mut value = parsed.get();
    let mut out = String::new();
    assert!(JS_StringifyWithLengthHint(
        cx,
        mutable(&mut value),
        HandleObject::null(),
        HandleValue::null(),
        Some(json_write),
        &mut out as *mut _ as *mut c_void,
        16,
    ));
    assert_eq!(out, "{\"a\":[1,2]}");

    rooted!(&in(cx) let object = JS_NewPlainObject(cx));
    let mut safe_out = String::new();
    assert!(ToJSONMaybeSafely(
        cx,
        object.handle(),
        Some(json_write),
        &mut safe_out as *mut _ as *mut c_void,
    ));
    assert_eq!(safe_out, "{}");
}

unsafe fn conversions(cx: &mut JSContext) {
    rooted!(&in(cx) let empty = JS_GetEmptyString(cx));
    rooted!(&in(cx) let nonempty = JS_NewStringCopyZ(cx, c"x".as_ptr()));
    rooted!(&in(cx) let empty_value = StringValue(&*empty.get()));
    rooted!(&in(cx) let nonempty_value = StringValue(&*nonempty.get()));
    assert!(!ToBooleanSlow(empty_value.handle()));
    assert!(ToBooleanSlow(nonempty_value.handle()));

    rooted!(&in(cx) let slow_value = BooleanValue(true));
    let mut number = 0.0;
    assert!(ToNumberSlow(cx, slow_value.handle(), &mut number));
    assert_eq!(number, 1.0);
    let mut i32_ = 0;
    let mut u32_ = 0;
    let mut u16_ = 0;
    let mut i64_ = 0;
    let mut u64_ = 0;
    assert!(ToInt32Slow(cx, slow_value.handle(), &mut i32_));
    assert!(ToUint32Slow(cx, slow_value.handle(), &mut u32_));
    assert!(ToUint16Slow(cx, slow_value.handle(), &mut u16_));
    assert!(ToInt64Slow(cx, slow_value.handle(), &mut i64_));
    assert!(ToUint64Slow(cx, slow_value.handle(), &mut u64_));
    assert_eq!((i32_, u32_, u16_, i64_, u64_), (1, 1, 1, 1, 1));

    rooted!(&in(cx) let mut value = Int32Value(42));
    let string = ToStringSlow(cx, value.handle());
    assert_eq!(to_rust(cx, string), "42");
    let object = ToObjectSlow(cx, value.handle(), false);
    assert!(!object.is_null());
    value.set(NullValue());
    let null_object = ToObjectSlow(cx, value.handle(), false);
    assert!(null_object.is_null());
    let pending = js::jsapi::JS_IsExceptionPending(cx.raw_cx());
    js::jsapi::JS_ClearPendingException(cx.raw_cx());
    assert!(pending);

    let mut tb = vec![0x00e9u16, 0x20ac, 0];
    let two_byte = range(tb.as_mut_ptr(), 2);
    let latin1 = encode_abi::lossy_two_byte_to_latin1(cx, &two_byte);
    assert_eq!(*latin1._base.mPtr, 0xe9);
    assert_eq!(*latin1._base.mPtr.add(1), 0xac);
    JS_free(cx, latin1._base.mPtr as *mut c_void);

    let mut utf8 = vec![b'c', b'a', b'f', 0xc3, 0xa9, 0];
    let chars = js::jsapi::JS::UTF8Chars {
        _base: range(utf8.as_mut_ptr(), 5),
    };
    let mut out_len = 0;
    let units = encode_abi::utf8_to_two_byte(cx, &chars, &mut out_len);
    assert_eq!(out_len, 4);
    assert_eq!(*units._base.mPtr, b'c' as u16);
    assert_eq!(*units._base.mPtr.add(3), 0xe9);
    JS_free(cx, units._base.mPtr as *mut c_void);

    let mut bad = vec![b'a', 0xff, 0];
    let chars = js::jsapi::JS::UTF8Chars {
        _base: range(bad.as_mut_ptr(), 2),
    };
    let mut out_len = 0;
    let lost = encode_abi::lossy_utf8_to_two_byte(cx, &chars, &mut out_len);
    assert_eq!(out_len, 2);
    JS_free(cx, lost._base.mPtr as *mut c_void);
    js::jsapi::JS_ClearPendingException(cx.raw_cx());
}

unsafe fn allocator(cx: &mut JSContext) {
    let p = JS_malloc(cx, 8);
    assert!(!p.is_null());
    *(p as *mut u64) = 7;
    let p = JS_realloc(cx, p, 8, 16);
    assert!(!p.is_null());
    assert_eq!(*(p as *mut u64), 7);
    JS_free(cx, p);

    let src = [b'a' as u16, b'b' as u16, b'c' as u16];
    let mut dest = [0u16; 4];
    let dest_range = range(dest.as_mut_ptr(), 3);
    let string = JS_NewUCStringCopyN(cx, src.as_ptr(), src.len());
    assert!(JS_CopyStringChars(cx, &dest_range, string));
    assert_eq!(&dest[..3], &src);
}
