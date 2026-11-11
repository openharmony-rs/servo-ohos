/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! Strings, conversions, property keys and JSON.

#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_void};
use std::ptr::NonNull;
use std::slice;

use js::context::JSContext;
use js::conversions::{ToJSValConvertible, jsstr_to_string};
use js::jsapi::{
    JS_ClearPendingException, JS_DeprecatedStringHasLatin1Chars, JS_IsExceptionPending, JSString,
    JSType, SymbolCode, jsid,
};
use js::jsval::{DoubleValue, Int32Value, JSVal, StringValue, UndefinedValue};
use js::rooted;
use js::rust::wrappers2::{
    EncodeStringToUTF8, GetWellKnownSymbol, JS_AtomizeAndPinString, JS_CompareStrings,
    JS_ConcatStrings, JS_DefinePropertyById2, JS_GetLatin1StringCharsAndLength, JS_GetStringCharAt,
    JS_GetTwoByteStringCharsAndLength, JS_IdToValue, JS_IndexToId, JS_IsIdentifier,
    JS_NewDependentString, JS_NewStringCopyN, JS_NewUCStringCopyN, JS_ParseJSON,
    JS_ParseJSONWithReviver, JS_SetProperty, JS_Stringify, JS_TypeOfValue, JS_ValueToId,
    JS_ValueToSource, OrdinaryToPrimitive, RUST_INTERNED_STRING_TO_JSID, RUST_SYMBOL_TO_JSID,
    SameValue, StrictlyEqual, ToInt8Slow, ToInt16Slow, ToJSON, ToPrimitive, ToUint8Slow,
};
use js::rust::{
    CompileOptionsWrapper, HandleObject, HandleValue, JSEngine, ToBoolean, ToInt32, ToInt64,
    ToNumber, ToString, ToUint16, ToUint32, ToUint64, evaluate_script,
};

use crate::support::{Observation, Value, record_case};

pub fn run(engine: &JSEngine) {
    record_case(engine, "strings.core", STRING_APIS, |cx, global| unsafe {
        latin1_and_two_byte(cx, global);
        lone_surrogates(cx, global);
        number_conversions(cx, global);
        string_conversions(cx, global);
        primitives_and_equality(cx, global);
        value_to_source(cx, global);
        ids(cx, global);
        json(cx, global);
        Observation::ok(Value::Boolean(true))
    });
}

const STRING_APIS: &[&str] = &[
    "JS_NewStringCopyN",
    "JS_DeprecatedStringHasLatin1Chars",
    "JS_GetLatin1StringCharsAndLength",
    "JS_GetStringCharAt",
    "JS_GetTwoByteStringCharsAndLength",
    "JS_NewUCStringCopyN",
    "JS_ConcatStrings",
    "JS_NewDependentString",
    "JS_CompareStrings",
    "EncodeStringToUTF8",
    "ToInt8Slow",
    "ToUint8Slow",
    "ToInt16Slow",
    "ToPrimitive",
    "OrdinaryToPrimitive",
    "JS_TypeOfValue",
    "SameValue",
    "StrictlyEqual",
    "JS_ValueToSource",
    "JS_IndexToId",
    "JS_IdToValue",
    "JS_ValueToId",
    "JS_AtomizeAndPinString",
    "RUST_INTERNED_STRING_TO_JSID",
    "RUST_SYMBOL_TO_JSID",
    "GetWellKnownSymbol",
    "JS_DefinePropertyById2",
    "JS_ParseJSON",
    "JS_ParseJSONWithReviver",
    "JS_Stringify",
    "ToJSON",
];

unsafe fn eval(cx: &mut JSContext, global: HandleObject, source: &str) -> JSVal {
    rooted!(&in(cx) let mut rval = UndefinedValue());
    let options = CompileOptionsWrapper::new(cx, c"qjs_strings.js".to_owned(), 1);
    evaluate_script(cx, global, source, rval.handle_mut(), options)
        .unwrap_or_else(|_| panic!("evaluating {source:?} failed"));
    rval.get()
}

unsafe fn to_rust(cx: &mut JSContext, s: *mut JSString) -> String {
    jsstr_to_string(cx, NonNull::new(s).unwrap())
}

unsafe fn units(cx: &mut JSContext, s: *mut JSString) -> Vec<u16> {
    let mut len = 0;
    if JS_DeprecatedStringHasLatin1Chars(s) {
        let p = JS_GetLatin1StringCharsAndLength(cx, s, &mut len);
        slice::from_raw_parts(p, len)
            .iter()
            .map(|&b| b as u16)
            .collect()
    } else {
        let p = JS_GetTwoByteStringCharsAndLength(cx, s, &mut len);
        slice::from_raw_parts(p, len).to_vec()
    }
}

/// Sets `globalThis[name] = v`.
unsafe fn set_global(cx: &mut JSContext, global: HandleObject, name: &CStr, v: JSVal) {
    rooted!(&in(cx) let v = v);
    assert!(JS_SetProperty(cx, global, name.as_ptr(), v.handle()));
}

fn take_exception(cx: &mut JSContext) -> bool {
    unsafe {
        let pending = JS_IsExceptionPending(cx.raw_cx());
        JS_ClearPendingException(cx.raw_cx());
        pending
    }
}

unsafe fn latin1_and_two_byte(cx: &mut JSContext, global: HandleObject) {
    let latin1 = JS_NewStringCopyN(cx, b"caf\xe9".as_ptr() as *const c_char, 4);
    rooted!(&in(cx) let latin1 = latin1);
    assert!(JS_DeprecatedStringHasLatin1Chars(latin1.get()));
    let mut len = 0;
    let chars = JS_GetLatin1StringCharsAndLength(cx, latin1.get(), &mut len);
    assert_eq!(slice::from_raw_parts(chars, len), b"caf\xe9");
    assert_eq!(to_rust(cx, latin1.get()), "café");
    let mut c = 0;
    assert!(JS_GetStringCharAt(cx, latin1.get(), 3, &mut c));
    assert_eq!(c, 0xe9);

    // Rust strings are created from UTF-8; non-Latin-1 text becomes two-byte.
    rooted!(&in(cx) let mut v = UndefinedValue());
    "1 € ☃".to_jsval(cx, v.handle_mut());
    let wide = v.get().to_string();
    assert!(!JS_DeprecatedStringHasLatin1Chars(wide));
    assert_eq!(units(cx, wide), "1 € ☃".encode_utf16().collect::<Vec<_>>());
    assert_eq!(to_rust(cx, wide), "1 € ☃");

    "café".to_jsval(cx, v.handle_mut());
    assert!(JS_DeprecatedStringHasLatin1Chars(v.get().to_string()));
    assert_eq!(to_rust(cx, v.get().to_string()), "café");

    // UTF-16 input that fits Latin-1 is stored as Latin-1.
    let narrow: Vec<u16> = "naïve".encode_utf16().collect();
    let s = JS_NewUCStringCopyN(cx, narrow.as_ptr(), narrow.len());
    assert!(JS_DeprecatedStringHasLatin1Chars(s));
    assert_eq!(to_rust(cx, s), "naïve");

    // Strings made by script are readable in place, both kinds.
    let s = eval(cx, global, "'abc'.repeat(3) + 'd'").to_string();
    assert_eq!(to_rust(cx, s), "abcabcabcd");
    let s = eval(cx, global, "'x'.repeat(40) + '\\u20ac'").to_string();
    assert!(!JS_DeprecatedStringHasLatin1Chars(s));
    assert_eq!(to_rust(cx, s), format!("{}€", "x".repeat(40)));

    rooted!(&in(cx) let left = latin1.get());
    rooted!(&in(cx) let right = wide);
    let cat = JS_ConcatStrings(cx, left.handle(), right.handle());
    assert_eq!(to_rust(cx, cat), "café1 € ☃");
    rooted!(&in(cx) let cat = cat);
    let sub = JS_NewDependentString(cx, cat.handle(), 3, 3);
    assert_eq!(to_rust(cx, sub), "é1 ");
    let mut order = 0;
    assert!(JS_CompareStrings(cx, latin1.get(), cat.get(), &mut order));
    assert!(order < 0);
    assert!(JS_CompareStrings(cx, cat.get(), latin1.get(), &mut order));
    assert!(order > 0);
    assert!(JS_CompareStrings(
        cx,
        latin1.get(),
        latin1.get(),
        &mut order
    ));
    assert_eq!(order, 0);

    thread_local!(static ENCODED: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) });
    unsafe extern "C" fn record(s: *const c_char) {
        ENCODED.with(|e| *e.borrow_mut() = CStr::from_ptr(s).to_bytes().to_vec());
    }
    EncodeStringToUTF8(cx, latin1.handle(), record);
    assert_eq!(ENCODED.with(|e| e.borrow().clone()), "café".as_bytes());
}

unsafe fn lone_surrogates(cx: &mut JSContext, global: HandleObject) {
    let chars = [0x61, 0xd800, 0x62, 0xdc00];
    let s = JS_NewUCStringCopyN(cx, chars.as_ptr(), chars.len());
    assert!(!JS_DeprecatedStringHasLatin1Chars(s));
    assert_eq!(units(cx, s), chars);
    assert_eq!(to_rust(cx, s), "a\u{fffd}b\u{fffd}");

    // Through script and back.
    set_global(cx, global, c"lone", StringValue(&*s));
    let v = eval(
        cx,
        global,
        "lone.length === 4 && lone.charCodeAt(1) === 0xd800 && lone.charCodeAt(3) === 0xdc00",
    );
    assert!(v.to_boolean());
    let s = eval(cx, global, "lone.slice(1, 2) + '\\udbff\\udfff'").to_string();
    assert_eq!(units(cx, s), [0xd800, 0xdbff, 0xdfff]);
    assert_eq!(to_rust(cx, s), "\u{fffd}\u{10ffff}");

    rooted!(&in(cx) let s = s);
    thread_local!(static ENCODED: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) });
    unsafe extern "C" fn record(s: *const c_char) {
        ENCODED.with(|e| *e.borrow_mut() = CStr::from_ptr(s).to_bytes().to_vec());
    }
    EncodeStringToUTF8(cx, s.handle(), record);
    assert_eq!(
        ENCODED.with(|e| e.borrow().clone()),
        "\u{fffd}\u{10ffff}".as_bytes()
    );
}

unsafe fn number_conversions(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let mut v = UndefinedValue());
    let mut number = |cx: &mut JSContext, source: &str| {
        v.set(eval(cx, global, source));
        ToNumber(cx, v.handle())
    };
    assert_eq!(number(cx, "' 42 '"), Ok(42.0));
    assert_eq!(number(cx, "'0x10'"), Ok(16.0));
    assert_eq!(number(cx, "''"), Ok(0.0));
    assert!(number(cx, "'4a'").unwrap().is_nan());
    assert!(number(cx, "undefined").unwrap().is_nan());
    assert_eq!(number(cx, "null"), Ok(0.0));
    assert_eq!(number(cx, "true"), Ok(1.0));
    assert_eq!(number(cx, "({ valueOf() { return 7; } })"), Ok(7.0));
    assert_eq!(number(cx, "[5]"), Ok(5.0));
    assert_eq!(number(cx, "Symbol()"), Err(()));
    assert!(take_exception(cx));
    assert_eq!(number(cx, "1n"), Err(()));
    assert!(take_exception(cx));
    assert_eq!(number(cx, "({ valueOf() { throw 1; } })"), Err(()));
    assert!(take_exception(cx));

    rooted!(&in(cx) let mut v = UndefinedValue());
    let cases: [(f64, i32, u32, u16, i64, u64); 10] = [
        (4294967301.0, 5, 5, 5, 4294967301, 4294967301),
        (-1.0, -1, u32::MAX, u16::MAX, -1, u64::MAX),
        (-3.7, -3, u32::MAX - 2, u16::MAX - 2, -3, u64::MAX - 2),
        (2147483648.0, i32::MIN, 1 << 31, 0, 1 << 31, 1 << 31),
        (65537.9, 65537, 65537, 1, 65537, 65537),
        (-0.0, 0, 0, 0, 0, 0),
        (f64::NAN, 0, 0, 0, 0, 0),
        (f64::INFINITY, 0, 0, 0, 0, 0),
        (18446744073709555712.0, 4096, 4096, 4096, 4096, 4096),
        (
            -9007199254740993.0,
            0,
            0,
            0,
            -9007199254740992,
            18437736874454810624,
        ),
    ];
    for (d, i32_, u32_, u16_, i64_, u64_) in cases {
        v.set(DoubleValue(d));
        assert_eq!(ToInt32(cx, v.handle()), Ok(i32_), "ToInt32({d})");
        assert_eq!(ToUint32(cx, v.handle()), Ok(u32_), "ToUint32({d})");
        assert_eq!(ToUint16(cx, v.handle()), Ok(u16_), "ToUint16({d})");
        assert_eq!(ToInt64(cx, v.handle()), Ok(i64_), "ToInt64({d})");
        assert_eq!(ToUint64(cx, v.handle()), Ok(u64_), "ToUint64({d})");
    }

    v.set(eval(cx, global, "'-129'"));
    let (mut i8_, mut u8_, mut i16_) = (0i8, 0u8, 0i16);
    assert!(ToInt8Slow(cx, v.handle(), &mut i8_));
    assert!(ToUint8Slow(cx, v.handle(), &mut u8_));
    assert!(ToInt16Slow(cx, v.handle(), &mut i16_));
    assert_eq!((i8_, u8_, i16_), (127, 127, -129));
    v.set(eval(cx, global, "'0x1ff'"));
    assert!(ToUint8Slow(cx, v.handle(), &mut u8_));
    assert_eq!(u8_, 0xff);

    for (source, expected) in [
        ("''", false),
        ("'0'", true),
        ("0n", false),
        ("5n", true),
        ("({})", true),
        ("NaN", false),
    ] {
        v.set(eval(cx, global, source));
        assert_eq!(ToBoolean(v.handle()), expected, "ToBoolean({source})");
    }
}

unsafe fn string_conversions(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let mut v = UndefinedValue());
    let cases = [
        (DoubleValue(1.5), "1.5"),
        (DoubleValue(-0.0), "0"),
        (DoubleValue(1e21), "1e+21"),
        (DoubleValue(0.1 + 0.2), "0.30000000000000004"),
        (DoubleValue(f64::NEG_INFINITY), "-Infinity"),
        (Int32Value(-42), "-42"),
        (UndefinedValue(), "undefined"),
    ];
    for (value, expected) in cases {
        v.set(value);
        let s = ToString(cx, v.handle());
        assert_eq!(to_rust(cx, s), expected);
    }
    for (source, expected) in [
        ("({})", "[object Object]"),
        ("[1, [2, 3], null]", "1,2,3,"),
        ("({ toString() { return 'custom'; } })", "custom"),
        (
            "({ [Symbol.toPrimitive](hint) { return hint; } })",
            "string",
        ),
        ("12345678901234567890n", "12345678901234567890"),
        ("'a' + 'b'.repeat(3)", "abbb"),
    ] {
        v.set(eval(cx, global, source));
        let s = ToString(cx, v.handle());
        assert_eq!(to_rust(cx, s), expected, "ToString({source})");
    }
    v.set(eval(cx, global, "Symbol('s')"));
    assert!(ToString(cx, v.handle()).is_null());
    assert!(take_exception(cx));
}

unsafe fn primitives_and_equality(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let obj = eval(cx, global, "({ [Symbol.toPrimitive](hint) { return 'hint:' + hint; } })").to_object());
    rooted!(&in(cx) let mut v = UndefinedValue());
    for (hint, expected) in [
        (JSType::JSTYPE_STRING, "hint:string"),
        (JSType::JSTYPE_NUMBER, "hint:number"),
        (JSType::JSTYPE_UNDEFINED, "hint:default"),
    ] {
        assert!(ToPrimitive(cx, obj.handle(), hint, v.handle_mut()));
        assert_eq!(to_rust(cx, v.get().to_string()), expected);
    }

    rooted!(&in(cx) let obj = eval(cx, global, "({ valueOf() { return 1; }, toString() { return 's'; } })").to_object());
    assert!(ToPrimitive(
        cx,
        obj.handle(),
        JSType::JSTYPE_UNDEFINED,
        v.handle_mut()
    ));
    assert_eq!(v.get().to_int32(), 1);
    assert!(OrdinaryToPrimitive(
        cx,
        obj.handle(),
        JSType::JSTYPE_STRING,
        v.handle_mut()
    ));
    assert_eq!(to_rust(cx, v.get().to_string()), "s");

    rooted!(&in(cx) let obj = eval(cx, global, "({ valueOf() { return {}; }, toString() { return {}; } })").to_object());
    assert!(!ToPrimitive(
        cx,
        obj.handle(),
        JSType::JSTYPE_NUMBER,
        v.handle_mut()
    ));
    assert!(take_exception(cx));

    for (source, expected) in [
        ("undefined", JSType::JSTYPE_UNDEFINED),
        ("null", JSType::JSTYPE_OBJECT),
        ("({})", JSType::JSTYPE_OBJECT),
        ("(function () {})", JSType::JSTYPE_FUNCTION),
        ("(class A {})", JSType::JSTYPE_FUNCTION),
        ("'s'", JSType::JSTYPE_STRING),
        ("1.5", JSType::JSTYPE_NUMBER),
        ("false", JSType::JSTYPE_BOOLEAN),
        ("Symbol()", JSType::JSTYPE_SYMBOL),
        ("1n", JSType::JSTYPE_BIGINT),
    ] {
        v.set(eval(cx, global, source));
        assert_eq!(JS_TypeOfValue(cx, v.handle()), expected, "typeof {source}");
    }

    rooted!(&in(cx) let mut a = UndefinedValue());
    rooted!(&in(cx) let mut b = UndefinedValue());
    let mut compare = |cx: &mut JSContext, x: JSVal, y: JSVal| {
        a.set(x);
        b.set(y);
        let (mut same, mut strict) = (false, false);
        assert!(SameValue(cx, a.handle(), b.handle(), &mut same));
        assert!(StrictlyEqual(cx, a.handle(), b.handle(), &mut strict));
        (same, strict)
    };
    assert_eq!(
        compare(cx, DoubleValue(f64::NAN), DoubleValue(f64::NAN)),
        (true, false)
    );
    assert_eq!(
        compare(cx, DoubleValue(0.0), DoubleValue(-0.0)),
        (false, true)
    );
    assert_eq!(compare(cx, Int32Value(3), DoubleValue(3.0)), (true, true));
    let s1 = eval(cx, global, "'ab' + 'c'");
    let s2 = eval(cx, global, "'a' + 'bc'");
    assert_eq!(compare(cx, s1, s2), (true, true));
    let o1 = eval(cx, global, "({})");
    let o2 = eval(cx, global, "({})");
    assert_eq!(compare(cx, o1, o2), (false, false));
}

unsafe fn source_of(cx: &mut JSContext, v: HandleValue) -> String {
    let s = JS_ValueToSource(cx, v);
    to_rust(cx, s)
}

unsafe fn value_to_source(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let mut v = UndefinedValue());
    for (source, expected) in [
        ("undefined", "(void 0)"),
        ("null", "null"),
        ("true", "true"),
        ("-0", "-0"),
        ("1.25", "1.25"),
        ("10n", "10n"),
        (r#"'a"b\né€'"#, r#""a\"b\n\xE9\u20AC""#),
        ("Symbol.iterator", "Symbol.iterator"),
        ("Symbol('d')", r#"Symbol("d")"#),
        ("Symbol()", "Symbol()"),
        ("Symbol.for('k')", r#"Symbol.for("k")"#),
        ("[1, 'x', [null]]", r#"[1, "x", [null]]"#),
        (
            r#"({a: 1, 'b c': [2], 3: undefined, [Symbol.iterator]: 0})"#,
            r#"({3:(void 0), a:1, 'b c':[2], [Symbol.iterator]:0})"#,
        ),
        ("({})", "({})"),
        (
            "(() => { const o = {}; o.self = o; return o; })()",
            "({self:{}})",
        ),
        ("new Number(5)", "new Number(5)"),
        ("new String('s')", r#"new String("s")"#),
        ("new Boolean(false)", "new Boolean(false)"),
        ("new Date(0)", "new Date(0)"),
        ("/a+/g", "/a+/g"),
        (
            "(function f(a) { return a; })",
            "(function f(a) { return a; })",
        ),
        (
            "({ toSource() { return 'mine'; } })",
            "({toSource() { return 'mine'; }})",
        ),
        (
            "new TypeError('boom')",
            r#"(new TypeError("boom", "qjs_strings.js", 1))"#,
        ),
    ] {
        v.set(eval(cx, global, source));
        assert_eq!(
            source_of(cx, v.handle()),
            expected,
            "ValueToSource({source})"
        );
    }
}

unsafe fn ids(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let mut id = jsid::default());
    rooted!(&in(cx) let mut v = UndefinedValue());

    assert!(JS_IndexToId(cx, 5, id.handle_mut()));
    assert!(id.get().is_int());
    assert_eq!(id.get().to_int(), 5);
    assert!(JS_IdToValue(cx, id.get(), v.handle_mut()));
    assert_eq!(v.get().to_int32(), 5);

    assert!(JS_IndexToId(cx, u32::MAX - 1, id.handle_mut()));
    assert!(id.get().is_string());
    assert_eq!(to_rust(cx, id.get().to_string()), "4294967294");
    assert!(JS_IdToValue(cx, id.get(), v.handle_mut()));
    assert_eq!(to_rust(cx, v.get().to_string()), "4294967294");

    for (source, int) in [
        ("'17'", Some(17)),
        ("17", Some(17)),
        ("17.0", Some(17)),
        ("'017'", None),
        ("'-1'", None),
        ("-1", None),
        ("2147483648", None),
    ] {
        v.set(eval(cx, global, source));
        assert!(JS_ValueToId(cx, v.handle(), id.handle_mut()));
        match int {
            Some(i) => assert_eq!(
                (id.get().is_int(), id.get().to_int()),
                (true, i),
                "id of {source}"
            ),
            None => assert!(id.get().is_string(), "id of {source}"),
        }
    }

    // String ids are atoms: equal names give equal ids, and the name reads back.
    v.set(eval(cx, global, "'na' + 'me'"));
    assert!(JS_ValueToId(cx, v.handle(), id.handle_mut()));
    let name_id = id.get();
    assert!(name_id.is_string());
    let pinned = JS_AtomizeAndPinString(cx, c"name".as_ptr());
    rooted!(&in(cx) let mut interned = jsid::default());
    RUST_INTERNED_STRING_TO_JSID(cx, pinned, interned.handle_mut());
    assert_eq!(interned.get().asBits_, name_id.asBits_);
    assert_eq!(to_rust(cx, name_id.to_string()), "name");
    assert!(JS_IdToValue(cx, name_id, v.handle_mut()));
    assert_eq!(to_rust(cx, v.get().to_string()), "name");

    // Symbol ids.
    v.set(eval(cx, global, "Symbol.iterator"));
    assert!(JS_ValueToId(cx, v.handle(), id.handle_mut()));
    assert!(id.get().is_symbol());
    let symbol = GetWellKnownSymbol(cx, SymbolCode::iterator);
    rooted!(&in(cx) let mut well_known = jsid::default());
    RUST_SYMBOL_TO_JSID(symbol, well_known.handle_mut());
    assert_eq!(well_known.get().asBits_, id.get().asBits_);
    rooted!(&in(cx) let mut back = UndefinedValue());
    assert!(JS_IdToValue(cx, id.get(), back.handle_mut()));
    let mut same = false;
    assert!(SameValue(cx, v.handle(), back.handle(), &mut same));
    assert!(same);

    // Ids work as property keys from both sides.
    rooted!(&in(cx) let obj = eval(cx, global, "globalThis.keyed = {}; keyed").to_object());
    rooted!(&in(cx) let value = Int32Value(9));
    assert!(JS_DefinePropertyById2(
        cx,
        obj.handle(),
        interned.handle(),
        value.handle(),
        1
    ));
    assert!(JS_DefinePropertyById2(
        cx,
        obj.handle(),
        well_known.handle(),
        value.handle(),
        1
    ));
    assert!(JS_IndexToId(cx, 3, id.handle_mut()));
    assert!(JS_DefinePropertyById2(
        cx,
        obj.handle(),
        id.handle(),
        value.handle(),
        1
    ));
    assert!(
        eval(
            cx,
            global,
            "keyed.name === 9 && keyed[Symbol.iterator] === 9 && keyed[3] === 9"
        )
        .to_boolean()
    );

    for (name, expected) in [
        ("foo", true),
        ("$a1", true),
        ("_", true),
        ("class", true),
        ("café", true),
        ("a\u{200c}b", true),
        ("\u{1d4b3}x", true),
        ("1a", false),
        ("", false),
        ("a-b", false),
        ("a b", false),
    ] {
        let utf16: Vec<u16> = name.encode_utf16().collect();
        rooted!(&in(cx) let s = JS_NewUCStringCopyN(cx, utf16.as_ptr(), utf16.len()));
        let mut is_identifier = false;
        assert!(JS_IsIdentifier(cx, s.handle(), &mut is_identifier));
        assert_eq!(is_identifier, expected, "JS_IsIdentifier({name:?})");
    }
    let lone = [0x61, 0xd800];
    rooted!(&in(cx) let s = JS_NewUCStringCopyN(cx, lone.as_ptr(), lone.len()));
    let mut is_identifier = true;
    assert!(JS_IsIdentifier(cx, s.handle(), &mut is_identifier));
    assert!(!is_identifier);

    for (s, expected) in [
        ("0", Some(0)),
        ("123", Some(123)),
        ("0123", None),
        ("4294967294", Some(4294967294)),
        ("4294967295", None),
        ("", None),
        ("12a", None),
    ] {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        let mut index = 0;
        let is_index =
            js::jsapi::js::StringIsArrayIndex1(utf16.as_ptr(), utf16.len() as u32, &mut index);
        assert_eq!(
            is_index.then_some(index),
            expected,
            "StringIsArrayIndex({s:?})"
        );
    }
}

unsafe extern "C" fn append_json(buf: *const u16, len: u32, data: *mut c_void) -> bool {
    let out = &mut *(data as *mut Option<String>);
    out.get_or_insert_with(String::new)
        .push_str(&String::from_utf16_lossy(slice::from_raw_parts(
            buf,
            len as usize,
        )));
    true
}

unsafe fn to_json(cx: &mut JSContext, v: HandleValue) -> Option<String> {
    let mut out: Option<String> = None;
    assert!(ToJSON(
        cx,
        v,
        HandleObject::null(),
        HandleValue::null(),
        Some(append_json),
        &mut out as *mut _ as *mut c_void
    ));
    out
}

unsafe fn json(cx: &mut JSContext, global: HandleObject) {
    rooted!(&in(cx) let mut v = UndefinedValue());
    let text: Vec<u16> = r#"{"a": [1, 2.5, {"b": "é€"}], "c": null, "d": true}"#
        .encode_utf16()
        .collect();
    assert!(JS_ParseJSON(
        cx,
        text.as_ptr(),
        text.len() as u32,
        v.handle_mut()
    ));
    assert!(v.get().is_object());
    assert_eq!(
        to_json(cx, v.handle()).unwrap(),
        r#"{"a":[1,2.5,{"b":"é€"}],"c":null,"d":true}"#
    );
    set_global(cx, global, c"parsed", v.get());
    assert!(
        eval(
            cx,
            global,
            "parsed.a[2].b === '\\u00e9\\u20ac' && parsed.a[1] === 2.5"
        )
        .to_boolean()
    );

    let bad: Vec<u16> = "{a: 1}".encode_utf16().collect();
    assert!(!JS_ParseJSON(
        cx,
        bad.as_ptr(),
        bad.len() as u32,
        v.handle_mut()
    ));
    assert!(take_exception(cx));

    // A raw lone surrogate inside a JSON string survives parsing.
    let lone = [0x22, 0xd800, 0x78, 0x22];
    assert!(JS_ParseJSON(
        cx,
        lone.as_ptr(),
        lone.len() as u32,
        v.handle_mut()
    ));
    assert_eq!(units(cx, v.get().to_string()), [0xd800, 0x78]);
    assert_eq!(to_json(cx, v.handle()).unwrap(), r#""\ud800x""#);

    rooted!(&in(cx) let reviver = eval(cx, global, "(k, v) => typeof v === 'number' ? v * 2 : v"));
    let text: Vec<u16> = "[1, [2]]".encode_utf16().collect();
    assert!(JS_ParseJSONWithReviver(
        cx,
        text.as_ptr(),
        text.len() as u32,
        reviver.handle(),
        v.handle_mut()
    ));
    assert_eq!(to_json(cx, v.handle()).unwrap(), "[2,[4]]");

    // ToJSON writes nothing for undefined, JS_Stringify writes "null".
    v.set(UndefinedValue());
    assert_eq!(to_json(cx, v.handle()), None);
    let mut out: Option<String> = None;
    assert!(JS_Stringify(
        cx,
        v.handle_mut(),
        HandleObject::null(),
        HandleValue::null(),
        Some(append_json),
        &mut out as *mut _ as *mut c_void
    ));
    assert_eq!(out.as_deref(), Some("null"));

    v.set(eval(
        cx,
        global,
        "({a: [1], toJSON() { return {b: this.a}; }})",
    ));
    rooted!(&in(cx) let space = Int32Value(2));
    let mut out: Option<String> = None;
    assert!(JS_Stringify(
        cx,
        v.handle_mut(),
        HandleObject::null(),
        space.handle(),
        Some(append_json),
        &mut out as *mut _ as *mut c_void
    ));
    assert_eq!(out.as_deref(), Some("{\n  \"b\": [\n    1\n  ]\n}"));

    v.set(eval(cx, global, "({ get x() { throw new Error('no'); } })"));
    let mut out: Option<String> = None;
    assert!(!ToJSON(
        cx,
        v.handle(),
        HandleObject::null(),
        HandleValue::null(),
        Some(append_json),
        &mut out as *mut _ as *mut c_void
    ));
    assert!(take_exception(cx));
    v.set(eval(cx, global, "1n"));
    assert!(!ToJSON(
        cx,
        v.handle(),
        HandleObject::null(),
        HandleValue::null(),
        Some(append_json),
        &mut out as *mut _ as *mut c_void
    ));
    assert!(take_exception(cx));
}
