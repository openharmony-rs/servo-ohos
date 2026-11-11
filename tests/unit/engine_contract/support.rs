/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::ptr;
use std::sync::{Mutex, Once};

use js::context::JSContext;
use js::conversions::jsstr_to_string;
use js::jsapi::{ModuleType, OnNewGlobalHookOption, PromiseState};
use js::jsval::{JSVal, UndefinedValue};
use js::realm::AutoRealm;
use js::rooted;
use js::rust::wrappers2::{
    HasAnyMicroTasks, JS_DequeueNextMicroTask, JS_NewGlobalObject, JS_SetProperty, RunJSMicroTask,
    SetJobQueue,
};
use js::rust::{
    CompileOptionsWrapper, HandleObject, JSEngine, RealmOptions, Runtime, SIMPLE_GLOBAL_CLASS,
    evaluate_script,
};

/// The normalized, engine-independent observation of one contract case.
#[derive(Debug, PartialEq)]
pub enum Value {
    Undefined,
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Object,
    Array(Vec<Value>),
}

impl Value {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Undefined => serde_json::json!({ "type": "undefined" }),
            Value::Null => serde_json::Value::Null,
            Value::Boolean(value) => serde_json::Value::Bool(*value),
            Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
                serde_json::Number::from(*value as i64).into()
            },
            Value::Number(value) => serde_json::json!(value),
            Value::String(value) => serde_json::Value::String(value.clone()),
            Value::Object => serde_json::json!({ "type": "object" }),
            Value::Array(values) => values.iter().map(Value::to_json).collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Observation {
    pub fields: Vec<(&'static str, Value)>,
    pub exception: Option<String>,
}

impl Observation {
    pub fn new(fields: Vec<(&'static str, Value)>) -> Self {
        Observation {
            fields,
            exception: None,
        }
    }

    pub fn ok(value: Value) -> Self {
        Observation {
            fields: vec![("value", value)],
            exception: None,
        }
    }

    pub fn exception(name: &str) -> Self {
        Observation {
            fields: Vec::new(),
            exception: Some(name.to_owned()),
        }
    }

    pub fn failure(message: &str) -> Self {
        Observation {
            fields: vec![("panic", Value::String(message.to_owned()))],
            exception: None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        let fields = self
            .fields
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.to_json()))
            .collect::<serde_json::Map<_, _>>();
        serde_json::json!({
            "fields": fields,
            "exception": self.exception,
        })
    }
}

/// Runs `f` in a fresh runtime and global object, like a document realm.
pub fn with_global<R>(engine: &JSEngine, f: impl FnOnce(&mut JSContext, HandleObject) -> R) -> R {
    with_realm_options(engine, RealmOptions::default(), f)
}

/// Runs `f` in a fresh runtime with caller-selected realm options.
pub fn with_realm_options<R>(
    engine: &JSEngine,
    options: RealmOptions,
    f: impl FnOnce(&mut JSContext, HandleObject) -> R,
) -> R {
    let mut runtime = Runtime::new(engine.handle());
    let cx = runtime.cx();
    unsafe {
        rooted!(&in(cx) let global = JS_NewGlobalObject(
            cx,
            &SIMPLE_GLOBAL_CLASS,
            ptr::null_mut(),
            OnNewGlobalHookOption::FireOnNewGlobalHook,
            &*options,
        ));
        let mut realm = AutoRealm::new_from_handle(cx, global.handle());
        f(&mut realm, global.handle())
    }
}

/// Evaluates `source` in the realm and returns its completion value.
pub fn eval(cx: &mut JSContext, global: HandleObject, source: &str) -> JSVal {
    rooted!(&in(cx) let mut rval = UndefinedValue());
    let options = CompileOptionsWrapper::new(cx, c"engine_contract.js".to_owned(), 1);
    evaluate_script(cx, global, source, rval.handle_mut(), options)
        .unwrap_or_else(|_| panic!("evaluating {source:?} failed"));
    rval.get()
}

/// Sets `globalThis[name] = value` so later snippets can observe a native object.
pub fn set_global(cx: &mut JSContext, global: HandleObject, name: &CStr, value: JSVal) {
    unsafe {
        rooted!(&in(cx) let value = value);
        assert!(
            JS_SetProperty(cx, global, name.as_ptr(), value.handle()),
            "setting global {name:?}"
        );
    }
}

/// Normalizes a JS value without depending on engine-specific types.
pub fn observe(cx: &mut JSContext, value: JSVal) -> Value {
    if value.is_undefined() {
        Value::Undefined
    } else if value.is_null() {
        Value::Null
    } else if value.is_boolean() {
        Value::Boolean(value.to_boolean())
    } else if value.is_number() {
        Value::Number(value.to_number())
    } else if value.is_string() {
        let string = value.to_string();
        let string = std::ptr::NonNull::new(string).expect("non-null JS string");
        Value::String(unsafe { jsstr_to_string(cx, string) })
    } else {
        Value::Object
    }
}

/// Evaluates `source` and normalizes its completion value.
pub fn eval_observe(cx: &mut JSContext, global: HandleObject, source: &str) -> Value {
    let value = eval(cx, global, source);
    observe(cx, value)
}

/// Returns a stable name for a promise state.
pub fn promise_state_name(state: PromiseState) -> &'static str {
    match state {
        PromiseState::Pending => "pending",
        PromiseState::Fulfilled => "fulfilled",
        PromiseState::Rejected => "rejected",
    }
}

/// Returns a stable name for a module request type.
pub fn module_type_name(module_type: ModuleType) -> &'static str {
    match module_type {
        ModuleType::Unknown => "unknown",
        ModuleType::JavaScript => "javascript",
        ModuleType::JSON => "json",
        ModuleType::CSS => "css",
        ModuleType::Bytes => "bytes",
        ModuleType::Text => "text",
    }
}

/// Reads `name` from an arbitrary object.
pub fn get_property(cx: &mut JSContext, object: JSVal, name: &CStr) -> JSVal {
    use js::rust::wrappers2::JS_GetProperty;

    unsafe {
        rooted!(&in(cx) let object = object.to_object());
        rooted!(&in(cx) let mut value = UndefinedValue());
        assert!(
            JS_GetProperty(cx, object.handle(), name.as_ptr(), value.handle_mut()),
            "getting property {name:?}"
        );
        value.get()
    }
}

/// Reads and normalizes `name` from an arbitrary object.
pub fn observe_property(cx: &mut JSContext, object: JSVal, name: &CStr) -> Value {
    let value = get_property(cx, object, name);
    observe(cx, value)
}

static JOB_QUEUE_TRAPS: js::glue::JobQueueTraps = js::glue::JobQueueTraps {
    getHostDefinedData: Some(job_queue_host_defined_data),
    getHostDefinedGlobal: Some(job_queue_host_defined_global),
    runJobs: Some(run_queued_jobs),
    traceNonGCThingMicroTask: None,
};

unsafe extern "C" fn job_queue_host_defined_data(
    _cx: *mut js::jsapi::JSContext,
    incumbent: js::jsapi::MutableHandleObject,
    data: js::jsapi::MutableHandleObject,
) -> bool {
    incumbent.set(ptr::null_mut());
    data.set(ptr::null_mut());
    true
}

unsafe extern "C" fn job_queue_host_defined_global(
    _cx: *mut js::jsapi::JSContext,
    _incumbent: js::jsapi::MutableHandleObject,
) -> bool {
    true
}

unsafe extern "C" fn run_queued_jobs(cx: *mut js::jsapi::JSContext) {
    let cx = std::ptr::NonNull::new(cx).expect("non-null JSContext in the job queue trap");
    let mut cx = JSContext::from_ptr(cx);
    while HasAnyMicroTasks(&cx) {
        rooted!(&in(cx) let mut task = UndefinedValue());
        JS_DequeueNextMicroTask(&cx, task.handle_mut());
        assert!(js::jsapi::IsJSMicroTask(task.as_ptr()));
        let task = js::jsapi::ToMaybeWrappedJSMicroTask(task.as_ptr());
        rooted!(&in(cx) let task = task);
        assert!(RunJSMicroTask(&mut cx, task.handle()));
    }
}

/// Installs a host job queue for the current context and returns the queue to delete after use.
pub unsafe fn install_job_queue(cx: &mut JSContext) -> *mut std::ffi::c_void {
    let queue = js::glue::CreateJobQueue(&JOB_QUEUE_TRAPS);
    assert!(!queue.is_null());
    SetJobQueue(cx, queue as *mut js::jsapi::JobQueue);
    queue.cast()
}

pub unsafe fn remove_job_queue(cx: &mut JSContext, queue: *mut std::ffi::c_void) {
    SetJobQueue(cx, ptr::null_mut());
    js::glue::DeleteJobQueue(queue.cast());
}

/// Writes one JSON line per case so the two engine runs can be diffed.
///
/// `apis` names every mozjs entry point the case exercises. The coverage gate checks these names
/// against `api-manifest.toml`, so a case cannot silently claim capability it never calls.
pub fn record(case: &str, observation: &Observation, apis: &[&str]) {
    static RESET: Once = Once::new();

    let Ok(path) = std::env::var("CONTRACT_OUT") else {
        return;
    };
    if let Some(parent) = Path::new(&path).parent() {
        std::fs::create_dir_all(parent).expect("creating CONTRACT_OUT parent directory");
    }
    RESET.call_once(|| {
        std::fs::write(&path, b"").expect("truncating CONTRACT_OUT");
    });
    let engine = std::env::var("CONTRACT_ENGINE").unwrap_or_else(|_| "unknown".to_owned());
    let observation = observation.to_json();
    let line = serde_json::json!({
        "id": case,
        "engine": engine,
        "fields": observation["fields"],
        "exception": observation["exception"],
        "apis": apis,
    });
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("opening CONTRACT_OUT");
    writeln!(file, "{line}").expect("writing CONTRACT_OUT");
}

static CASE_FAILURES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "case panicked with a non-string payload".to_owned()
    }
}

/// Runs one case in isolation. A panic is recorded as that case's observation and collected for
/// the final test failure, so every later case still runs on the same engine.
pub fn run_case(case: &str, apis: &[&str], f: impl FnOnce() -> Observation) {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(observation) => record(case, &observation, apis),
        Err(payload) => {
            let message = panic_message(payload);
            eprintln!("case {case} failed: {message}");
            record(case, &Observation::failure(&message), apis);
            CASE_FAILURES
                .lock()
                .expect("case failure list")
                .push(format!("{case}: {message}"));
        },
    }
}

/// Isolates a case that records itself on success. On panic, a failure observation is written
/// under the same case id with no API list; the failed run is therefore never silently compared
/// as a match, and later cases still execute.
pub fn isolate_case(case: &str, f: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        let message = panic_message(payload);
        eprintln!("case {case} failed: {message}");
        record(case, &Observation::failure(&message), &[]);
        CASE_FAILURES
            .lock()
            .expect("case failure list")
            .push(format!("{case}: {message}"));
    }
}

/// Fails the enclosing test after every case has had a chance to run.
pub fn assert_no_case_failures() {
    let failures = CASE_FAILURES.lock().expect("case failure list");
    if !failures.is_empty() {
        panic!(
            "{} engine contract case(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

/// Runs a module after its cases have been isolated. A panic outside a case still stops that
/// module, but it is reported with the other failures instead of hiding later modules.
pub fn run_module(name: &str, f: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        let message = panic_message(payload);
        eprintln!("module {name} failed: {message}");
        CASE_FAILURES
            .lock()
            .expect("case failure list")
            .push(format!("{name}: {message}"));
    }
}

/// Runs a case in a fresh runtime and records its observation together with the entries it
/// exercises. The `apis` list is the coverage contract: every entry named here must be classified
/// as implemented in `api-manifest.toml`.
pub fn record_case(
    engine: &JSEngine,
    case: &str,
    apis: &[&str],
    f: impl FnOnce(&mut JSContext, HandleObject) -> Observation,
) {
    run_case(case, apis, || with_global(engine, |cx, global| f(cx, global)));
}

/// Like [`record_case`], but constructs the realm with the supplied options.
pub fn record_case_with_options(
    engine: &JSEngine,
    case: &str,
    apis: &[&str],
    options: RealmOptions,
    f: impl FnOnce(&mut JSContext, HandleObject) -> Observation,
) {
    run_case(case, apis, || {
        with_realm_options(engine, options, |cx, global| f(cx, global))
    });
}
