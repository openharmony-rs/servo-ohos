/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Promise values, reactions, rejection tracking and the microtask job queue.

use std::cell::RefCell;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use js::jsapi::{
    self, CallArgs, JSSecurityCallbacks, OnNewGlobalHookOption, PromiseRejectionHandlingState,
    PromiseState, PromiseUserInputEventHandlingState, Value,
};
use js::jsval::{Int32Value, ObjectValue, UndefinedValue};
use js::realm::AutoRealm;
use js::rooted;
use js::rust::wrappers2::{
    AddPromiseReactions, CallOriginalPromiseReject, CallOriginalPromiseResolve,
    DequeueNextRegularMicroTask, EnqueueMicroTask, GetPromiseIsHandled, GetPromiseState,
    GetJobQueue, GetRegularMicroTaskCount, HasAnyMicroTasks,
    HasRegularMicroTasks, IsPromiseObject,
    JS_DequeueNextMicroTask, NewPromiseObject, PeekNextMicroTask, PrependMicroTask, RejectPromise,
    MaybeGetHostDefinedDataFromJSMicroTask, ResolvePromise,
    RunJSMicroTask, SetAnyPromiseIsHandled, SetJobQueue,
    SetPromiseRejectionTrackerCallback, SetPromiseUserInputEventHandlingState,
    UseInternalJobQueues, JS_AddInterruptCallback, JS_GetSecurityCallbacks,
    JS_RequestInterruptCallback, JS_RequestInterruptCallbackCanWait, JS_SetSecurityCallbacks,
};
use js::rust::wrappers2::JS_NewGlobalObject;
use js::rust::{JSEngine, RealmOptions, SIMPLE_GLOBAL_CLASS};

use crate::support::{
    eval, observe, record, record_case, set_global, Observation, Value as Observed,
};

thread_local! {
    static FULFILLED: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
    static REJECTIONS: RefCell<Vec<PromiseRejectionHandlingState>> = const { RefCell::new(Vec::new()) };
}

static INTERRUPTS: AtomicUsize = AtomicUsize::new(0);
static KEEP_RUNNING: AtomicBool = AtomicBool::new(true);
static SECURITY_CALLBACKS: JSSecurityCallbacks = JSSecurityCallbacks {
    contentSecurityPolicyAllows: None,
    codeForEvalGets: None,
    subsumes: None,
};

unsafe extern "C" fn interrupt_callback(_cx: *mut jsapi::JSContext) -> bool {
    INTERRUPTS.fetch_add(1, Ordering::SeqCst);
    KEEP_RUNNING.load(Ordering::SeqCst)
}

unsafe extern "C" fn on_fulfilled(_cx: *mut jsapi::JSContext, argc: u32, vp: *mut Value) -> bool {
    let args = CallArgs::from_vp(vp, argc);
    FULFILLED.with(|f| f.borrow_mut().push(args.get(0).to_int32()));
    args.rval().set(UndefinedValue());
    true
}

unsafe extern "C" fn on_rejected(_cx: *mut jsapi::JSContext, argc: u32, vp: *mut Value) -> bool {
    let args = CallArgs::from_vp(vp, argc);
    FULFILLED.with(|f| f.borrow_mut().push(-args.get(0).to_int32()));
    args.rval().set(UndefinedValue());
    true
}

unsafe extern "C" fn executor(_cx: *mut jsapi::JSContext, argc: u32, vp: *mut Value) -> bool {
    let args = CallArgs::from_vp(vp, argc);
    args.rval().set(UndefinedValue());
    true
}

unsafe extern "C" fn get_host_defined_data(
    _cx: *mut jsapi::JSContext,
    incumbent: jsapi::MutableHandleObject,
    data: jsapi::MutableHandleObject,
) -> bool {
    incumbent.set(ptr::null_mut());
    data.set(ptr::null_mut());
    true
}

unsafe extern "C" fn get_host_defined_global(
    _cx: *mut jsapi::JSContext,
    _incumbent: jsapi::MutableHandleObject,
) -> bool {
    true
}

unsafe extern "C" fn track_rejection(
    _cx: *mut jsapi::JSContext,
    _muted_errors: bool,
    _promise: js::jsapi::HandleObject,
    state: PromiseRejectionHandlingState,
    _data: *mut c_void,
) {
    REJECTIONS.with(|r| r.borrow_mut().push(state));
}

unsafe fn new_function(
    cx: &mut js::context::JSContext,
    native: jsapi::JSNative,
    name: &std::ffi::CStr,
) -> *mut jsapi::JSFunction {
    let function = jsapi::JS_NewFunction(cx.raw_cx(), native, 1, 0, name.as_ptr());
    assert!(!function.is_null());
    function
}

fn promise_values(engine: &JSEngine) {
    record_case(
        engine,
        "promise.values_and_reactions",
        &[
            "AddPromiseReactions",
            "JS_AddInterruptCallback",
            "JS_GetSecurityCallbacks",
            "JS_RequestInterruptCallback",
            "JS_RequestInterruptCallbackCanWait",
            "JS_SetSecurityCallbacks",
            "CallOriginalPromiseReject",
            "CallOriginalPromiseResolve",
            "GetPromiseIsHandled",
            "GetPromiseState",
            "GetPromiseUserInputEventHandlingState",
            "JS_GetPromiseResult",
            "IsPromiseObject",
            "NewPromiseObject",
            "RejectPromise",
            "ResolvePromise",
            "SetAnyPromiseIsHandled",
            "SetPromiseUserInputEventHandlingState",
            "SetPromiseRejectionTrackerCallback",
        ],
        |cx, global| unsafe {
            rooted!(&in(cx) let executor = new_function(cx, Some(executor), c"executor"));
            rooted!(&in(cx) let executor_object = jsapi::JS_GetFunctionObject(executor.get()));

            rooted!(&in(cx) let promise = NewPromiseObject(cx, executor_object.handle()));
            assert!(IsPromiseObject(promise.handle()));
            assert_eq!(GetPromiseState(promise.handle()), PromiseState::Pending);
            rooted!(&in(cx) let value = Int32Value(42));
            assert!(ResolvePromise(cx, promise.handle(), value.handle()));
            assert_eq!(GetPromiseState(promise.handle()), PromiseState::Fulfilled);
            rooted!(&in(cx) let mut result = UndefinedValue());
            js::glue::JS_GetPromiseResult(promise.handle().into(), result.handle_mut().into());
            assert_eq!(result.get().to_int32(), 42);
            assert!(RejectPromise(cx, promise.handle(), value.handle()));
            assert_eq!(GetPromiseState(promise.handle()), PromiseState::Fulfilled);

            rooted!(&in(cx) let rejected = CallOriginalPromiseReject(cx, value.handle()));
            assert_eq!(GetPromiseState(rejected.handle()), PromiseState::Rejected);
            assert!(!GetPromiseIsHandled(rejected.handle()));
            assert!(SetAnyPromiseIsHandled(cx, rejected.handle()));
            assert!(GetPromiseIsHandled(rejected.handle()));

            rooted!(&in(cx) let resolved = CallOriginalPromiseResolve(cx, value.handle()));
            assert_eq!(GetPromiseState(resolved.handle()), PromiseState::Fulfilled);
            rooted!(&in(cx) let resolved_value = ObjectValue(resolved.get()));
            assert_eq!(CallOriginalPromiseResolve(cx, resolved_value.handle()), resolved.get());

            assert!(SetPromiseUserInputEventHandlingState(
                resolved.handle(),
                PromiseUserInputEventHandlingState::HadUserInteractionAtCreation
            ));
            assert_eq!(
                jsapi::GetPromiseUserInputEventHandlingState(resolved.get()),
                PromiseUserInputEventHandlingState::HadUserInteractionAtCreation
            );

            let queue = js::glue::CreateJobQueue(&QUEUE_TRAPS);
            assert!(!queue.is_null());
            SetJobQueue(cx, queue as *mut jsapi::JobQueue);
            FULFILLED.with(|f| f.borrow_mut().clear());
            rooted!(&in(cx) let on_fulfilled_fn = new_function(cx, Some(on_fulfilled), c"onFulfilled"));
            rooted!(&in(cx) let on_fulfilled = jsapi::JS_GetFunctionObject(on_fulfilled_fn.get()));
            rooted!(&in(cx) let on_rejected_fn = new_function(cx, Some(on_rejected), c"onRejected"));
            rooted!(&in(cx) let on_rejected = jsapi::JS_GetFunctionObject(on_rejected_fn.get()));
            assert!(AddPromiseReactions(
                cx,
                resolved.handle(),
                on_fulfilled.handle(),
                on_rejected.handle()
            ));

            jsapi::RunJobs(cx.raw_cx());
            assert_eq!(FULFILLED.with(|f| f.borrow().clone()), [42]);
            REJECTIONS.with(|r| r.borrow_mut().clear());
            SetPromiseRejectionTrackerCallback(cx, Some(track_rejection), ptr::null_mut());
            rooted!(&in(cx) let tracked = CallOriginalPromiseReject(cx, value.handle()));
            assert_eq!(
                REJECTIONS.with(|r| r.borrow().clone()),
                [PromiseRejectionHandlingState::Unhandled]
            );
            assert!(SetAnyPromiseIsHandled(cx, tracked.handle()));
            assert_eq!(
                REJECTIONS.with(|r| r.borrow().clone()),
                [
                    PromiseRejectionHandlingState::Unhandled,
                    PromiseRejectionHandlingState::Handled
                ]
            );
            SetPromiseRejectionTrackerCallback(cx, None, ptr::null_mut());

            INTERRUPTS.store(0, Ordering::SeqCst);
            KEEP_RUNNING.store(true, Ordering::SeqCst);
            assert!(JS_AddInterruptCallback(cx, Some(interrupt_callback)));
            JS_RequestInterruptCallback(cx);
            rooted!(&in(cx) let counted = eval(cx, global, "for (var i = 0; i < 10000; i++) {} i"));
            assert_eq!(counted.get().to_int32(), 10000);
            assert_eq!(INTERRUPTS.load(Ordering::SeqCst), 1);
            JS_RequestInterruptCallbackCanWait(cx);
            rooted!(&in(cx) let counted = eval(cx, global, "for (var i = 0; i < 10000; i++) {} i"));
            assert_eq!(counted.get().to_int32(), 10000);
            assert_eq!(INTERRUPTS.load(Ordering::SeqCst), 2);

            JS_SetSecurityCallbacks(cx, &SECURITY_CALLBACKS);
            assert_eq!(
                JS_GetSecurityCallbacks(cx),
                &SECURITY_CALLBACKS as *const _
            );
            JS_SetSecurityCallbacks(cx, ptr::null());

            SetJobQueue(cx, ptr::null_mut());
            js::glue::DeleteJobQueue(queue);

            set_global(cx, global, c"p", ObjectValue(resolved.get()));
            rooted!(&in(cx) let check = eval(cx, global, "p instanceof Promise && p.then !== undefined"));
            let observed = observe(cx, check.get());
            Observation::ok(observed)
        },
    );
}

fn job_queue(engine: &JSEngine) {
    record_case(
        engine,
        "promise.job_queue",
        &[
            "CreateJobQueue",
            "DeleteJobQueue",
            "DequeueNextRegularMicroTask",
            "EnqueueMicroTask",
            "GetExecutionGlobalFromJSMicroTask",
            "GetJobQueue",
            "GetRegularMicroTaskCount",
            "HasAnyMicroTasks",
            "HasRegularMicroTasks",
            "IsJSMicroTask",
            "JS_DequeueNextMicroTask",
            "MaybeGetHostDefinedDataFromJSMicroTask",
            "MaybeGetPromiseFromJSMicroTask",
            "PeekNextMicroTask",
            "PrependMicroTask",
            "RunJSMicroTask",
            "SetJobQueue",
            "ToMaybeWrappedJSMicroTask",
        ],
        |cx, global| unsafe {
            let queue = js::glue::CreateJobQueue(&QUEUE_TRAPS);
            assert!(!queue.is_null());
            SetJobQueue(cx, queue as *mut jsapi::JobQueue);
            assert_eq!(GetJobQueue(cx), queue as *mut jsapi::JobQueue);
            assert!(eval(
                cx,
                global,
                "var log = [];
                 Promise.resolve(1).then(() => log.push('a'));
                 Promise.resolve(2).then(() => log.push('b'));
                 log.join() === ''"
            )
            .to_boolean());
            assert!(HasAnyMicroTasks(cx));
            assert!(HasRegularMicroTasks(cx));
            assert_eq!(GetRegularMicroTaskCount(cx), 2);
            rooted!(&in(cx) let peeked = PeekNextMicroTask(cx));
            assert!(!peeked.is_null());
            assert!(jsapi::IsJSMicroTask(peeked.as_ptr()));

            rooted!(&in(cx) let a = DequeueNextRegularMicroTask(cx));
            rooted!(&in(cx) let b = DequeueNextRegularMicroTask(cx));
            assert!(jsapi::IsJSMicroTask(a.as_ptr()));
            assert!(jsapi::IsJSMicroTask(b.as_ptr()));
            assert_eq!(GetRegularMicroTaskCount(cx), 0);
            let micro = jsapi::ToMaybeWrappedJSMicroTask(a.as_ptr());
            assert!(!micro.is_null());
            assert!(!jsapi::GetExecutionGlobalFromJSMicroTask(micro).is_null());
            let promise = jsapi::MaybeGetPromiseFromJSMicroTask(micro);
            assert!(!promise.is_null());
            rooted!(&in(cx) let mut incumbent = ptr::null_mut::<jsapi::JSObject>());
            rooted!(&in(cx) let mut data = ptr::null_mut::<jsapi::JSObject>());
            assert!(MaybeGetHostDefinedDataFromJSMicroTask(
                micro,
                incumbent.handle_mut(),
                data.handle_mut()
            ));
            assert!(PrependMicroTask(cx, b.as_ptr()));
            assert!(EnqueueMicroTask(cx, a.as_ptr()));
            assert_eq!(GetRegularMicroTaskCount(cx), 2);

            rooted!(&in(cx) let mut next = UndefinedValue());
            JS_DequeueNextMicroTask(cx, next.handle_mut());
            assert!(jsapi::IsJSMicroTask(next.as_ptr()));
            let next_micro = jsapi::ToMaybeWrappedJSMicroTask(next.as_ptr());
            rooted!(&in(cx) let next_micro = next_micro);
            assert!(RunJSMicroTask(cx, next_micro.handle()));

            JS_DequeueNextMicroTask(cx, next.handle_mut());
            assert!(jsapi::IsJSMicroTask(next.as_ptr()));
            let next_micro = jsapi::ToMaybeWrappedJSMicroTask(next.as_ptr());
            rooted!(&in(cx) let next_micro = next_micro);
            assert!(RunJSMicroTask(cx, next_micro.handle()));
            assert!(!HasAnyMicroTasks(cx));
            rooted!(&in(cx) let log = eval(cx, global, "log.join()"));
            assert_eq!(observe(cx, log.get()), Observed::String("b,a".to_owned()));

            SetJobQueue(cx, ptr::null_mut());
            js::glue::DeleteJobQueue(queue);
            Observation::ok(Observed::Boolean(true))
        },
    );
}

fn internal_job_queue(engine: &JSEngine) {
    let observed = unsafe {
        let cx = jsapi::JS_NewContext(1 << 20, ptr::null_mut());
        assert!(!cx.is_null(), "JS_NewContext");
        let cx = NonNull::new(cx).unwrap();
        let mut cx = js::context::JSContext::from_ptr(cx);

        // SpiderMonkey requires this before self-hosting is initialized.
        assert!(UseInternalJobQueues(&cx));

        let cache = jsapi::__BindgenOpaqueArray::<u64, 2>::default();
        assert!(jsapi::InitSelfHostedCode(cx.raw_cx(), cache, None));
        jsapi::JS_SetNativeStackQuota(cx.raw_cx(), 256 * 1024, 192 * 1024, 128 * 1024);

        {
            rooted!(&in(cx) let global = JS_NewGlobalObject(
                &mut cx,
                &SIMPLE_GLOBAL_CLASS,
                ptr::null_mut(),
                OnNewGlobalHookOption::FireOnNewGlobalHook,
                &*RealmOptions::default(),
            ));
            let mut realm = AutoRealm::new_from_handle(&mut cx, global.handle());
            let (global, cx) = realm.global_and_reborrow();
            let queued = eval(
                cx,
                global,
                "globalThis.log = [];
                 Promise.resolve(1).then(value => log.push(value));
                 log.join()",
            );
            assert_eq!(observe(cx, queued), Observed::String(String::new()));
            assert!(HasAnyMicroTasks(cx));
            jsapi::RunJobs(cx.raw_cx());
            let drained = eval(cx, global, "log.join()");
            assert_eq!(observe(cx, drained), Observed::String("1".to_owned()));
            assert!(!HasAnyMicroTasks(cx));
        }

        jsapi::JS_DestroyContext(cx.raw_cx());
        let _ = engine;
        Observation::ok(Observed::String("1".to_owned()))
    };
    record(
        "promise.internal_job_queue",
        &observed,
        &[
            "HasAnyMicroTasks",
            "JS_NewContext",
            "JS_NewGlobalObject",
            "JS_SetNativeStackQuota",
            "RunJobs",
            "UseInternalJobQueues",
        ],
    );
}

static QUEUE_TRAPS: js::glue::JobQueueTraps = js::glue::JobQueueTraps {
    getHostDefinedData: Some(get_host_defined_data),
    getHostDefinedGlobal: Some(get_host_defined_global),
    runJobs: Some(run_jobs),
    traceNonGCThingMicroTask: None,
};

unsafe extern "C" fn run_jobs(cx: *mut jsapi::JSContext) {
    let cx = NonNull::new(cx).expect("non-null JSContext in the job queue trap");
    let mut cx = js::context::JSContext::from_ptr(cx);
    while HasAnyMicroTasks(&cx) {
        rooted!(&in(cx) let mut task = UndefinedValue());
        JS_DequeueNextMicroTask(&cx, task.handle_mut());
        assert!(jsapi::IsJSMicroTask(task.as_ptr()));
        let task = jsapi::ToMaybeWrappedJSMicroTask(task.as_ptr());
        rooted!(&in(cx) let task = task);
        assert!(RunJSMicroTask(&mut cx, task.handle()));
    }
}

pub fn run(engine: &JSEngine) {
    crate::support::isolate_case("promise.values_and_reactions", || promise_values(engine));
    crate::support::isolate_case("promise.job_queue", || job_queue(engine));
    crate::support::isolate_case("promise.internal_job_queue", || {
        internal_job_queue(engine)
    });
}
