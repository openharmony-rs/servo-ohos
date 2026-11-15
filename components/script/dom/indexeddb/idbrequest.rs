/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#![cfg_attr(crown, allow(crown::jscontext_first_arg))]

use std::cell::Cell;

use dom_struct::dom_struct;
use js::context::JSContext;
use js::conversions::ToJSValConvertible;
use js::jsapi::Heap;
use js::jsval::{DoubleValue, JSVal, NullValue, ObjectValue, UndefinedValue};
use js::rust::{HandleValue, MutableHandleValue};
use script_bindings::reflector::{DomObject, reflect_dom_object};
use servo_base::generic_channel::GenericSend;
use storage_traits::indexeddb::{
    AsyncOperation, AsyncReadOnlyOperation, BackendError, BackendResult, IndexedDBKeyType,
    IndexedDBRecord, IndexedDBThreadMsg, IndexedDBTxnMode, IndexedDBValue, PutItemResult,
    RequestOutput,
};
use stylo_atoms::Atom;

use crate::dom::bindings::codegen::Bindings::IDBRequestBinding::{
    IDBRequestMethods, IDBRequestReadyState,
};
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::IDBTransactionMode;
use crate::dom::bindings::error::{Error, Fallible, create_dom_exception};
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::bindings::root::{DomRoot, MutNullableDom};
use crate::dom::domexception::DOMException;
use crate::dom::event::{Event, EventBubbles, EventCancelable};
use crate::dom::eventtarget::EventTarget;
use crate::dom::globalscope::GlobalScope;
use crate::dom::indexeddb::idbcursor::{IterationParam, iterate_cursor};
use crate::dom::indexeddb::idbcursorwithvalue::IDBCursorWithValue;
use crate::dom::indexeddb::idbobjectstore::IDBObjectStore;
use crate::dom::indexeddb::idbtransaction::IDBTransaction;
use crate::dom::indexeddb::key::key_type_to_jsval;

pub enum IdbResult {
    Key(IndexedDBKeyType),
    Keys(Vec<IndexedDBKeyType>),
    Value(IndexedDBValue),
    Values(Vec<IndexedDBValue>),
    Count(u64),
    Iterate(Vec<IndexedDBRecord>),
    Error(Error),
    None,
}

impl From<IndexedDBKeyType> for IdbResult {
    fn from(value: IndexedDBKeyType) -> Self {
        IdbResult::Key(value)
    }
}

impl From<Vec<IndexedDBKeyType>> for IdbResult {
    fn from(value: Vec<IndexedDBKeyType>) -> Self {
        IdbResult::Keys(value)
    }
}

impl From<IndexedDBValue> for IdbResult {
    fn from(value: IndexedDBValue) -> Self {
        IdbResult::Value(value)
    }
}

impl From<Vec<IndexedDBValue>> for IdbResult {
    fn from(value: Vec<IndexedDBValue>) -> Self {
        IdbResult::Values(value)
    }
}

impl From<PutItemResult> for IdbResult {
    fn from(value: PutItemResult) -> Self {
        match value {
            PutItemResult::Key(key) => Self::Key(key),
            PutItemResult::CannotOverwrite | PutItemResult::KeyGeneratorExhausted => {
                Self::Error(Error::Constraint(None))
            },
        }
    }
}

impl From<Vec<IndexedDBRecord>> for IdbResult {
    fn from(value: Vec<IndexedDBRecord>) -> Self {
        Self::Iterate(value)
    }
}

impl From<()> for IdbResult {
    fn from(_value: ()) -> Self {
        Self::None
    }
}

impl<T> From<Option<T>> for IdbResult
where
    T: Into<IdbResult>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => IdbResult::None,
        }
    }
}

impl From<u64> for IdbResult {
    fn from(value: u64) -> Self {
        IdbResult::Count(value)
    }
}

impl From<RequestOutput> for IdbResult {
    fn from(output: RequestOutput) -> Self {
        match output {
            RequestOutput::Key(key) => key.into(),
            RequestOutput::Value(value) => value.into(),
            RequestOutput::Keys(keys) => keys.into(),
            RequestOutput::Values(values) => values.into(),
            RequestOutput::Count(count) => count.into(),
            RequestOutput::Records(records) => records.into(),
            RequestOutput::Put(result) => result.into(),
            RequestOutput::Done => IdbResult::None,
        }
    }
}

#[dom_struct]
pub struct IDBRequest {
    eventtarget: EventTarget,
    #[ignore_malloc_size_of = "mozjs"]
    result: Heap<JSVal>,
    error: MutNullableDom<DOMException>,
    source: MutNullableDom<IDBObjectStore>,
    transaction: MutNullableDom<IDBTransaction>,
    ready_state: Cell<IDBRequestReadyState>,
}

impl IDBRequest {
    pub fn new_inherited() -> IDBRequest {
        IDBRequest {
            eventtarget: EventTarget::new_inherited(),

            result: Heap::default(),
            error: Default::default(),
            source: Default::default(),
            transaction: Default::default(),
            ready_state: Cell::new(IDBRequestReadyState::Pending),
        }
    }

    pub fn new(cx: &mut JSContext, global: &GlobalScope) -> DomRoot<IDBRequest> {
        reflect_dom_object(cx, Box::new(IDBRequest::new_inherited()), global)
    }

    pub fn set_source(&self, source: Option<&IDBObjectStore>) {
        self.source.set(source);
    }

    pub fn set_ready_state_done(&self) {
        self.ready_state.set(IDBRequestReadyState::Done);
    }

    pub fn set_result(&self, result: HandleValue) {
        self.result.set(result.get());
    }

    pub fn set_error(&self, cx: &mut JSContext, error: Option<Error>) {
        if let Some(error) = error {
            if let Ok(exception) = create_dom_exception(cx, &self.global(), error) {
                self.error.set(Some(&exception));
            }
        } else {
            self.error.set(None);
        }
    }

    pub fn set_transaction(&self, transaction: &IDBTransaction) {
        self.transaction.set(Some(transaction));
    }

    pub fn clear_transaction(&self) {
        self.transaction.set(None);
    }

    pub fn unset_done_flag(&self) {
        self.ready_state.set(IDBRequestReadyState::Pending);
    }

    fn is_done(&self) -> bool {
        self.ready_state.get() == IDBRequestReadyState::Done
    }

    pub(crate) fn transaction(&self) -> Option<DomRoot<IDBTransaction>> {
        self.transaction.get()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#asynchronously-execute-a-request>
    ///
    /// The result arrives on the connection's result channel and is delivered by the
    /// transaction, in request order, see `IDBTransaction::handle_event`.
    pub fn execute_async(
        cx: &mut JSContext,
        source: &IDBObjectStore,
        operation: AsyncOperation,
        request: Option<DomRoot<IDBRequest>>,
        iteration_param: Option<IterationParam>,
    ) -> Fallible<DomRoot<IDBRequest>> {
        // Step 1: Let transaction be the transaction associated with source.
        let transaction = source.transaction();
        let global = transaction.global();
        // Step 2: Assert: transaction is active.
        if !transaction.is_active() || !transaction.is_usable() {
            return Err(Error::TransactionInactive(None));
        }

        if matches!(
            operation,
            AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Iterate { .. })
        ) {
            assert!(
                iteration_param.is_some(),
                "iteration_param must be provided for Iterate"
            );
        } else {
            assert!(
                iteration_param.is_none(),
                "iteration_param should not be provided for operation other than Iterate"
            );
        }

        let request_id = transaction.allocate_request_id();

        // Step 3: If request was not given, let request be a new request with source as source.
        let request = request.unwrap_or_else(|| {
            let new_request = IDBRequest::new(cx, &global);
            new_request.set_source(Some(source));
            new_request.set_transaction(&transaction);
            new_request
        });

        // Step 4: Add request to the end of transaction’s request list.
        transaction.add_request(&request, request_id, iteration_param);

        // Step 5: Run these steps in parallel:
        // The backend runs the requests of a transaction in the order they arrive, which is the
        // order they were made in, and reports the result on the connection's result channel.
        let transaction_mode = match transaction.get_mode() {
            IDBTransactionMode::Readonly => IndexedDBTxnMode::Readonly,
            IDBTransactionMode::Readwrite => IndexedDBTxnMode::Readwrite,
            IDBTransactionMode::Versionchange => IndexedDBTxnMode::Versionchange,
        };
        transaction
            .global()
            .storage_threads()
            .send(IndexedDBThreadMsg::Async(
                global.origin().immutable().clone(),
                String::from(transaction.get_db_name()),
                source.id(),
                transaction.id(),
                request_id,
                transaction_mode,
                operation,
            ))
            .unwrap();

        // Step 6: Return request.
        Ok(request)
    }

    /// Converts the result of this request, as the backend reported it, into `answer`, or
    /// returns the error that is the request's result.
    pub(crate) fn convert_result(
        &self,
        cx: &mut JSContext,
        result: BackendResult<RequestOutput>,
        iteration_param: Option<&IterationParam>,
        mut answer: MutableHandleValue,
    ) -> Result<(), Error> {
        let result = result
            .inspect_err(|error| {
                if let BackendError::DbErr(error) = error {
                    error!("Error in IndexedDB operation: {}", error);
                }
            })
            // FIXME:(arihant2math) dispatch correct error
            .map_err(|_| Error::Data(None))?;
        match IdbResult::from(result) {
            IdbResult::Key(key) => key_type_to_jsval(cx, &key, answer),
            IdbResult::Keys(keys) => {
                rooted!(&in(cx) let mut array = vec![JSVal::default(); keys.len()]);
                for (i, key) in keys.into_iter().enumerate() {
                    key_type_to_jsval(cx, &key, array.handle_mut_at(i));
                }
                array.to_jsval(cx, answer);
            },
            IdbResult::Value(value) => {
                let store = self.source.get().expect("Value requests have a source");
                store
                    .deserialize_value(cx, &value.primary_key, &value.value, answer)
                    .inspect_err(|_| warn!("Error reading structuredclone data"))?;
            },
            IdbResult::Values(serialized_values) => {
                let store = self.source.get().expect("Value requests have a source");
                rooted!(&in(cx) let mut values = vec![JSVal::default(); serialized_values.len()]);
                for (i, value) in serialized_values.into_iter().enumerate() {
                    store
                        .deserialize_value(
                            cx,
                            &value.primary_key,
                            &value.value,
                            values.handle_mut_at(i),
                        )
                        .inspect_err(|_| warn!("Error reading structuredclone data"))?;
                }
                values.to_jsval(cx, answer);
            },
            IdbResult::Count(count) => {
                answer.set(DoubleValue(count as f64));
            },
            IdbResult::Iterate(records) => {
                let param = iteration_param.expect(
                    "iteration_param must be provided by IDBRequest::execute_async for Iterate",
                );
                let cursor = iterate_cursor(cx, param, records)
                    .inspect_err(|_| warn!("Error reading structuredclone data"))?;
                match cursor {
                    Some(cursor) => match cursor.downcast::<IDBCursorWithValue>() {
                        Some(cursor_with_value) => {
                            answer.set(ObjectValue(*cursor_with_value.reflector().get_jsobject()));
                        },
                        None => {
                            answer.set(ObjectValue(*cursor.reflector().get_jsobject()));
                        },
                    },
                    None => answer.set(NullValue()),
                }
            },
            IdbResult::None => {},
            IdbResult::Error(error) => return Err(error),
        }
        Ok(())
    }

    /// <https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request>
    /// Step 5.8, the queued task, for a request whose result is `answer`.
    pub(crate) fn finish_with_success(
        &self,
        cx: &mut JSContext,
        transaction: &IDBTransaction,
        answer: HandleValue,
    ) {
        // Step 5.8.2: Set request’s done flag to true.
        self.set_ready_state_done();

        // Step 5.8.4.1: Set request’s result to result.
        self.set_result(answer);

        // Step 5.8.4.2: Set request’s error to undefined.
        self.set_error(cx, None);

        // Step 5.8.4.3: Fire a success event at request.
        // https://w3c.github.io/IndexedDB/#fire-success-event
        // Step 1: Let event be the result of creating an event using Event.
        // Step 2: Set event’s type attribute to "success".
        // Step 3: Set event’s bubbles and cancelable attributes to false.
        let global = self.global();
        let event = Event::new(
            cx,
            &global,
            Atom::from("success"),
            EventBubbles::DoesNotBubble,
            EventCancelable::NotCancelable,
        );

        // Step 5: Let legacyOutputDidListenersThrowFlag be initially false.
        let did_listeners_throw = Cell::new(false);
        // Step 6: If transaction’s state is inactive, then set transaction’s state to active.
        if transaction.is_inactive() {
            transaction.set_active_flag(true);
        }
        // Step 7: Dispatch event at request with legacyOutputDidListenersThrowFlag.
        event
            .upcast::<Event>()
            .fire_with_legacy_output_did_listeners_throw(cx, self.upcast(), &did_listeners_throw);
        // Step 8: If transaction’s state is active, then:
        if transaction.is_active() {
            // Step 8.1: Set transaction’s state to inactive.
            transaction.set_active_flag(false);
            // Step 8.2: If legacyOutputDidListenersThrowFlag is true, then run abort a
            // transaction with transaction and a newly created "AbortError" DOMException.
            if did_listeners_throw.get() {
                transaction.abort(cx, Error::Abort(None));
            }
        }
        // Step 8.3: If transaction’s request list is empty, then run commit a transaction with
        // transaction.
        transaction.maybe_commit(cx);
    }

    /// <https://w3c.github.io/IndexedDB/#asynchronously-execute-a-request>
    /// Step 5.8, the queued task, for a request whose result is the error `error`.
    pub(crate) fn finish_with_error(
        &self,
        cx: &mut JSContext,
        transaction: &IDBTransaction,
        error: Error,
    ) {
        // Step 5.8.2: Set request’s done flag to true.
        self.set_ready_state_done();

        // Step 5.8.3.1: Set request’s result to undefined.
        rooted!(&in(cx) let undefined = UndefinedValue());
        self.set_result(undefined.handle());

        // Step 5.8.3.2: Set request’s error to result.
        self.set_error(cx, Some(error.clone()));

        // Step 5.8.3.3: Fire an error event at request.
        // https://w3c.github.io/IndexedDB/#fire-error-event
        // Step 1: Let event be the result of creating an event using Event.
        // Step 2: Set event’s type attribute to "error".
        // Step 3: Set event’s bubbles and cancelable attributes to true.
        let global = self.global();
        let event = Event::new(
            cx,
            &global,
            Atom::from("error"),
            EventBubbles::Bubbles,
            EventCancelable::Cancelable,
        );

        // Step 5: Let legacyOutputDidListenersThrowFlag be initially false.
        let did_listeners_throw = Cell::new(false);
        // Step 6: If transaction’s state is inactive, then set transaction’s state to active.
        if transaction.is_inactive() {
            transaction.set_active_flag(true);
        }
        // Step 7: Dispatch event at request with legacyOutputDidListenersThrowFlag.
        let default_not_prevented = event
            .upcast::<Event>()
            .fire_with_legacy_output_did_listeners_throw(cx, self.upcast(), &did_listeners_throw);
        // Step 8: If transaction’s state is active, then:
        if transaction.is_active() {
            // Step 8.1: Set transaction’s state to inactive.
            transaction.set_active_flag(false);
            // Step 8.2: If legacyOutputDidListenersThrowFlag is true, then run abort a transaction
            // with transaction and a newly created "AbortError" DOMException and terminate these steps.
            // NOTE: This is done even if event’s canceled flag is false.
            // NOTE: This means that if an error event is fired and any of the event handlers throw an
            // exception, transaction’s error property is set to an AbortError rather than request’s
            // error, even if preventDefault() is never called.
            if did_listeners_throw.get() {
                transaction.abort(cx, Error::Abort(None));
                return;
            }
            // Step 8.3: If event’s canceled flag is false, then run abort a transaction
            // using transaction and request’s error, and terminate these steps.
            if default_not_prevented {
                transaction.abort(cx, error);
                return;
            }
        }
        // Step 8.4: If transaction’s request list is empty, then run commit a transaction with
        // transaction.
        transaction.maybe_commit(cx);
    }

    /// <https://w3c.github.io/IndexedDB/#abort-a-transaction>
    /// Step 6, the queued task for a request of the aborted transaction.
    pub(crate) fn finish_with_abort_error(&self, cx: &mut JSContext) {
        // Step 6.1: Set request’s done flag to true.
        self.set_ready_state_done();

        // Step 6.2: Set request’s result to undefined.
        rooted!(&in(cx) let undefined = UndefinedValue());
        self.set_result(undefined.handle());

        // Step 6.3: Set request’s error to a newly created "AbortError" DOMException.
        self.set_error(
            cx,
            Some(Error::Abort(Some(
                "The transaction of the request was aborted".to_owned(),
            ))),
        );

        // Step 6.4: Fire an event named error at request with its bubbles and cancelable
        // attributes initialized to true.
        let global = self.global();
        let event = Event::new(
            cx,
            &global,
            Atom::from("error"),
            EventBubbles::Bubbles,
            EventCancelable::Cancelable,
        );
        event.fire(cx, self.upcast());
    }
}

impl IDBRequestMethods<crate::DomTypeHolder> for IDBRequest {
    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-result>
    fn GetResult(
        &self,
        _cx: &mut JSContext,
        mut val: js::rust::MutableHandle<'_, js::jsapi::Value>,
    ) -> Fallible<()> {
        // Step 1. If this's done flag is false, then throw an "InvalidStateError" DOMException.
        if !self.is_done() {
            return Err(Error::InvalidState(Some(
                "Cannot get result on a request that is still pending.".into(),
            )));
        }

        // Step 2. Return this's result, or undefined if the request resulted in an error.
        val.set(self.result.get());
        Ok(())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-error>
    fn GetError(&self) -> Fallible<Option<DomRoot<DOMException>>> {
        // Step 1. If this's done flag is false, then throw an "InvalidStateError" DOMException.
        if !self.is_done() {
            return Err(Error::InvalidState(Some(
                "Cannot get error on a request that is still pending.".into(),
            )));
        }

        // Step 2. Return this's error, or null if no error occurred.
        Ok(self.error.get())
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-source>
    fn GetSource(&self) -> Option<DomRoot<IDBObjectStore>> {
        self.source.get()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-transaction>
    fn GetTransaction(&self) -> Option<DomRoot<IDBTransaction>> {
        self.transaction.get()
    }

    /// <https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-readystate>
    fn ReadyState(&self) -> IDBRequestReadyState {
        self.ready_state.get()
    }

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-onsuccess
    event_handler!(success, GetOnsuccess, SetOnsuccess);

    // https://www.w3.org/TR/IndexedDB-3/#dom-idbrequest-onerror
    event_handler!(error, GetOnerror, SetOnerror);
}
