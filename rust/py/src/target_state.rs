use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::sync::{LazyLock, Mutex};

use cocoindex_core::engine::target_state::{
    ChildHandling, ChildInvalidation, ChildTargetSlot, TargetActionSink, TargetActionSinkKeeper,
    TargetActionWithChildSlot, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    TargetStateProviderRegistry, WeakTargetActionSinkKeeper,
};
use futures::FutureExt;
use pyo3::exceptions::PyDeprecationWarning;
use pyo3::types::{PyDict, PyList, PySequence, PyTuple};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::memo_fingerprint::plain_fingerprint;
use crate::prelude::*;

use crate::stable_path::PyStableKey;

use crate::runtime::{PyAsyncContext, PyCallback, python_objects};
use crate::target_state_codec::{DesiredForReconcile, PyTargetAction, PyTargetStateValue};
use crate::value::PyStoredValue;

#[pyclass(name = "TargetActionSink", from_py_object)]
#[derive(Clone)]
pub struct PyTargetActionSink {
    keeper: TargetActionSinkKeeper<PyEngineProfile>,
}

pub struct PyTargetActionSinkInner {
    callback: PyCallback,
    convention: SinkConvention,
}

/// How a sink's callback is called, and where child handlers come from.
enum SinkConvention {
    /// `from_fn` / `from_async_fn`: `callback(context_provider, actions)`.
    /// Child-bearing actions get slots, for the deprecated callback that
    /// returns `ChildTargetDef`s.
    Leaf,
    /// Deprecated `from_fn_with_children` / `from_async_fn_with_children`:
    /// `callback(context_provider, actions, child_slots)`.
    LegacySlots,
    /// `from_fn` / `from_async_fn` with `child_handler=`: the sync factory
    /// `child_handler(context_provider, action) -> handler` builds the child
    /// handlers; the callback is called as for `Leaf` and gets no slots.
    Factory(Py<PyAny>),
}

/// Interns sink keepers by callback object and calling convention, so that
/// sinks constructed from the same callback share one batching identity (the
/// keeper, whose pointer the engine groups actions by). The Python layer
/// canonicalizes callbacks that compare equal to one representative object
/// (`_ObjectDeduper` in `_internal/target_state.py`) before calling
/// `new_sync`/`new_async`, so together the two layers give sinks value-based
/// identity. The convention is part of the key because the keeper fixes how
/// the callback is invoked.
///
/// Entries hold the keeper only weakly, so an idle sink (no pending actions,
/// no live Python reference) is freed together with its batcher. A live entry
/// implies no pointer-reuse hazard: the keeper holds its callback strongly,
/// so while the weak reference upgrades, the keyed address is still that same
/// callback. Dead entries never match (upgrade fails) and are swept lazily
/// once the map grows past a doubling threshold.
struct SinkKeeperRegistry {
    map: HashMap<SinkKeeperKey, WeakTargetActionSinkKeeper<PyEngineProfile>>,
    prune_at: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SinkKeeperKey {
    callback_ptr: usize,
    convention: SinkConventionKey,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum SinkConventionKey {
    Leaf,
    LegacySlots,
    /// The canonical `child_handler` factory's address.
    Factory(usize),
}

impl SinkConvention {
    fn new(with_children: bool, child_handler: Option<Py<PyAny>>) -> PyResult<Self> {
        match (with_children, child_handler) {
            (false, None) => Ok(Self::Leaf),
            (true, None) => Ok(Self::LegacySlots),
            (false, Some(child_handler)) => Ok(Self::Factory(child_handler)),
            (true, Some(_)) => Err(pyo3::exceptions::PyValueError::new_err(
                "a sink built with child slots takes no `child_handler`",
            )),
        }
    }

    fn key(&self) -> SinkConventionKey {
        match self {
            Self::Leaf => SinkConventionKey::Leaf,
            Self::LegacySlots => SinkConventionKey::LegacySlots,
            Self::Factory(child_handler) => {
                SinkConventionKey::Factory(child_handler.as_ptr() as usize)
            }
        }
    }
}

const SINK_REGISTRY_MIN_PRUNE_AT: usize = 64;

impl SinkKeeperRegistry {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            prune_at: SINK_REGISTRY_MIN_PRUNE_AT,
        }
    }

    fn get_or_create(
        &mut self,
        key: SinkKeeperKey,
        make: impl FnOnce() -> PyTargetActionSinkInner,
    ) -> TargetActionSinkKeeper<PyEngineProfile> {
        if let Some(weak) = self.map.get(&key) {
            if let Some(keeper) = weak.upgrade() {
                return keeper;
            }
        }
        let keeper = TargetActionSinkKeeper::new(make());
        if self.map.len() >= self.prune_at {
            self.map.retain(|_, weak| weak.upgrade().is_some());
            self.prune_at = (self.map.len() * 2).max(SINK_REGISTRY_MIN_PRUNE_AT);
        }
        self.map.insert(key, keeper.downgrade());
        keeper
    }
}

// Sync and async callbacks are interned separately: the same callback object
// wrapped as sync vs async must not share a keeper, since the keeper fixes how
// the callback is invoked.
static SYNC_SINK_REGISTRY: LazyLock<Mutex<SinkKeeperRegistry>> =
    LazyLock::new(|| Mutex::new(SinkKeeperRegistry::new()));
static ASYNC_SINK_REGISTRY: LazyLock<Mutex<SinkKeeperRegistry>> =
    LazyLock::new(|| Mutex::new(SinkKeeperRegistry::new()));

#[pymethods]
impl PyTargetActionSink {
    #[staticmethod]
    #[pyo3(signature = (callback, with_children, child_handler=None))]
    pub fn new_sync(
        callback: Py<PyAny>,
        with_children: bool,
        child_handler: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let convention = SinkConvention::new(with_children, child_handler)?;
        let key = SinkKeeperKey {
            callback_ptr: callback.as_ptr() as usize,
            convention: convention.key(),
        };
        let keeper =
            SYNC_SINK_REGISTRY
                .lock()
                .unwrap()
                .get_or_create(key, || PyTargetActionSinkInner {
                    callback: PyCallback::Sync(Arc::new(callback)),
                    convention,
                });
        Ok(Self { keeper })
    }

    #[staticmethod]
    #[pyo3(signature = (callback, with_children, child_handler=None))]
    pub fn new_async(
        callback: Py<PyAny>,
        with_children: bool,
        child_handler: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let convention = SinkConvention::new(with_children, child_handler)?;
        let key = SinkKeeperKey {
            callback_ptr: callback.as_ptr() as usize,
            convention: convention.key(),
        };
        let keeper =
            ASYNC_SINK_REGISTRY
                .lock()
                .unwrap()
                .get_or_create(key, || PyTargetActionSinkInner {
                    callback: PyCallback::Async(Arc::new(callback)),
                    convention,
                });
        Ok(Self { keeper })
    }

    /// Two sinks are equal iff they share one batching identity: actions
    /// reconciled to them are batched and applied together.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<Self>() {
            Ok(other) => self.keeper == other.borrow().keeper,
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.keeper.hash(&mut hasher);
        hasher.finish()
    }
}

fn get_core_field(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let core_obj = obj.getattr(py, "_core")?;
    let core_py = core_obj.extract::<Py<PyAny>>(py)?;
    Ok(core_py)
}

#[async_trait]
impl TargetActionSink<PyEngineProfile> for PyTargetActionSinkInner {
    fn child_handling(&self) -> ChildHandling {
        match self.convention {
            // A leaf sink's callback may still return the deprecated
            // `ChildTargetDef` list, which fulfills slots.
            SinkConvention::Leaf | SinkConvention::LegacySlots => ChildHandling::LegacySlots,
            SinkConvention::Factory(_) => ChildHandling::Factory,
        }
    }

    async fn child_handlers(
        &self,
        _host_runtime_ctx: &PyAsyncContext,
        host_ctx: Arc<Py<PyAny>>,
        actions: &[&PyTargetAction],
    ) -> Result<Vec<PyTargetHandler>> {
        let SinkConvention::Factory(child_handler) = &self.convention else {
            internal_bail!("target action sink has no child handler factory");
        };
        Python::attach(|py| {
            let context_provider = host_ctx.as_ref().bind(py);
            let wrap = &python_objects().typed_target_handler_wrapper_fn;
            actions
                .iter()
                .map(|action| {
                    let action = action.to_object(py).from_py_result()?;
                    let handler = child_handler
                        .call1(py, (context_provider, action))
                        .from_py_result()?;
                    let handler = wrap.call1(py, (handler,)).from_py_result()?;
                    PyTargetHandler::new(handler.into_bound(py)).from_py_result()
                })
                .collect()
        })
    }

    async fn apply(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        host_ctx: Arc<Py<PyAny>>,
        actions: &[TargetActionWithChildSlot<PyEngineProfile>],
    ) -> Result<()> {
        let actions_len = actions.len();
        let (call, legacy_child_slots) = Python::attach(|py| -> Result<_> {
            let context_provider = host_ctx.as_ref().clone_ref(py);
            // Split the child slots off while building the action list, so a
            // leaf-only batch costs nothing beyond the list itself. The engine
            // keeps the actions (to retry subsets of a failed batch), hence
            // the borrow; an encoded action is decoded anew for each call.
            let mut child_slots = Vec::new();
            let mut objects = Vec::with_capacity(actions.len());
            for (idx, (action, slot)) in actions.iter().enumerate() {
                if let Some(slot) = slot {
                    child_slots.push((idx, slot.clone()));
                }
                objects.push(action.to_object(py).from_py_result()?);
            }
            let actions = PyList::new(py, objects).from_py_result()?;
            if matches!(self.convention, SinkConvention::LegacySlots) {
                let slots = PyDict::new(py);
                let wrap = &python_objects().child_slot_wrapper_fn;
                for (idx, slot) in child_slots {
                    let slot = wrap
                        .call1(py, (PyChildTargetSlot(slot),))
                        .from_py_result()?;
                    slots.set_item(idx, slot).from_py_result()?;
                }
                let call = self
                    .callback
                    .call(
                        host_runtime_ctx,
                        (context_provider, actions.unbind(), slots.unbind()),
                    )?
                    .boxed();
                Ok((call, None))
            } else {
                // A `from_fn` / `from_async_fn` sink gets no slots. If it
                // returns the deprecated index-aligned child-handler list, the
                // slots are fulfilled from it after the call.
                let call = self
                    .callback
                    .call(host_runtime_ctx, (context_provider, actions.unbind()))?
                    .boxed();
                Ok((call, Some(child_slots)))
            }
        })?;
        let ret = call.await?;
        if let Some(child_slots) = legacy_child_slots {
            Python::attach(|py| {
                self.fulfill_from_legacy_child_defs(py, ret.bind(py), actions_len, child_slots)
            })?;
        }
        Ok(())
    }
}

impl PyTargetActionSinkInner {
    /// Deprecated pre-slot contract: a `from_fn` / `from_async_fn` callback
    /// returned a sequence of `ChildTargetDef | None` index-aligned with the
    /// actions. Fulfill the engine's slots from it (after a deprecation
    /// warning) so such sinks keep working until the contract is removed.
    fn fulfill_from_legacy_child_defs(
        &self,
        py: Python<'_>,
        ret: &Bound<'_, PyAny>,
        actions_len: usize,
        child_slots: Vec<(usize, ChildTargetSlot<PyEngineProfile>)>,
    ) -> Result<()> {
        if let SinkConvention::Factory(_) = self.convention {
            if !ret.is_none() {
                client_bail!(
                    "target action sink built with `child_handler=` returned a value from \
                     {}; it must return `None`, as `child_handler` builds the child handlers",
                    callback_label(self.callback.object().bind(py))
                );
            }
            return Ok(());
        }
        if ret.is_none() {
            if child_slots.is_empty() {
                return Ok(());
            }
            client_bail!(
                "target action sink built with `TargetActionSink.from_fn()` / \
                 `from_async_fn()` received {} action(s) with child target states; \
                 pass it a `child_handler=` factory that builds their handlers",
                child_slots.len()
            );
        }
        (|| -> PyResult<()> {
            let message = CString::new(format!(
                "{} returned child handler definitions from a target action sink \
                 callback; that contract is deprecated and will be removed. Build the \
                 sink with `TargetActionSink.from_fn()` / `from_async_fn()` and pass a \
                 `child_handler=` factory that builds the child handlers instead (see \
                 \"Migrating from child slots and ChildTargetDef\" in the custom target \
                 connector docs).",
                callback_label(self.callback.object().bind(py))
            ))?;
            let category = py.get_type::<PyDeprecationWarning>();
            PyErr::warn(py, category.as_any(), &message, 1)
        })()
        .from_py_result()?;
        let defs = ret
            .cast::<PySequence>()
            .map_err(PyErr::from)
            .from_py_result()?;
        let len = defs.len().from_py_result()?;
        if len != actions_len {
            client_bail!(
                "target action sink returned {} child handler definitions for {} actions",
                len,
                actions_len
            );
        }
        let wrap = &python_objects().child_slot_wrapper_fn;
        for (idx, slot) in child_slots {
            let def = defs.get_item(idx).from_py_result()?;
            if def.is_none() {
                client_bail!(
                    "target action sink returned no child handler for the action at index {} \
                     whose target state declared a child",
                    idx
                );
            }
            let handler = def.getattr("handler").from_py_result()?;
            // Route through the Python `ChildSlot` so the handler gets its
            // typed-deserialization wrapper, as a slot-aware sink's would.
            wrap.call1(py, (PyChildTargetSlot(slot),))
                .from_py_result()?
                .call_method1(py, "fulfill", (handler,))
                .from_py_result()?;
        }
        Ok(())
    }
}

/// `module.qualname` of a sink callback for messages: a function or bound
/// method carries its own name, a callable object falls back to its type's.
fn callback_label(callback: &Bound<'_, PyAny>) -> String {
    fn attr(obj: &Bound<'_, PyAny>, name: &str) -> Option<String> {
        obj.getattr(name).ok()?.extract::<String>().ok()
    }
    let named = |obj: &Bound<'_, PyAny>| {
        Some(format!(
            "{}.{}",
            attr(obj, "__module__")?,
            attr(obj, "__qualname__")?
        ))
    };
    named(callback)
        .or_else(|| named(callback.get_type().as_any()))
        .unwrap_or_else(|| {
            callback
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_else(|_| "<callback>".to_string())
        })
}

/// Core child slot handed to a container sink. The Python-facing `ChildSlot`
/// wraps it (see `child_slot_wrapper_fn`), adding the typed-deserialization
/// wrapper around the handler before it reaches `fulfill`.
#[pyclass(name = "ChildTargetSlot")]
pub struct PyChildTargetSlot(ChildTargetSlot<PyEngineProfile>);

#[pymethods]
impl PyChildTargetSlot {
    fn fulfill(&self, py: Python<'_>, handler: Py<PyAny>) -> PyResult<()> {
        self.0
            .fulfill(PyTargetHandler::new(handler.into_bound(py))?)
            .into_py_result()
    }
}

#[pyclass(name = "TargetHandler")]
pub struct PyTargetHandler {
    handler: Py<PyAny>,
    /// The handler's `tracks_value_fingerprint` opt-in: its tracking record
    /// for a declared value is `fingerprint_object(value)`.
    tracks_value_fingerprint: bool,
}

impl PyTargetHandler {
    fn new(handler: Bound<'_, PyAny>) -> PyResult<Self> {
        let tracks_value_fingerprint = handler
            .getattr(pyo3::intern!(handler.py(), "tracks_value_fingerprint"))?
            .extract()?;
        Ok(Self {
            handler: handler.unbind(),
            tracks_value_fingerprint,
        })
    }
}

/// `serde.serialize(fingerprint_object(value))` for plain data: the msgspec
/// routing byte, then the 16-byte digest as a msgpack `bin 8`.
const FINGERPRINT_RECORD_HEADER: [u8; 3] = [0x01, 0xc4, 16];

impl TargetHandler<PyEngineProfile> for PyTargetHandler {
    fn reconcile(
        &self,
        key: cocoindex_core::state::stable_path::StableKey,
        desired_target_state: Option<&PyTargetStateValue>,
        prev_possible_records: &[PyStoredValue],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<PyEngineProfile>>> {
        Python::attach(|py| -> PyResult<_> {
            let prev_possible_records = PyList::new(
                py,
                prev_possible_records
                    .iter()
                    .map(|s| Py::new(py, s.clone()).unwrap()),
            )?;
            // A value held encoded is decoded for this call only: the action
            // returned is held encoded too (see `DesiredForReconcile::hold_action`).
            let desired = match desired_target_state {
                Some(value) => DesiredForReconcile::new(py, value)?,
                None => DesiredForReconcile::non_existence(py),
            };
            let py_output = self.handler.call_method(
                py,
                "reconcile",
                (
                    PyStableKey(key),
                    desired.object.bind(py),
                    prev_possible_records,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let output = if py_output.is_none(py) {
                None
            } else {
                let (action, sink, state, py_child_invalidation) =
                    py_output.extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)>(py)?;
                let child_invalidation = if py_child_invalidation.is_none(py) {
                    None
                } else {
                    let s = py_child_invalidation.extract::<String>(py)?;
                    match s.as_str() {
                        "destructive" => Some(ChildInvalidation::Destructive),
                        "lossy" => Some(ChildInvalidation::Lossy),
                        other => {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Invalid child_invalidation value: {other:?}"
                            )));
                        }
                    }
                };
                let sink = get_core_field(py, sink)?
                    .extract::<PyTargetActionSink>(py)?
                    .keeper;
                let tracking_record = if python_objects().non_existence.is(&state) {
                    None
                } else {
                    Some(PyStoredValue::new(state))
                };
                Some(TargetReconcileOutput {
                    action: desired.hold_action(py, action)?,
                    sink,
                    tracking_record,
                    child_invalidation,
                })
            };
            Ok(output)
        })
        .from_py_result()
    }

    fn value_fingerprint_record(
        &self,
        desired_target_state: &PyTargetStateValue,
    ) -> Result<Option<bytes::Bytes>> {
        if !self.tracks_value_fingerprint {
            return Ok(None);
        }
        Python::attach(|py| -> PyResult<_> {
            // A value held encoded is fingerprinted as its decoding: an exact
            // copy, so it has the declared object's fingerprint.
            let value = desired_target_state.to_object(py)?;
            Ok(plain_fingerprint(value.bind(py))?.map(|fp| {
                let mut record = Vec::with_capacity(FINGERPRINT_RECORD_HEADER.len() + 16);
                record.extend_from_slice(&FINGERPRINT_RECORD_HEADER);
                record.extend_from_slice(fp.as_slice());
                record.into()
            }))
        })
        .from_py_result()
    }

    fn attachments(&self) -> Result<Vec<(Arc<str>, PyTargetHandler)>> {
        Python::attach(|py| -> PyResult<_> {
            let obj = self.handler.bind(py);
            if !obj.hasattr("attachments")? {
                return Ok(vec![]);
            }
            let result = obj.call_method0("attachments")?;
            let dict = result.cast::<pyo3::types::PyDict>()?;
            let mut entries = Vec::with_capacity(dict.len());
            for (key, value) in dict.iter() {
                let att_type: String = key.extract()?;
                entries.push((Arc::from(att_type), PyTargetHandler::new(value)?));
            }
            Ok(entries)
        })
        .from_py_result()
    }
}

#[pyclass(name = "TargetStateProvider")]
pub struct PyTargetStateProvider(TargetStateProvider<PyEngineProfile>);

#[pymethods]
impl PyTargetStateProvider {
    pub fn coco_memo_key(&self) -> String {
        let path = self.0.target_state_path().to_string();
        match self.0.provider_generation() {
            Some(g) => format!("{}[{},{}]", path, g.provider_id, g.provider_schema_version),
            None => path,
        }
    }

    pub fn stable_key_chain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let chain = self.0.stable_key_chain();
        let py_keys: Vec<Bound<'py, PyAny>> = chain
            .into_iter()
            .map(|k| PyStableKey(k).into_pyobject(py))
            .collect::<Result<_, _>>()?;
        PyTuple::new(py, py_keys)
    }

    pub fn register_attachment_provider(
        &self,
        comp_ctx: &PyComponentProcessorContext,
        att_type: &str,
    ) -> PyResult<PyTargetStateProvider> {
        let provider = self
            .0
            .register_attachment_provider(&comp_ctx.0, att_type)
            .into_py_result()?;
        Ok(PyTargetStateProvider(provider))
    }
}

#[pyfunction]
pub fn declare_target_state<'py>(
    py: Python<'py>,
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<()> {
    cocoindex_core::engine::execution::declare_target_state(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        PyTargetStateValue::new(py, value)?,
    )
    .into_py_result()?;
    Ok(())
}

#[pyfunction]
pub fn declare_target_state_with_child<'py>(
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    // A container's value (its spec) is held as is: there are few of them,
    // and their actions go on to fulfill child slots.
    let output = cocoindex_core::engine::execution::declare_target_state_with_child(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        PyTargetStateValue::Object(value),
    )
    .into_py_result()?;
    Ok(PyTargetStateProvider(output))
}

static ROOT_TARGET_STATE_PROVIDER_REGISTRY: LazyLock<
    ManuallyDrop<Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>>>,
> = LazyLock::new(|| ManuallyDrop::new(Default::default()));

pub fn root_target_states_provider_registry()
-> &'static Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>> {
    &**ROOT_TARGET_STATE_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_target_states_provider(
    name: String,
    handler: Bound<'_, PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let provider = root_target_states_provider_registry()
        .lock()
        .unwrap()
        .register_root(name, PyTargetHandler::new(handler)?)
        .into_py_result()?;
    Ok(PyTargetStateProvider(provider))
}
