"""
Type stubs for the cocoindex._internal.core Rust extension module (PyO3).
"""

from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generic,
    TypeVar,
)
import asyncio

from cocoindex._internal.typing import Fingerprintable as Fingerprintable
from cocoindex._internal.typing import StableKey as StableKey

########################################################
# Core
########################################################

# --- Symbol ---
class Symbol:
    def __new__(cls, name: str) -> Symbol: ...
    @property
    def name(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

__version__: str

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
R_co = TypeVar("R_co", covariant=True)

# --- StablePath ---
class StablePath:
    def __new__(cls) -> StablePath: ...
    def concat(self, part: StableKey) -> StablePath: ...
    def to_string(self) -> str: ...
    def parts(self) -> list[StableKey]: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __coco_memo_key__(self) -> str: ...

# --- Fingerprint ---
class Fingerprint:
    def as_bytes(self) -> bytes: ...
    def to_base64(self) -> str: ...
    def __bytes__(self) -> bytes: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

# --- ComponentProcessorInfo ---
class ComponentProcessorInfo:
    def __new__(cls, name: str) -> ComponentProcessorInfo: ...
    @property
    def name(self) -> str: ...

# --- ComponentProcessor ---
class ComponentProcessor(Generic[T_co]):
    @staticmethod
    def new_sync(
        processor_fn: Callable[[ComponentProcessorContext], T_co],
        processor_info: ComponentProcessorInfo,
        memo_key_fingerprint: Fingerprint | None = None,
        state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None,
    ) -> ComponentProcessor[T_co]: ...
    @staticmethod
    def new_async(
        processor_fn: Callable[[ComponentProcessorContext], Coroutine[Any, Any, T_co]],
        processor_info: ComponentProcessorInfo,
        memo_key_fingerprint: Fingerprint | None = None,
        state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None,
    ) -> ComponentProcessor[T_co]: ...

# --- ComponentProcessorContext ---
class ComponentProcessorContext:
    @property
    def environment(self) -> "Environment": ...
    @property
    def stable_path(self) -> StablePath: ...
    def join_fn_call(self, child_fn_ctx: FnCallContext) -> None: ...
    async def next_id(self, key: StableKey | None = None) -> int: ...

# --- FnCallContext ---
class FnCallContext:
    def __new__(cls) -> FnCallContext: ...
    def join_child(self, child_fn_ctx: FnCallContext) -> None: ...
    def join_child_memo(self, memo_fp: Fingerprint) -> None: ...
    def add_logic_dep(self, fp: Fingerprint) -> None: ...

# --- FnCallMemoGuard ---
class FnCallMemoGuard:
    @property
    def is_cached(self) -> bool: ...
    @property
    def cached_value(self) -> Any | None: ...
    @property
    def cached_memo_states(self) -> list[Any] | None: ...
    def update_memo_states(self, memo_states: list[Any]) -> None: ...
    def resolve(
        self, fn_ctx: FnCallContext, ret: Any, memo_states: list[Any] | None = None
    ) -> bool: ...
    def close(self) -> None: ...

# --- ComponentMountHandle ---
class ComponentMountHandle:
    def wait_until_ready(self) -> None: ...
    async def ready_async(self) -> None: ...

# --- ComponentMountRunHandle ---
class ComponentMountRunHandle(Generic[T_co]):
    def result(self, comp_ctx: ComponentProcessorContext) -> T_co: ...
    async def result_async(self, comp_ctx: ComponentProcessorContext) -> T_co: ...

# --- AsyncContext ---
class AsyncContext:
    def __new__(cls, event_loop: asyncio.AbstractEventLoop) -> "AsyncContext": ...

# --- Environment ---
class Environment:
    def __new__(cls, settings: Any, async_context: AsyncContext) -> "Environment": ...

# --- Inspect helpers ---
def list_app_names(env: Environment) -> list[str]: ...
def iter_stable_paths(app: App) -> StablePathInfoAsyncIterator: ...

class StablePathNodeType:
    @staticmethod
    def directory() -> StablePathNodeType: ...
    @staticmethod
    def component() -> StablePathNodeType: ...

class StablePathInfo:
    path: StablePath
    node_type: StablePathNodeType

class StablePathInfoAsyncIterator:
    """Async iterator of StablePathInfo; use with async for."""

    def __aiter__(self) -> StablePathInfoAsyncIterator: ...
    def __anext__(self) -> Awaitable[StablePathInfo]: ...

# --- App ---
class App:
    def __new__(
        cls, name: str, env: Environment, max_inflight_components: int | None = None
    ) -> App: ...
    def update(
        self,
        root_processor: ComponentProcessor[T_co],
        report_to_stdout: bool,
        full_reprocess: bool,
    ) -> T_co: ...
    async def update_async(
        self,
        root_processor: ComponentProcessor[T_co],
        report_to_stdout: bool,
        full_reprocess: bool,
    ) -> T_co: ...
    def drop(self, report_to_stdout: bool) -> None: ...
    def drop_async(self, report_to_stdout: bool) -> Coroutine[Any, Any, None]: ...

# --- TargetActionSink ---
class TargetActionSink:
    @staticmethod
    def new_sync(callback: Callable[..., Any]) -> TargetActionSink: ...
    @staticmethod
    def new_async(
        callback: Callable[..., Coroutine[Any, Any, Any]],
    ) -> TargetActionSink: ...

# --- TargetHandler (marker class, used for typing) ---
class TargetHandler: ...

# --- TargetStateProvider ---
class TargetStateProvider:
    def coco_memo_key(self) -> str: ...
    def stable_key_chain(self) -> tuple[StableKey, ...]: ...

# --- Module-level functions ---

def init_runtime(
    *,
    serialize_fn: Callable[[Any], bytes],
    deserialize_fn: Callable[[bytes], Any],
    non_existence: Any,
    not_set: Any,
) -> None: ...
def mount(
    processor: ComponentProcessor[T_co],
    stable_path: StablePath,
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
) -> ComponentMountHandle: ...
def mount_run(
    processor: ComponentProcessor[T_co],
    stable_path: StablePath,
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
) -> ComponentMountRunHandle[T_co]: ...
async def mount_async(
    processor: ComponentProcessor[T_co],
    stable_path: StablePath,
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
) -> ComponentMountHandle: ...
async def mount_run_async(
    processor: ComponentProcessor[T_co],
    stable_path: StablePath,
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
) -> ComponentMountRunHandle[T_co]: ...
def declare_target_state(
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
    provider: TargetStateProvider,
    key: Any,
    value: Any,
) -> None: ...
def declare_target_state_with_child(
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
    provider: TargetStateProvider,
    key: Any,
    value: Any,
) -> TargetStateProvider: ...
def register_root_target_states_provider(
    name: str, handler: Any
) -> TargetStateProvider: ...
def fingerprint_simple_object(obj: Fingerprintable) -> Fingerprint: ...
def fingerprint_bytes(data: bytes) -> Fingerprint: ...
def fingerprint_str(s: str) -> Fingerprint: ...
def register_logic_fingerprint(fp: Fingerprint) -> None: ...
def unregister_logic_fingerprint(fp: Fingerprint) -> None: ...
def reserve_memoization(
    comp_ctx: ComponentProcessorContext,
    memo_fp: Fingerprint,
) -> FnCallMemoGuard: ...
async def reserve_memoization_async(
    comp_ctx: ComponentProcessorContext,
    memo_fp: Fingerprint,
) -> FnCallMemoGuard: ...

########################################################
# Inspect
########################################################

def list_stable_paths(app: App) -> list[StablePath]: ...

########################################################
# Ops (Text Processing Operations)
########################################################

# --- Chunk (from ops) ---
class Chunk:
    @property
    def text(self) -> str: ...
    @property
    def start_byte(self) -> int: ...
    @property
    def end_byte(self) -> int: ...
    @property
    def start_char_offset(self) -> int: ...
    @property
    def start_line(self) -> int: ...
    @property
    def start_column(self) -> int: ...
    @property
    def end_char_offset(self) -> int: ...
    @property
    def end_line(self) -> int: ...
    @property
    def end_column(self) -> int: ...

# --- SeparatorSplitter (from ops) ---
class SeparatorSplitter:
    def __new__(
        cls,
        separators_regex: list[str],
        keep_separator: str | None = None,
        include_empty: bool = False,
        trim: bool = True,
    ) -> "SeparatorSplitter": ...
    def split(self, text: str) -> list[Chunk]: ...

# --- CustomLanguageConfig (from ops) ---
class CustomLanguageConfig:
    language_name: str
    aliases: list[str]
    separators_regex: list[str]

    def __new__(
        cls,
        language_name: str,
        separators_regex: list[str],
        aliases: list[str] | None = None,
    ) -> "CustomLanguageConfig": ...

# --- RecursiveSplitter (from ops) ---
class RecursiveSplitter:
    def __new__(
        cls,
        *,
        custom_languages: list[CustomLanguageConfig] | None = None,
    ) -> "RecursiveSplitter": ...
    def split(
        self,
        text: str,
        chunk_size: int,
        min_chunk_size: int | None = None,
        chunk_overlap: int | None = None,
        language: str | None = None,
    ) -> list[Chunk]: ...

def detect_code_language(*, filename: str) -> str | None: ...

# --- PatternMatcher (from ops) ---
class PatternMatcher:
    def __new__(
        cls,
        included_patterns: list[str] | None = None,
        excluded_patterns: list[str] | None = None,
    ) -> "PatternMatcher": ...
    def is_dir_included(self, path: str) -> bool: ...
    def is_file_included(self, path: str) -> bool: ...

########################################################
# Synchronization Primitives
########################################################

# --- RWLock (fair read-write lock) ---
class RWLock:
    def __new__(cls) -> "RWLock": ...
    def read(self) -> "RWLockReadGuard": ...
    def write(self) -> "RWLockWriteGuard": ...

class RWLockReadGuard:
    def release(self) -> None: ...
    def __enter__(self) -> "RWLockReadGuard": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None: ...
    def __aenter__(self) -> Coroutine[Any, Any, "RWLockReadGuard"]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> Coroutine[Any, Any, None]: ...

class RWLockWriteGuard:
    def release(self) -> None: ...
    def __enter__(self) -> "RWLockWriteGuard": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None: ...
    def __aenter__(self) -> Coroutine[Any, Any, "RWLockWriteGuard"]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> Coroutine[Any, Any, None]: ...

########################################################
# Batching Infrastructure
########################################################

# --- BatchingOptions ---
class BatchingOptions:
    """Options for batching behavior."""

    max_batch_size: int | None

    def __new__(cls, max_batch_size: int | None = None) -> "BatchingOptions": ...

# --- BatchQueue ---
class BatchQueue:
    """A shared queue that processes batches in FIFO order.

    Multiple batchers can share the same queue. Each batcher provides its own
    runner function, and batches are processed using the runner from the batcher
    that created them.
    """

    def __new__(cls) -> "BatchQueue": ...

# --- Batcher ---
class Batcher(Generic[T, R_co]):
    """A batcher that collects inputs and submits them to a shared queue.

    Each batcher maintains at most one non-full, non-sealed batch in the queue.
    When inputs are submitted, they are added to the current batch or a new batch is created.

    Multiple batchers can share the same queue with different runner functions.
    Each batch uses the runner function from the batcher that created it.
    """

    @staticmethod
    def new_sync(
        queue: BatchQueue,
        options: BatchingOptions,
        runner_fn: Callable[[list[T]], list[R_co]],
        async_ctx: AsyncContext,
    ) -> "Batcher[T, R_co]": ...
    @staticmethod
    def new_async(
        queue: BatchQueue,
        options: BatchingOptions,
        runner_fn: Callable[[list[T]], Coroutine[Any, Any, list[R_co]]],
        async_ctx: AsyncContext,
    ) -> "Batcher[T, R_co]": ...
    def run(self, input: T) -> Coroutine[Any, Any, R_co]: ...
