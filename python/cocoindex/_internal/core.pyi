"""
Type stubs for the cocoindex._internal.core Rust extension module (PyO3).
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Generic, TypeVar
import asyncio
import uuid

########################################################
# Core
########################################################

__version__: str

# --- StableKey type alias (accepted by StablePath.concat) ---
StableKey = None | bool | int | str | bytes | uuid.UUID | tuple["StableKey", ...]

T_co = TypeVar("T_co", covariant=True)

# --- StablePath ---
class StablePath:
    def __new__(cls) -> StablePath: ...
    def concat(self, part: StableKey) -> StablePath: ...
    def to_string(self) -> str: ...
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
    ) -> ComponentProcessor[T_co]: ...
    @staticmethod
    def new_async(
        processor_fn: Callable[[ComponentProcessorContext], Coroutine[Any, Any, T_co]],
        processor_info: ComponentProcessorInfo,
        memo_key_fingerprint: Fingerprint | None = None,
    ) -> ComponentProcessor[T_co]: ...

# --- ComponentProcessorContext ---
class ComponentProcessorContext:
    @property
    def environment(self) -> "Environment": ...
    @property
    def stable_path(self) -> StablePath: ...
    def join_fn_call(self, child_fn_ctx: FnCallContext) -> None: ...

# --- FnCallContext ---
class FnCallContext:
    def __new__(cls) -> FnCallContext: ...
    def join_child(self, child_fn_ctx: FnCallContext) -> None: ...
    def join_child_memo(self, memo_fp: Fingerprint) -> None: ...

# --- PendingFnCallMemo ---
class PendingFnCallMemo:
    def resolve(self, fn_ctx: FnCallContext, ret: Any) -> bool: ...
    def close(self) -> None: ...

# --- ComponentMountHandle ---
class ComponentMountHandle:
    def ready(self) -> None: ...
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

# --- App ---
class App:
    def __new__(cls, name: str, env: Environment) -> App: ...
    def run(
        self, root_processor: ComponentProcessor[T_co], report_to_stdout: bool
    ) -> T_co: ...
    async def run_async(
        self, root_processor: ComponentProcessor[T_co], report_to_stdout: bool
    ) -> T_co: ...

# --- EffectSink ---
class EffectSink:
    @staticmethod
    def new_sync(callback: Callable[..., Any]) -> EffectSink: ...
    @staticmethod
    def new_async(
        callback: Callable[..., Coroutine[Any, Any, Any]],
    ) -> EffectSink: ...

# --- EffectHandler (marker class, used for typing) ---
class EffectHandler: ...

# --- EffectProvider ---
class EffectProvider:
    def coco_memo_key(self) -> str: ...

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
def declare_effect(
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
    provider: EffectProvider,
    key: Any,
    value: Any,
) -> None: ...
def declare_effect_with_child(
    comp_ctx: ComponentProcessorContext,
    fn_ctx: FnCallContext,
    provider: EffectProvider,
    key: Any,
    value: Any,
) -> EffectProvider: ...
def register_root_effect_provider(name: str, handler: Any) -> EffectProvider: ...
def fingerprint_memo_key(obj: Any) -> Fingerprint: ...
def reserve_memoization(
    comp_ctx: ComponentProcessorContext,
    memo_fp: Fingerprint,
) -> PendingFnCallMemo | Any: ...
async def reserve_memoization_async(
    comp_ctx: ComponentProcessorContext,
    memo_fp: Fingerprint,
) -> PendingFnCallMemo | Any: ...

########################################################
# Inspect
########################################################

def list_stable_paths(app: App) -> list[StablePath]: ...

########################################################
# Extras
########################################################

# --- Chunk (from extras) ---
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

# --- SeparatorSplitter (from extras) ---
class SeparatorSplitter:
    def __new__(
        cls,
        separators_regex: list[str],
        keep_separator: str | None = None,
        include_empty: bool = False,
        trim: bool = True,
    ) -> "SeparatorSplitter": ...
    def split(self, text: str) -> list[Chunk]: ...

# --- CustomLanguageConfig (from extras) ---
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

# --- RecursiveSplitter (from extras) ---
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
