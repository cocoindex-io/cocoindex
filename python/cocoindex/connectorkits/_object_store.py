"""Key and path handling shared by the object store sources (S3, Azure Blob, OCI).

An object store has flat keys, not directories. A walker lists the objects in
a folder (a key prefix) and keys each one by the rest of its key, exactly as
stored: two objects with different keys always get different item keys.
"""

from __future__ import annotations

from abc import abstractmethod
from pathlib import PurePath

from cocoindex.resources.file import FilePath


def folder_prefix(prefix: str) -> str:
    """Return the key prefix of the objects in the folder ``prefix``.

    A prefix names a folder, so ``"docs"`` and ``"docs/"`` both select the
    objects under ``docs/``, and neither selects ``docsa.md``.
    """
    if not prefix or prefix.endswith("/"):
        return prefix
    return prefix + "/"


def relative_key(object_key: str, folder: str) -> str | None:
    """Return the item key of ``object_key`` in ``folder`` (from :func:`folder_prefix`).

    Returns ``None`` for an object outside the folder and for a folder marker
    (a key ending with ``/``).
    """
    if object_key.endswith("/") or not object_key.startswith(folder):
        return None
    return object_key[len(folder) :]


class ObjectStoreFilePath(FilePath[str]):
    """File path of an object in an object store.

    ``path`` is the object key relative to the listed folder, and ``resolve()``
    returns the full object key. A derived path (``parent``, ``/``,
    ``with_suffix()`` ...) resolves to the object key of the new path in the
    same folder.

    Equality, hashing, ordering and the memo key use the store (bucket or
    container) and the relative key as stored, so keys that ``PurePath``
    normalizes to one path (``a//b.txt`` and ``a/b.txt``) stay distinct.
    """

    __slots__ = ("_key_prefix", "_relative_key")

    _key_prefix: str
    _relative_key: str

    def __init__(self, path: str | PurePath, *, object_key: str) -> None:
        relative_key = path if isinstance(path, str) else path.as_posix()
        if not object_key.endswith(relative_key):
            raise ValueError(
                f"Object key {object_key!r} does not end with the path {relative_key!r}."
            )
        super().__init__(None, PurePath(relative_key))
        self._key_prefix = object_key[: len(object_key) - len(relative_key)]
        self._relative_key = relative_key

    @abstractmethod
    def _store(self) -> tuple[str, ...]:
        """Identify the bucket or container that holds the object."""

    def resolve(self) -> str:
        """Return the full object key."""
        return self._key_prefix + self._relative_key

    def _object_key_of(self, path: PurePath) -> str:
        """Return the object key of ``path`` in the same folder as this path."""
        return self._key_prefix + path.as_posix()

    def __coco_memo_key__(self) -> object:
        # Existing memo keys hold the PurePath: keep it whenever it gives back the
        # key, and use the key itself only when PurePath would merge it with another.
        if self._path.as_posix() == self._relative_key:
            return (*self._store(), self._path)
        return (*self._store(), self._relative_key)

    def _identity(self) -> tuple[str, ...]:
        return (*self._store(), self._relative_key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectStoreFilePath) or type(other) is not type(self):
            return NotImplemented
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash((type(self), self._identity()))

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ObjectStoreFilePath) or type(other) is not type(self):
            return NotImplemented
        return self._identity() < other._identity()
