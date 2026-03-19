import os

from cocoindex._internal.serde import enable_strict_serialize

os.environ.setdefault("PYTHONASYNCIODEBUG", "1")
enable_strict_serialize()
