"""
FastAPI wrapper for the v1 image search pipeline.
"""

from __future__ import annotations

import pathlib
import sys

_EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(_EXAMPLES_DIR) not in sys.path:
    sys.path.append(str(_EXAMPLES_DIR))

from _image_search_shared import create_search_api

try:
    from . import main as image_search
except ImportError:
    import importlib

    image_search = importlib.import_module("main")

app = create_search_api(image_search)
