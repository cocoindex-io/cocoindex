import threading
import asyncio

class _OpExecutionContext:
    _lock: threading.Lock
    _event_loop: asyncio.AbstractEventLoop | None = None

    def __init__(self):
        self._lock = threading.Lock()

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop for the cocoindex library."""
        with self._lock:
            if self._event_loop is None:
                self._event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._event_loop)
                threading.Thread(target=self._event_loop.run_forever, daemon=True).start()
            return self._event_loop

op_execution_context = _OpExecutionContext()
