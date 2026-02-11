from collections import deque
import copy


class RingBufferManager:
    """
    Ring Buffer Manager
    ===================
    - Per asset + point buffer
    - Fixed window size
    - Safe against malformed payload
    - Backward compatible (add() alias)
    """

    def __init__(self, window_size=4096):
        self.window_size = window_size
        self.buffers = {}

    # =========================================================
    # INTERNAL
    # =========================================================
    def _key(self, asset, point):
        return f"{asset}:{point}"

    # =========================================================
    # PUBLIC API
    # =========================================================
    def append(self, asset, point, raw):
        """
        Append raw acceleration data into ring buffer.
        Expected format:
        raw = {
            "acceleration": [...]
        }
        """

        if not raw or "acceleration" not in raw:
            return  # ignore invalid payload safely

        key = self._key(asset, point)

        if key not in self.buffers:
            self.buffers[key] = deque(maxlen=self.window_size)

        self.buffers[key].extend(raw["acceleration"])

    # 🔥 BACKWARD COMPATIBILITY
    def add(self, asset, point, raw):
        """Alias for append() to prevent breaking old code."""
        self.append(asset, point, raw)

    def is_window_ready(self, asset, point):
        key = self._key(asset, point)
        return key in self.buffers and len(self.buffers[key]) >= self.window_size

    def get_window(self, asset, point):
        key = self._key(asset, point)

        if key not in self.buffers:
            return None

        return copy.deepcopy(list(self.buffers[key]))

    def clear(self, asset, point):
        key = self._key(asset, point)
        if key in self.buffers:
            self.buffers[key].clear()


