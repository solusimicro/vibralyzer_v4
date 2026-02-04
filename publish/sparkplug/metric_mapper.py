import time
from .sparkplug_b import MetricWrapper

TYPE_MAP = {}  # tidak perlu, karena kita handle value secara otomatis

class MetricMapper:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def _now(self):
        return int(time.time() * 1000)

    def build_runtime_metrics(self, values: dict):
        metrics = []

        for name, value in values.items():
            if name not in self.mapping:
                continue

            m = MetricWrapper(name=name, value=value, timestamp=self._now())
            metrics.append(m)

        return metrics

