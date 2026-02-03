# publish/sparkplug/sparkplug_b.py
# Shim sederhana: eksport Metric dan sediakan Payload container minimal
from .sparkplug_b_pb2 import Metric

class Payload:
    """Simple Payload container compatible with basic usage:
    - menyimpan daftar metrics di attribute .metrics
    - menyediakan helper add_metric(name, int_value=None, float_value=None, timestamp=None)
    """
    def __init__(self):
        self.metrics = []
        self.timestamp = None
        self.seq = None

    def add_metric(self, name, int_value=None, float_value=None, timestamp=None, is_null=False):
        m = Metric()
        m.name = name
        m.int_value = int_value
        m.float_value = float_value
        m.timestamp = timestamp
        m.is_null = is_null
        self.metrics.append(m)
        return m

__all__ = ["Payload", "Metric"]
