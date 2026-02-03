"""Sparkplug B protobuf definitions (stub implementation)"""


class Metric:
    """Represents a Sparkplug B metric"""

    def __init__(self):
        self.name = None
        self.int_value = None
        self.float_value = None
        self.timestamp = None
        self.is_null = False

    def __repr__(self):
        return f"Metric(name={self.name}, int_value={self.int_value}, float_value={self.float_value})"
