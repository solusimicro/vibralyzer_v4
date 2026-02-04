from . import sparkplug_b_pb2 as pb

Payload = pb.Payload  # protobuf Payload

class MetricWrapper:
    """Wrapper sederhana untuk satu metric dalam Payload"""
    def __init__(self, name: str = None, value=None, timestamp: int | None = None):
        # Buat temporary Payload untuk menampung metric
        self.pb_payload = Payload()
        self.pb_metric = self.pb_payload.metrics.add()

        if name:
            self.name = name
        if value is not None:
            self.set_value(value)
        if timestamp:
            self.timestamp = timestamp

    @property
    def name(self):
        return self.pb_metric.name

    @name.setter
    def name(self, v):
        self.pb_metric.name = v

    @property
    def timestamp(self):
        return self.pb_metric.timestamp

    @timestamp.setter
    def timestamp(self, v):
        self.pb_metric.timestamp = v

    def set_value(self, value):
        if value is None:
            self.pb_metric.is_null = True
        elif isinstance(value, bool):
            self.pb_metric.boolean_value = value
        elif isinstance(value, int):
            self.pb_metric.long_value = value
        elif isinstance(value, float):
            self.pb_metric.double_value = value
        elif isinstance(value, str):
            self.pb_metric.string_value = value
        else:
            raise TypeError(f"Unsupported type: {type(value)}")

    @property
    def long_value(self):
        return self.pb_metric.long_value

    @property
    def double_value(self):
        return self.pb_metric.double_value

    @property
    def boolean_value(self):
        return self.pb_metric.boolean_value

    @property
    def string_value(self):
        return self.pb_metric.string_value



