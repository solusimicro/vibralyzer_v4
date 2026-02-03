import time
import paho.mqtt.client as mqtt
from .sparkplug_b import Payload


class SparkplugPublisher:
    """
    Sparkplug B Publisher
    - Metric MUST be built outside (metric_mapper)
    - This class only publishes
    """

    def __init__(self, broker, port, group_id, edge_node):
        self.group_id = group_id
        self.edge_node = edge_node

        client_id = f"spBv1.0-{group_id}-{edge_node}"

        self.client = mqtt.Client(
            client_id=client_id,
            clean_session=True,
        )

        # Sparkplug DEATH certificate
        self.client.will_set(
            self._topic("NDEATH"),
            payload=b"",
            qos=0,
            retain=False,
        )

        self.client.connect(broker, port)
        self.client.loop_start()

    # ==================================================
    # INTERNAL
    # ==================================================
    def _topic(self, msg_type, device_id=None):
        if device_id:
            return f"spBv1.0/{self.group_id}/{msg_type}/{self.edge_node}/{device_id}"
        return f"spBv1.0/{self.group_id}/{msg_type}/{self.edge_node}"

    # ==================================================
    # PUBLIC
    # ==================================================
    def publish_ddata(self, asset, point, metrics: list):
        """
        metrics: list of Sparkplug Metric objects
        """

        device_id = f"{asset}_{point}"

        payload = Payload()
        payload.timestamp = int(time.time() * 1000)

        # 🚫 DO NOT BUILD METRIC HERE
        payload.metrics.extend(metrics)

        self.client.publish(
            self._topic("DDATA", device_id),
            payload.SerializeToString(),
            qos=0,
            retain=False,
        )

