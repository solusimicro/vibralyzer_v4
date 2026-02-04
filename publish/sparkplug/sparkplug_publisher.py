<<<<<<< HEAD
# publish/sparkplug/sparkplug_publisher.py
=======
import time
import paho.mqtt.client as mqtt

from .sparkplug_b import Payload
from .sparkplug_b_pb2 import Metric
>>>>>>> 5a14687 (Sparkplug B publisher)

import time
from . import sparkplug_b_pb2 # Pastikan file pb2 sudah ada

class SparkplugPublisher:
<<<<<<< HEAD
=======
    """
    Sparkplug B Publisher
    - Metric MUST be built outside (metric_mapper)
    - This class handles lifecycle & publish only
    - TeslaSCADA compliant
    """

>>>>>>> 5a14687 (Sparkplug B publisher)
    def __init__(self, broker, port, group_id, edge_node):
        self.group_id = group_id
        self.edge_node = edge_node
        self.seq = 0 # Sequence number 0-255

<<<<<<< HEAD
    def _get_next_seq(self):
        self.seq = (self.seq + 1) % 256
        return self.seq

    def publish_dbirth(self, device_id, point_config):
        """Mendaftarkan Device (p1mt, dll) ke SCADA"""
        topic = f"spBv1.0/{self.group_id}/DBIRTH/{self.edge_node}/{device_id}"
        
        # Payload harus berisi semua definisi metrik awal
        # Contoh metrik: 'type', 'rpm_nominal', 'phi', 'velocity_rms'
        payload = {
            "timestamp": int(time.time() * 1000),
            "metrics": [
                {"name": "metadata/type", "value": point_config['type'], "type": "String"},
                {"name": "metadata/rpm_nominal", "value": point_config['rpm'], "type": "Int32"},
                {"name": "phi", "value": 100.0, "type": "Float"},
                {"name": "state", "value": "NORMAL", "type": "String"}
            ],
            "seq": self._get_next_seq()
        }
        # Kirim ke MQTT (Gunakan library paho untuk publish payload yang sudah di-serialize)
        print(f"DEBUG: DBIRTH sent for {device_id}")

    def publish_ddata(self, device_id, metrics_dict):
        """Kirim update data real-time"""
        topic = f"spBv1.0/{self.group_id}/DDATA/{self.edge_node}/{device_id}"
        # Konversi dict ke format Sparkplug Metrics
        # ...
=======
        self.seq = 0

        client_id = f"spBv1.0-{group_id}-{edge_node}"

        self.client = mqtt.Client(
            client_id=client_id,
            clean_session=False,  # REQUIRED by Sparkplug
        )

        # --------------------------------------------------
        # NDEATH certificate (Last Will)
        # --------------------------------------------------
        death_payload = Payload()
        death_payload.timestamp = self._now()
        death_payload.seq = self._next_seq()

        self.client.will_set(
            self._topic("NDEATH"),
            payload=death_payload.SerializeToString(),
            qos=0,
            retain=False,
        )

        self.client.connect(broker, port)
        self.client.loop_start()

    # ==================================================
    # INTERNAL
    # ==================================================
    def _now(self):
        return int(time.time() * 1000)

    def _next_seq(self):
        self.seq = (self.seq + 1) % 256
        return self.seq

    def _topic(self, msg_type, device_id=None):
        if device_id:
            return f"spBv1.0/{self.group_id}/{msg_type}/{self.edge_node}/{device_id}"
        return f"spBv1.0/{self.group_id}/{msg_type}/{self.edge_node}"

    def _build_payload(self, metrics=None):
        payload = Payload()
        payload.timestamp = self._now()
        payload.seq = self._next_seq()

        if metrics:
            payload.metrics.extend(metrics)

        return payload

    # ==================================================
    # PUBLIC – STARTUP (LIFECYCLE ENTRYPOINT)
    # ==================================================
    def start(self):
        """
        Sparkplug lifecycle entrypoint
        MUST be called explicitly after init
        """
        self.publish_nbirth()
        self.publish_l2_dbirth()

    # ==================================================
    # PUBLIC – LIFECYCLE
    # ==================================================
    def publish_nbirth(self, metrics=None):
        """
        Node Birth
        """
        if metrics is None:
            metrics = []

            m = Metric()
            m.name = "Node Control/Rebirth"
            m.datatype = Metric.Boolean
            m.boolean_value = False
            metrics.append(m)

        payload = self._build_payload(metrics)

        self.client.publish(
            self._topic("NBIRTH"),
            payload.SerializeToString(),
            qos=0,
            retain=False,
        )

    def publish_dbirth(self, device_id, metrics):
        """
        Device Birth
        """
        payload = self._build_payload(metrics)

        self.client.publish(
            self._topic("DBIRTH", device_id),
            payload.SerializeToString(),
            qos=0,
            retain=False,
        )

    def publish_l2_dbirth(self):
        """
        DBIRTH for logical L2 analytics engine
        """
        metrics = []

        m_alive = Metric()
        m_alive.name = "l2_alive"
        m_alive.datatype = Metric.Boolean
        m_alive.boolean_value = True
        metrics.append(m_alive)

        m_ts = Metric()
        m_ts.name = "l2_start_ts"
        m_ts.datatype = Metric.Int64
        m_ts.long_value = int(time.time())
        metrics.append(m_ts)

        self.publish_dbirth(
            device_id="L2_ENGINE",
            metrics=metrics,
        )

    # ==================================================
    # PUBLIC – DATA
    # ==================================================
    def publish_ddata(self, asset, point, metrics):
        """
        Publish device data (DDATA)
        Device ID convention: {asset}_{point}
        """
        device_id = f"{asset}_{point}"
        payload = self._build_payload(metrics)

        self.client.publish(
            self._topic("DDATA", device_id),
            payload.SerializeToString(),
            qos=0,
            retain=False,
        )

    # ==================================================
    # PUBLIC – HEARTBEAT
    # ==================================================
    def publish_l2_alive(self):
        """
        Periodic heartbeat for L2 engine
        """
        metrics = []

        m = Metric()
        m.name = "l2_alive"
        m.datatype = Metric.Boolean
        m.boolean_value = True
        metrics.append(m)

        payload = self._build_payload(metrics)

        self.client.publish(
            self._topic("DDATA", device_id="L2_ENGINE"),
            payload.SerializeToString(),
            qos=0,
            retain=False,
        )
>>>>>>> 5a14687 (Sparkplug B publisher)
