# publish/sparkplug/sparkplug_publisher.py
import logging
import warnings
import time
import paho.mqtt.client as mqtt
from .sparkplug_b import Payload, MetricWrapper

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message="Callback API version 1 is deprecated"
)

logger = logging.getLogger("SPARKPLUG_PUBLISHER")

class SparkplugPublisher:
    def __init__(
        self,
        broker: str,
        port: int = 1883,
        group_id: str = "VIBRALYZER",
        edge_node: str = "EDGE01",
    ):
        self.seq = 0
        self.group_id = group_id
        self.edge_node = edge_node

        self.client = mqtt.Client()
        self.client.connect(broker, port)
        self.client.loop_start()

        logger.info(f"MQTT Connected to {broker}:{port}")

    def _now(self):
        return int(time.time() * 1000)

    # ==================================================
    # NBIRTH
    # ==================================================
    def publish_nbirth(self):
        payload = Payload()
        payload.seq = self.seq
        self.seq += 1

        # Mandatory Sparkplug metric
        payload.metrics.add().CopyFrom(
            MetricWrapper(
                name="Node Control/Rebirth",
                value=False,
                timestamp=self._now()
            ).pb_metric
        )

        payload.metrics.add().CopyFrom(
            MetricWrapper(
                name="Node Control/Reboot",
                value=False,
                timestamp=self._now()
            ).pb_metric
        )

        topic = f"spBv1.0/{self.group_id}/NBIRTH/{self.edge_node}"
        raw = payload.SerializeToString()
        self.client.publish(topic, raw)

        logger.info(f"🟢 NBIRTH published → {topic}")
        
        logger.warning("🚨 publish_nbirth() CALLED")  # <--- WAJIB MUNCUL
    # ==================================================
    # DBIRTH
    # ==================================================
    def publish_dbirth(
        self,
        device: str,
        metric_templates: list[MetricWrapper]
    ):
        payload = Payload()
        payload.seq = self.seq
        self.seq += 1

        # Semua metric yang akan dikirim runtime
        for m in metric_templates:
            payload.metrics.add().CopyFrom(m.pb_metric)

        topic = f"spBv1.0/{self.group_id}/DBIRTH/{self.edge_node}/{device}"
        raw = payload.SerializeToString()
        self.client.publish(topic, raw)

        logger.info(f"🟢 DBIRTH published → {topic}")
    
    # ==================================================
    # DDATA
    # ==================================================
    def publish_ddatas(
        self,
        device: str,
        metrics: list[MetricWrapper],
        point: str | None = None,
    ):
        payload = Payload()
        payload.seq = self.seq
        self.seq += 1

        for m in metrics:
            payload.metrics.add().CopyFrom(m.pb_metric)

        if point:
            topic = f"spBv1.0/{self.group_id}/DDATA/{self.edge_node}/{device}/{point}"
        else:
            topic = f"spBv1.0/{self.group_id}/DDATA/{self.edge_node}/{device}"

        raw_bytes = payload.SerializeToString()
        self.client.publish(topic, raw_bytes)

        logger.info(
            f"📡 DDATA published → {topic} | metrics={len(metrics)} seq={payload.seq}"
        )
  
    