import logging
import paho.mqtt.client as mqtt
from .sparkplug_b import Payload, MetricWrapper

logger = logging.getLogger("SPARKPLUG_PUBLISHER")

class SparkplugPublisher:
    def __init__(self, broker: str, port: int = 1883):
        self.seq = 0
        self.client = mqtt.Client()
        self.client.connect(broker, port)
        self.client.loop_start()
        logger.info(f"MQTT Connected to {broker}:{port}")

    def publish_ddatas(self, topic: str, metrics: list[MetricWrapper]):
        payload = Payload()
        payload.seq = self.seq
        self.seq += 1

        for m in metrics:
            payload.metrics.add().CopyFrom(m.pb_metric)

        raw_bytes = payload.SerializeToString()
        self.client.publish(topic, raw_bytes)
        logger.info(f"📡 Published {topic} | metrics={len(metrics)} seq={payload.seq} size={len(raw_bytes)} bytes")


