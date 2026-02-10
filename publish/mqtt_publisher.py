import json
import time  # noqa: F401
import paho.mqtt.client as mqtt


class MQTTPublisher:
    def __init__(self, broker: str, port: int = 1883):
        self.client = mqtt.Client()
        self.client.connect(broker, port)
        self.client.loop_start()

    def publish(self, topic: str, payload: dict):
        self.client.publish(topic, json.dumps(payload), qos=1)

    # --- Minimal L1 Feature ---
    def publish_l1(self, asset: str, point: str, payload: dict):
        topic = f"vibration/l1/{asset}/{point}"
        self.publish(topic, payload)

    # --- PHI / State ---
    def publish_health(self, asset: str, point: str, payload: dict):
        topic = f"vibration/health/{asset}/{point}"
        self.publish(topic, payload)

    # --- Recommendation / Final Alarm ---
    def publish_recommendation(self, asset: str, point: str, payload: dict):
        topic = f"vibration/recommendation/{asset}/{point}"
        self.publish(topic, payload)

    # --- Heartbeat ---
    def publish_heartbeat(self, payload: dict):
        topic = "vibration/system/heartbeat"
        self.publish(topic, payload)






