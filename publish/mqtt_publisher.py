import json
import time
import paho.mqtt.client as mqtt


class MQTTPublisher:
    """
    Vibralyzer v4 – MQTT Publisher (ISO-Purist Multi-Site)

    Published Layers:
    -----------------
    1. L1 Features
       Topic: vibration/l1/{site}/{asset}/{point}

    2. Health (PHI Authority)
       Topic: vibration/health/{site}/{asset}/{point}

    3. Recommendation (Final Alarm)
       Topic: vibration/recommendation/{site}/{asset}/{point}

    4. L2 (Optional Diagnostic Layer)
       Topic: vibration/l2/{site}/{asset}/{point}
    """

    # =========================================================
    # INIT
    # =========================================================
    def __init__(self, broker: str, port: int):

        self.client = mqtt.Client()
        self.client.connect(broker, port)
        self.client.loop_start()

    # =========================================================
    # INTERNAL PUBLISH
    # =========================================================
    def _publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False):

        payload["timestamp"] = payload.get("timestamp", time.time())

        self.client.publish(
            topic,
            json.dumps(payload),
            qos=qos,
            retain=retain,
        )

    # =========================================================
    # L1 FEATURE
    # =========================================================
    def publish_l1(self, site: str, asset: str, point: str, payload: dict):

        topic = f"vibration/l1/{site}/{asset}/{point}"
        self._publish(topic, payload)

    # =========================================================
    # HEALTH (PHI AUTHORITY)
    # =========================================================
    def publish_health(self, site: str, asset: str, point: str, payload: dict):

        topic = f"vibration/health/{site}/{asset}/{point}"
        self._publish(topic, payload)

    # =========================================================
    # RECOMMENDATION (FINAL LAYER)
    # =========================================================
    def publish_recommendation(self, site: str, asset: str, point: str, payload: dict):

        topic = f"vibration/recommendation/{site}/{asset}/{point}"
        self._publish(topic, payload)

    # =========================================================
    # L2 DIAGNOSTIC (OPTIONAL)
    # =========================================================
    def publish_l2(self, site: str, asset: str, point: str, payload: dict):

        topic = f"vibration/l2/{site}/{asset}/{point}"
        self._publish(topic, payload)






