import json
import time
import copy
import paho.mqtt.client as mqtt


class MQTTPublisher:
    """
    Vibralyzer v4 – MQTT Publisher (Industrial-Grade)

    Layers:
    -------
    Point:
        vibration/l1/{site}/{asset}/{point}
        vibration/health/{site}/{asset}/{point}
        vibration/recommendation/{site}/{asset}/{point}
        vibration/diagnostic/{site}/{asset}/{point}

    Asset:
        vibration/asset/health/{site}/{asset}
        vibration/asset/recommendation/{site}/{asset}
    """

    # =========================================================
    # INIT
    # =========================================================
    def __init__(self, broker: str, port: int, client_id: str = "vibralyzer_v4"):

        self.client = mqtt.Client(client_id=client_id)
        self.client.connect(broker, port)
        self.client.loop_start()

    # =========================================================
    # INTERNAL SAFE PUBLISH
    # =========================================================
    def _publish(
        self,
        topic: str,
        payload: dict,
        qos: int = 1,
        retain: bool = False,
    ):

        # Copy to avoid mutating original payload
        data = copy.deepcopy(payload)

        # Only add timestamp if not provided
        if "timestamp" not in data:
            data["timestamp"] = time.time()

        result = self.client.publish(
            topic,
            json.dumps(data),
            qos=qos,
            retain=retain,
        )

        # Optional delivery check
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[MQTT] Publish failed: {topic} (rc={result.rc})")

    # =========================================================
    # ---------------- POINT LEVEL ----------------
    # =========================================================

    def publish_l1(self, site: str, asset: str, point: str, payload: dict):
        topic = f"vibration/l1/{site}/{asset}/{point}"
        self._publish(topic, payload, retain=False)

    def publish_health(self, site: str, asset: str, point: str, payload: dict):
        topic = f"vibration/health/{site}/{asset}/{point}"
        self._publish(topic, payload, retain=True)

    def publish_recommendation(self, site: str, asset: str, point: str, payload: dict):
        topic = f"vibration/recommendation/{site}/{asset}/{point}"
        self._publish(topic, payload, retain=True)

    def publish_diagnostic(self, site: str, asset: str, point: str, payload: dict):
        topic = f"vibration/diagnostic/{site}/{asset}/{point}"
        self._publish(topic, payload, retain=False)

    # =========================================================
    # ---------------- ASSET LEVEL ----------------
    # =========================================================

    def publish_asset_health(self, site: str, asset: str, payload: dict):
        topic = f"vibration/asset/health/{site}/{asset}"
        self._publish(topic, payload, retain=True)

    def publish_asset_recommendation(self, site: str, asset: str, payload: dict):
        topic = f"vibration/asset/recommendation/{site}/{asset}"
        self._publish(topic, payload, retain=True)
# =========================================================  



