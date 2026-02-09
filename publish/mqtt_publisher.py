import json
import time
import paho.mqtt.client as mqtt


class MQTTPublisher:
    """
    MQTT Publisher (ISO-Purist)

    Responsibility:
    - Publish FINAL authoritative signals only
    - NO business logic
    - NO interpretation
    - NO FSM knowledge
    - Flat JSON only (SCADA-safe)
    """

    def __init__(self, broker, port, base_topic="vibration"):
        self.base_topic = base_topic

        self.client = mqtt.Client()
        self.client.connect(broker, port)
        self.client.loop_start()

    # =========================================================
    # INTERNAL
    # =========================================================
    def _publish(self, topic, payload, qos=1, retain=False):
        self.client.publish(
            topic,
            json.dumps(payload),
            qos=qos,
            retain=retain,
        )

    # =========================================================
    # OPTIONAL – ENGINEERING VISIBILITY
    # =========================================================
    def publish_l1_features(self, asset, point, payload):
        """
        Optional.
        Engineering / analytics only.
        Disable in production SCADA if needed.
        """
        topic = f"{self.base_topic}/l1/{asset}/{point}"
        self._publish(topic, payload)

    # =========================================================
    # FINAL ALARM (SINGLE SOURCE OF TRUTH)
    # =========================================================
    def publish_final_alarm(self, asset, point, payload):
        """
        FINAL ALARM / RECOMMENDATION
        This is the ONLY alarm SCADA should consume.
        """
        topic = f"{self.base_topic}/final_alarm/{asset}/{point}"

        # ISO guard: mandatory fields
        final_payload = {
            "asset": asset,
            "point": point,
            "state": payload["state"],            # NORMAL | WATCH | WARNING | ALARM
            "phi": round(payload["phi"], 2),      # 0–100
            "action_code": payload.get("action_code"),
            "priority": payload.get("priority"),
            "text": payload.get("text"),
            "timestamp": payload.get("timestamp", time.time()),
        }

        self._publish(topic, final_payload, retain=True)

    # =========================================================
    # HEARTBEAT
    # =========================================================
    def publish_heartbeat(self, payload):
        """
        System liveness only.
        """
        topic = f"{self.base_topic}/heartbeat"
        self._publish(topic, payload, qos=0, retain=False)

    # =========================================================
    # SHUTDOWN
    # =========================================================
    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()





