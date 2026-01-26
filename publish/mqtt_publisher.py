import json
import paho.mqtt.client as mqtt


class MQTTPublisher:
    def __init__(self, broker: str, port: int):
        self.broker = broker
        self.port = port

        self.client = mqtt.Client()
        self.client.connect(self.broker, self.port)
        self.client.loop_start()

    # =========================
    # EARLY FAULT (FSM)
    # =========================
    def publish_early_fault(self, asset, point, payload: dict):
        topic = f"vibration/early_fault/{asset}/{point}"
        self.client.publish(topic, json.dumps(payload))

    # =========================
    # L2 DIAGNOSTIC
    # =========================
    def publish_l2_result(self, asset, point, payload: dict):
        topic = f"vibration/l2_result/{asset}/{point}"
        self.client.publish(topic, json.dumps(payload))

    # =========================
    # SCADA AGGREGATED OUTPUT
    # =========================
    def publish_scada(self, asset, point, payload: dict):
        """
        Final SCADA-facing topic.
        DO NOT change field names without SCADA approval.
        """
        topic = f"vibration/scada/{asset}/{point}"
        self.client.publish(topic, json.dumps(payload))
        
    # =========================
    # FINAL HEALTH ALARM (PHI-BASED)
    # =========================
    def publish_health_alarm(self, asset, point, payload: dict):
        """
        Final health alarm derived ONLY from Point Health Index (PHI)
        This is the authoritative alarm for SCADA.
        """
        topic = f"vibration/health_alarm/{asset}/{point}"
        self.client.publish(topic, json.dumps(payload))

    # =========================
    # SYSTEM HEARTBEAT
    # =========================
    def publish_heartbeat(self, payload: dict):
        topic = "vibration/heartbeat"
        self.client.publish(topic, json.dumps(payload))

    # =========================
    # GENERIC (INTERNAL USE ONLY)
    # =========================
    def publish_json(self, topic: str, payload: dict):
        """
        Internal utility.
        NOT for SCADA or external systems.
        """
        self.client.publish(topic, json.dumps(payload))


