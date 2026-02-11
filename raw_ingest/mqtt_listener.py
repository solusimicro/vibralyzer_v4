import json
import traceback
import paho.mqtt.client as mqtt


def start_mqtt_listener(
    callback,
    broker: str,
    port: int,
    topic: str,
):
    """
    Multi-Site MQTT Listener
    -------------------------
    Expected RAW Topic:
        vibration/raw/{site}/{asset}/{point}

    Callback signature:
        callback(
            site_id: str,
            asset_id: str,
            point: str,
            raw_payload: dict
        )
    """

    # =========================================================
    # ON CONNECT
    # =========================================================
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] Connected to {broker}:{port}")
            client.subscribe(topic)
            print(f"[MQTT] Subscribed to: {topic}")
        else:
            print(f"[MQTT] Connection failed with code {rc}")

    # =========================================================
    # ON MESSAGE
    # =========================================================
    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())

            site, asset, point = _parse_topic(msg.topic)

            callback(
                site_id=site,
                asset_id=asset,
                point=point,
                raw_payload=payload,
            )

        except Exception:
            print("[MQTT] Message processing error:")
            traceback.print_exc()

    # =========================================================
    # CLIENT INIT
    # =========================================================
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, keepalive=60)
    client.loop_forever()


# =========================================================
# TOPIC PARSER
# =========================================================
def _parse_topic(topic: str):
    """
    Supported formats:

    Multi-site:
        vibration/raw/<SITE>/<ASSET>/<POINT>

    Single-site (legacy):
        vibration/raw/<ASSET>/<POINT>
    """

    parts = topic.split("/")

    if len(parts) == 5:
        # Multi-site
        _, _, site, asset, point = parts
        return site, asset, point

    elif len(parts) == 4:
        # Single-site → default site
        _, _, asset, point = parts
        return "default", asset, point

    else:
        raise ValueError(f"Invalid RAW topic format: {topic}")
