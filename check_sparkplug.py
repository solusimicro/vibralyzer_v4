import json
import logging
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

BROKER = "localhost"
PORT = 1883
TOPIC = "vibration/#"

# =========================
# CALLBACKS API v1 (MQTT 3.1.1)
# =========================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("✅ Connected to MQTT broker")
        client.subscribe(TOPIC)
        logging.info(f"Subscribed to topic: {TOPIC}")
    else:
        logging.error(f"Failed to connect, rc={rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        try:
            data = json.loads(payload)
            logging.info(f"{msg.topic} → {json.dumps(data, indent=2)}")
        except json.JSONDecodeError:
            logging.info(f"{msg.topic} → {payload}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

# =========================
# CLIENT (pakai callback API v1)
# =========================
client = mqtt.Client(client_id="vibralyzer_debug_sub", protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message

try:
    logging.info("Connecting to MQTT broker...")
    client.connect(BROKER, PORT, 60)
    client.loop_forever()
except Exception as e:
    logging.error(f"MQTT connection error: {e}")



