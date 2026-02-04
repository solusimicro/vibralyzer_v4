# runner.py
import time
import logging
import json
import paho.mqtt.client as mqtt

from publish.sparkplug.metric_mapper import MetricMapper
from publish.sparkplug.sparkplug_publisher import SparkplugPublisher
from diagnostic_l2.worker import l2_worker
from config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("L2_RUNNER")

# ---------------------------
# CONFIG
# ---------------------------
mapping_cfg = load_config("config/sparkplug_mapping.yaml")
metric_mapper = MetricMapper(mapping_cfg)

BROKER = "localhost"
PORT = 1883
RAW_TOPIC = "vibration/+/+"  # listen to all vibration points

publisher = SparkplugPublisher(broker=BROKER, port=PORT)

# ---------------------------
# MQTT CALLBACKS
# ---------------------------
def on_connect(client, userdata, flags, rc):
    logger.info(f"Connected to broker {BROKER}:{PORT} rc={rc}")
    client.subscribe(RAW_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        asset = payload.get("asset")
        point = payload.get("point")

        logger.info(f"🔍 L2 processing | asset={asset} point={point}")

        # Run L2 worker
        l2_values = l2_worker(payload)
        
        # Build Sparkplug metrics
        metrics = metric_mapper.build_runtime_metrics(l2_values)

        # Publish
        topic = f"vibration/{asset}/{point}"
        publisher.publish_ddatas(topic, metrics)

        logger.info(f"✅ L2 result ready | asset={asset} point={point} keys={list(l2_values.keys())}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}")

# ---------------------------
# MQTT CLIENT
# ---------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

# Keep runner alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("Shutting down runner...")
    client.loop_stop()
    client.disconnect()





