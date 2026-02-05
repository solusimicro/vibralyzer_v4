# runner.py
import time
import json
import logging
import signal
import threading
import numpy as np
import paho.mqtt.client as mqtt

from collections import deque
from publish.sparkplug.metric_mapper import MetricMapper
from publish.sparkplug.sparkplug_publisher import SparkplugPublisher
from diagnostic_l2.worker import l2_worker
from config.config_loader import load_config
from core.l1_feature_pipeline import L1FeaturePipeline  # your L1 pipeline

# ---------------------------
# CONFIG
# ---------------------------
BROKER = "localhost"
PORT = 1883
RAW_TOPIC = "vibration/#"
GROUP_ID = "VIBRALYZER"
EDGE_NODE = "EDGE01"

BUFFER_SIZE = 2048       # window size for L1
L2_COOLDOWN = 5          # seconds between L2 runs per asset/point
FS = 25600.0             # sampling freq
RPM = 1800.0             # asset RPM example

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("L2_RUNNER")

mapping_cfg = load_config("config/sparkplug_mapping.yaml")
metric_mapper = MetricMapper(mapping_cfg)

l1_pipeline = L1FeaturePipeline(fs=FS, rpm=RPM)
SEEN_DEVICES = set()
ring_buffer = {}   # asset -> point -> deque
last_l2_run = {}   # asset -> point -> timestamp

# ---------------------------
# SPARKPLUG PUBLISHER
# ---------------------------
publisher = SparkplugPublisher(
    broker=BROKER,
    port=PORT,
    group_id=GROUP_ID,
    edge_node=EDGE_NODE
)
time.sleep(1)
publisher.publish_nbirth()

# ---------------------------
# Early Fault FSM (simplified)
# ---------------------------
def early_fault_state(features):
    # Simple threshold example
    rms = features.get("acc_rms_g", 0)
    if rms > 1.0:
        return "WARNING"
    return "NORMAL"

# ---------------------------
# MQTT CALLBACKS
# ---------------------------
def on_connect(client, userdata, flags, rc):
    logger.info("Connected to RAW broker")
    client.subscribe(RAW_TOPIC)

def on_message(client, userdata, msg):
    try:
        _, _, asset, point = msg.topic.split("/")
        payload = json.loads(msg.payload.decode())

        # --- init ring buffer ---
        if asset not in ring_buffer:
            ring_buffer[asset] = {}
        if point not in ring_buffer[asset]:
            ring_buffer[asset][point] = deque(maxlen=BUFFER_SIZE)

        acc_signal = np.asarray(payload.get("acceleration", []), dtype=float)
        ring_buffer[asset][point].extend(acc_signal)

        # --- First time DBIRTH ---
        if asset not in SEEN_DEVICES:
            dbirth_metrics = metric_mapper.build_runtime_metrics({})
            publisher.publish_dbirth(asset, dbirth_metrics)
            SEEN_DEVICES.add(asset)
            logger.info(f"🟢 DBIRTH published for {asset}")

        # --- L1 Feature Extraction ---
        if len(ring_buffer[asset][point]) > 0:
            window = np.array(ring_buffer[asset][point])
            l1_features = l1_pipeline.compute(window)
            logger.info(f"L1 features: {l1_features}")

            l1_metrics = metric_mapper.build_runtime_metrics({
                **l1_features,
                "l1_timestamp": int(time.time() * 1000)
            })
            publisher.publish_ddatas(
                device=asset,
                point="_L1_FEATURE",
                metrics=l1_metrics
            )

            # --- Early Fault FSM ---
            state = early_fault_state(l1_features)
            state_metrics = metric_mapper.build_runtime_metrics({
                "early_fault_state": state,
                "timestamp": int(time.time() * 1000)
            })
            publisher.publish_ddatas(
                device=asset,
                point="_EARLYFAULT_STATE",
                metrics=state_metrics
            )
            logger.info(f"Early Fault State: {state}")

        # --- L2 Diagnostic Async (cooldown) ---
        now = time.time()
        last_ts = last_l2_run.get(asset, {}).get(point, 0)
        if now - last_ts > L2_COOLDOWN:
            def run_l2():
                try:
                    l2_values = l2_worker({
                        "asset": asset,
                        "point": point,
                        "timestamp": payload.get("timestamp"),
                        "acceleration": acc_signal
                    })
                    metrics = metric_mapper.build_runtime_metrics(l2_values)
                    publisher.publish_ddatas(
                        device=asset,
                        point=point,
                        metrics=metrics
                    )
                    logger.info(f"✅ L2 published | asset={asset} point={point}")
                except Exception as e:
                    logger.exception(f"L2 worker error: {e}")

            threading.Thread(target=run_l2).start()
            if asset not in last_l2_run:
                last_l2_run[asset] = {}
            last_l2_run[asset][point] = now

    except Exception as e:
        logger.exception(f"Error processing message: {e}")

# ---------------------------
# MQTT Client
# ---------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_start()

# ---------------------------
# Shutdown
# ---------------------------
def shutdown(signum=None, frame=None):
    logger.warning("Shutting down… sending DEATH messages")
    for device in SEEN_DEVICES:
        publisher.publish_ddeath(device)
    publisher.publish_ndeath()
    publisher.shutdown()
    client.loop_stop()
    client.disconnect()
    exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ---------------------------
# Keep Alive
# ---------------------------
while True:
    time.sleep(1)







