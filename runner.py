import time
from venv import logger

# === INGEST ===
from raw_ingest.mqtt_listener import start_mqtt_listener

# === CORE ===
from core.ring_buffer import RingBufferManager
from core.l1_feature_pipeline import L1FeaturePipeline

# === EARLY FAULT (INTERNAL EVIDENCE) ===
from early_fault.trend_detector import TrendDetector
from early_fault.persistence import PersistenceChecker
from early_fault.scoring import EarlyFaultFSM
from early_fault.baseline import AdaptiveBaseline

# === HEALTH AUTHORITY ===
from health.point_health_index import compute_phi
from health.state_mapping import phi_to_state

# === PUBLISH ===
from publish.mqtt_publisher import MQTTPublisher

# === CONFIG ===
from config.config_loader import load_config

# === L2 ASYNC ===
from diagnostic_l2.cooldown import L2CooldownManager
from diagnostic_l2.l2_queue import L2JobQueue
from diagnostic_l2.worker import l2_worker

# === SYSTEM ===
from utils.heartbeat import Heartbeat


# ============================================================
# RUNNER — ISO PURIST
# ============================================================

def main():
    config = load_config()

    # ----------------------------
    # SYSTEM
    # ----------------------------
    heartbeat = Heartbeat(service_name="vibralyzer")
    last_heartbeat_ts = time.time()
    HEARTBEAT_INTERVAL = config.get("heartbeat", {}).get("interval_sec", 10)

    # ----------------------------
    # CORE PIPELINE
    # ----------------------------
    ring_buffers = RingBufferManager(
        window_size=config["raw"]["window_size"]
    )

    l1_pipeline = L1FeaturePipeline(
        fs=config["l1_feature"]["sampling_rate"],
        rpm=config["l1_feature"]["rpm_default"],
    )

    baseline = AdaptiveBaseline(
        alpha=config.get("baseline", {}).get("alpha", 0.01),
        min_samples=config.get("baseline", {}).get("min_samples", 100),
    )

    trend_detector = TrendDetector()
    persistence_checker = PersistenceChecker()

    early_fault_fsm = EarlyFaultFSM(
        watch_persistence=config["early_fault"]["watch_persistence"],
        warning_persistence=config["early_fault"]["warning_persistence"],
        alarm_persistence=config["early_fault"]["alarm_persistence"],
        hysteresis_clear=config["early_fault"]["hysteresis_clear"],
    )

    publisher = MQTTPublisher(
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
    )

    # ----------------------------
    # L2 ASYNC
    # ----------------------------
    l2_cooldown = L2CooldownManager(
        warning_sec=config["l2"]["cooldown_warning_sec"],
        alarm_sec=config["l2"]["cooldown_alarm_sec"],
    )

    l2_queue = L2JobQueue(maxsize=10)
    l2_queue.start(l2_worker)

    # ----------------------------
    # RAW CALLBACK
    # ----------------------------
    def on_raw_data(asset_id, point, raw_payload):
        nonlocal last_heartbeat_ts

        heartbeat.mark_raw_rx()
        ring_buffers.append(asset_id, point, raw_payload)

        if not ring_buffers.is_window_ready(asset_id, point):
            return

        # ---- WINDOW ----
        window = ring_buffers.get_window(asset_id, point)

        # ---- L1 FEATURES ----
        heartbeat.mark_l1_exec()
        l1_features = l1_pipeline.compute(window)

        # ---- BASELINE / TREND ----
        raw_trend = trend_detector.update(asset_id, point, l1_features)

        baseline.update(
            asset_id,
            point,
            l1_features,
            allow_update=(raw_trend.level == "NORMAL"),
        )

        persistence = persistence_checker.update(asset_id, point, raw_trend)

        # ---- FSM (EVIDENCE ONLY, INTERNAL) ----
        early_fault = early_fault_fsm.update(
            asset=asset_id,
            point=point,
            trend=raw_trend,
            persistence=persistence,
        )

        # ---- HEALTH AUTHORITY ----
        phi = compute_phi(l1_features)
        state = phi_to_state(phi)

        # ---- FINAL ALARM (RECOMMENDATION) ----
        publisher.publish_final_alarm(
            asset_id,
            point,
            {
                "asset": asset_id,
                "point": point,
                "state": state,
                "phi": phi,
                "timestamp": time.time(),
            },
        )

        # ---- L2 TRIGGER (INTERNAL) ----
        if config["l2"]["enable"] and state in ("WARNING", "ALARM"):
            if l2_cooldown.can_trigger(asset_id, point, state):
                l2_queue.enqueue({
                    "asset": asset_id,
                    "point": point,
                    "l1_snapshot": {
                        "asset": asset_id,
                        "point": point,
                        "features": l1_features,
                        "timestamp": time.time(),
                    },
                    "publisher": publisher,
                })
                l2_cooldown.mark_triggered(asset_id, point)

        # ---- HEARTBEAT ----
        now = time.time()
        if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL:
            publisher.publish_heartbeat(heartbeat.snapshot())
            last_heartbeat_ts = now

    # ----------------------------
    # START INGEST
    # ----------------------------
    start_mqtt_listener(
        callback=on_raw_data,
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
        topic=config["mqtt"]["raw_topic"],
    )


if __name__ == "__main__":
    try:
        logger.info("🚀 Vibralyzer Runner started (ISO-PURIST)")
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Runner stopped by user")
    except Exception:
        logger.exception("❌ Fatal runner error")

