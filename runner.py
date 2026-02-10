import time
from venv import logger  # noqa: F401

from raw_ingest.mqtt_listener import start_mqtt_listener
from core.ring_buffer import RingBufferManager
from core.l1_feature_pipeline import L1FeaturePipeline

from early_fault.trend_detector import TrendDetector
from early_fault.persistence import PersistenceChecker
from early_fault.scoring import EarlyFaultFSM
from early_fault.baseline import AdaptiveBaseline

from publish.mqtt_publisher import MQTTPublisher
from config.config_loader import load_config

from diagnostic_l2.cooldown import L2CooldownManager
from diagnostic_l2.l2_queue import L2JobQueue
from diagnostic_l2.worker import l2_worker

from analytics.recommendation.recommendation_engine import RecommendationEngine
from utils.heartbeat import Heartbeat

# === ISO Health Layer ===
from health.point_health_index import compute_phi
from health.state_mapping import phi_to_state


def main():
    config = load_config()

    heartbeat = Heartbeat(service_name="vibralyzer")
    last_heartbeat_ts = time.time()
    HEARTBEAT_INTERVAL = config.get("heartbeat", {}).get("interval_sec", 10)

    ring_buffers = RingBufferManager(window_size=config["raw"]["window_size"])
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

    recommendation_engine = RecommendationEngine()
    l2_cooldown = L2CooldownManager(
        warning_sec=config["l2"]["cooldown_warning_sec"],
        alarm_sec=config["l2"]["cooldown_alarm_sec"],
    )
    l2_queue = L2JobQueue(maxsize=10)
    l2_queue.start(l2_worker)

    def on_raw_data(asset_id, point, raw_payload):
        nonlocal last_heartbeat_ts

        heartbeat.mark_raw_rx()
        ring_buffers.append(asset_id, point, raw_payload)

        if not ring_buffers.is_window_ready(asset_id, point):
            return

        window = ring_buffers.get_window(asset_id, point)
        heartbeat.mark_l1_exec()
        l1_features = l1_pipeline.compute(window)

        # --- SCADA minimal L1 publish ---
        publisher.publish_l1(
            asset_id,
            point,
            {
                "acceleration_rms_g": l1_features["acc_rms_g"],
                "overall_vel_rms_mm_s": l1_features["overall_vel_rms_mm_s"],
                "crest_factor": l1_features["crest_factor"],
                "temperature_c": raw_payload.get("temperature"),
                "timestamp": time.time(),
            },
        )

        # --- BASELINE / TREND / FSM (internal) ---
        raw_trend = trend_detector.update(asset_id, point, l1_features)
        baseline.update(asset_id, point, l1_features, allow_update=(raw_trend.level == "NORMAL"))
        persistence = persistence_checker.update(asset_id, point, raw_trend)
        early_fault = early_fault_fsm.update(asset=asset_id, point=point, trend=raw_trend, persistence=persistence)

        # --- ISO Health / PHI ---
        phi = compute_phi(l1_features)
        state = phi_to_state(phi)
        fault_type = "GENERAL_HEALTH" if state in ("NORMAL", "WATCH") else early_fault.dominant_feature or "GENERAL_HEALTH"

        # --- Publish Health / PHI ---
        publisher.publish_health(
            asset_id,
            point,
            {
                "point_health_index": phi,
                "state": state,
                "fault_type": fault_type,
                "confidence": early_fault.confidence,
                "fsm_state": early_fault.state.value,
                "timestamp": time.time(),
            },
        )

        # --- L2 Async Trigger (internal only) ---
        if config["l2"]["enable"] and state in ("WARNING", "ALARM"):
            if l2_cooldown.can_trigger(asset_id, point, state):
                l2_queue.enqueue({
                    "asset": asset_id,
                    "point": point,
                    "l1_snapshot": l1_features,
                    "publisher": publisher,
                })
                l2_cooldown.mark_triggered(asset_id, point)

        # --- Recommendation / Final Alarm ---
        recommendation = recommendation_engine.recommend(fault_type=fault_type, state=state, lang="id")
        publisher.publish_recommendation(
            asset_id,
            point,
            {
                "state": state,
                "fault_type": fault_type,
                "rec_level": recommendation.get("level"),
                "rec_priority": recommendation.get("priority"),
                "rec_action_code": recommendation.get("action_code"),
                "rec_text": recommendation.get("text"),
                "timestamp": time.time(),
            },
        )

        # --- Heartbeat ---
        now = time.time()
        if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL:
            publisher.publish_heartbeat(heartbeat.snapshot())
            last_heartbeat_ts = now

    start_mqtt_listener(
        callback=on_raw_data,
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
        topic=config["mqtt"]["raw_topic"],
    )


if __name__ == "__main__":
    main()



