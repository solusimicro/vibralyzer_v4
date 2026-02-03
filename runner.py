import time

from raw_ingest.mqtt_listener import start_mqtt_listener
from core.ring_buffer import RingBufferManager
from core.l1_feature_pipeline import L1FeaturePipeline

from early_fault.trend_detector import TrendDetector
from early_fault.persistence import PersistenceChecker
from early_fault.scoring import EarlyFaultFSM
from early_fault.baseline import AdaptiveBaseline

from publish.mqtt_publisher import MQTTPublisher
from publish.sparkplug.sparkplug_publisher import SparkplugPublisher
from publish.sparkplug.metric_mapper import build_scada_metrics

from config.config_loader import load_config

from diagnostic_l2.cooldown import L2CooldownManager
from diagnostic_l2.l2_queue import L2JobQueue
from diagnostic_l2.worker import l2_worker

from analytics.interpretation.interpretation_engine import InterpretationEngine
from analytics.recommendation.recommendation_engine import RecommendationEngine

from utils.heartbeat import Heartbeat


# ==================================================
# PHI → STATE (FINAL AUTHORITY)
# ==================================================
def phi_to_state(phi: float) -> str:
    if phi >= 90:
        return "NORMAL"
    elif phi >= 75:
        return "WATCH"
    elif phi >= 55:
        return "WARNING"
    else:
        return "ALARM"


# ==================================================
# POINT HEALTH INDEX (PHYSICS-BASED)
# ==================================================
def compute_point_health_index(l1_features):
    vel = min(l1_features["overall_vel_rms_mm_s"] / 7.1, 1.0)
    env = min(l1_features["envelope_rms"] / 0.35, 1.0)
    crest = min(l1_features["crest_factor"] / 6.0, 1.0)

    severity = 0.5 * vel + 0.3 * env + 0.2 * crest
    phi = 100.0 * (1.0 - severity)

    return round(max(min(phi, 100.0), 0.0), 1)


def main():
    # =========================
    # LOAD CONFIG
    # =========================
    config = load_config()

    # =========================
    # ENGINES
    # =========================
    interpretation_engine = InterpretationEngine()
    recommendation_engine = RecommendationEngine()

    # =========================
    # HEARTBEAT
    # =========================
    heartbeat = Heartbeat(service_name="vibralyzer")
    last_heartbeat_ts = time.time()
    HEARTBEAT_INTERVAL = config.get("heartbeat", {}).get("interval_sec", 10)

    # =========================
    # BASELINE
    # =========================
    baseline = AdaptiveBaseline(
        alpha=config.get("baseline", {}).get("alpha", 0.01),
        min_samples=config.get("baseline", {}).get("min_samples", 100),
    )

    # =========================
    # L2 SYSTEM
    # =========================
    l2_cooldown = L2CooldownManager(
        warning_sec=config["l2"]["cooldown_warning_sec"],
        alarm_sec=config["l2"]["cooldown_alarm_sec"],
    )

    l2_queue = L2JobQueue(maxsize=10)
    l2_queue.start(l2_worker)

    # =========================
    # CORE PIPELINE
    # =========================
    ring_buffers = RingBufferManager(
        window_size=config["raw"]["window_size"]
    )

    l1_pipeline = L1FeaturePipeline(
        fs=config["l1_feature"]["sampling_rate"],
        rpm=config["l1_feature"]["rpm_default"],
    )

    trend_detector = TrendDetector()
    persistence_checker = PersistenceChecker()

    early_fault_fsm = EarlyFaultFSM(
        watch_persistence=config["early_fault"]["watch_persistence"],
        warning_persistence=config["early_fault"]["warning_persistence"],
        alarm_persistence=config["early_fault"]["alarm_persistence"],
        hysteresis_clear=config["early_fault"]["hysteresis_clear"],
    )

    # =========================
    # PUBLISHERS
    # =========================
    publisher = MQTTPublisher(
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
    )

    sparkplug = SparkplugPublisher(
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
        group_id=config["sparkplug"]["group_id"],
        edge_node="VIBRALYZER_EDGE",
    )

    # =========================
    # RAW CALLBACK
    # =========================
    def on_raw_data(asset_id, point, raw_payload):
        nonlocal last_heartbeat_ts

        # ---- RAW RX ----
        heartbeat.mark_raw_rx()
        ring_buffers.append(asset_id, point, raw_payload)

        if not ring_buffers.is_window_ready(asset_id, point):
            return

        heartbeat.mark_window_ready()
        window = ring_buffers.get_window(asset_id, point)

        # ---- L1 FEATURES ----
        heartbeat.mark_l1_exec()
        l1_features = l1_pipeline.compute(window)

        # ---- TREND ----
        raw_trend = trend_detector.update(asset_id, point, l1_features)

        # ---- BASELINE ----
        baseline.update(
            asset_id,
            point,
            l1_features,
            allow_update=(raw_trend.level == "NORMAL"),
        )

        # ---- PERSISTENCE ----
        persistence = persistence_checker.update(asset_id, point, raw_trend)

        # ---- EARLY FAULT FSM (EVIDENCE ONLY) ----
        early_fault = early_fault_fsm.update(
            asset=asset_id,
            point=point,
            trend=raw_trend,
            persistence=persistence,
        )

        # ---- FINAL HEALTH (PHI) ----
        point_health_index = compute_point_health_index(l1_features)
        state = phi_to_state(point_health_index)

        # ---- FAULT TYPE (ENGINEERING CONTEXT) ----
        if state in ("NORMAL", "WATCH"):
            fault_type = "GENERAL_HEALTH"
        else:
            fault_type = early_fault.dominant_feature or "GENERAL_HEALTH"

        # ---- INTERPRETATION ----
        interpretation = interpretation_engine.interpret(
            asset=asset_id,
            point=point,
            l1_features=l1_features,
            trend=raw_trend,
            early_fault=early_fault,
            phi=point_health_index,
            state=state,
        )

        # ---- RECOMMENDATION ----
        recommendation = recommendation_engine.recommend(
            fault_type=fault_type,
            state=state,
            lang="id",
        )

        # ==================================================
        # SCADA JSON (LEGACY / DEBUG ONLY)
        # ==================================================
        publisher.publish_scada(
            asset_id,
            point,
            {
                "asset": asset_id,
                "point": point,

                "acceleration_rms_g": l1_features["acc_rms_g"],
                "acc_peak_g": l1_features["acc_peak_g"],
                "acc_hf_rms_g": l1_features["acc_hf_rms_g"],
                "crest_factor": l1_features["crest_factor"],
                "envelope_rms": l1_features["envelope_rms"],
                "overall_vel_rms_mm_s": l1_features["overall_vel_rms_mm_s"],

                "energy_low": l1_features["energy_low"],
                "energy_high": l1_features["energy_high"],
                "temperature_c": raw_payload.get("temperature"),

                "point_health_index": point_health_index,
                "state": state,  # PHI ONLY
            },
        )

        # ==================================================
        # SPARKPLUG SCADA (OFFICIAL)
        # ==================================================
        if config.get("sparkplug", {}).get("enable", True):
            metrics = build_scada_metrics(
                l1_features,
                raw_payload,
                point_health_index,
                state,
            )

            # Contract safety
            assert 0 <= metrics["point_health_index"] <= 100

            sparkplug.publish_ddata(
                asset_id,
                point,
                metrics,
            )

        # ==================================================
        # EVENTS
        # ==================================================
        publisher.publish_early_fault(
            asset_id,
            point,
            {
                "asset": asset_id,
                "point": point,
                "fsm_state": early_fault.state.value,
                "confidence": early_fault.confidence,
                "fault_type": fault_type,
                "timestamp": early_fault.timestamp,
            },
        )

        publisher.publish_health_alarm(
            asset_id,
            point,
            {
                "asset": asset_id,
                "point": point,
                "state": state,
                "point_health_index": point_health_index,
                "timestamp": time.time(),
            },
        )

        publisher.publish_interpretation(
            asset_id,
            point,
            interpretation,
        )

        publisher.publish_recommendation(
            asset_id,
            point,
            {
                "asset": asset_id,
                "point": point,
                "state": state,
                "fault_type": fault_type,
                "rec_level": recommendation.get("level"),
                "rec_priority": recommendation.get("priority"),
                "rec_action_code": recommendation.get("action_code"),
                "rec_text": recommendation.get("text"),
                "timestamp": time.time(),
            },
        )

        # ==================================================
        # L2 TRIGGER
        # ==================================================
        if config["l2"]["enable"] and state in ("WARNING", "ALARM"):
            if l2_cooldown.can_trigger(asset_id, point, state):
                l2_queue.enqueue(
                    {
                        "asset": asset_id,
                        "point": point,
                        "window": window,
                        "early_fault_event": {
                            "fsm_state": early_fault.state.value,
                            "fault_type": fault_type,
                            "confidence": early_fault.confidence,
                        },
                        "health_event": {
                            "state": state,
                            "point_health_index": point_health_index,
                        },
                        "publisher": publisher,
                    }
                )
                heartbeat.mark_l2_exec()
                l2_cooldown.mark_triggered(asset_id, point)

        # ==================================================
        # HEARTBEAT
        # ==================================================
        now = time.time()
        if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL:
            publisher.publish_heartbeat(heartbeat.snapshot())
            last_heartbeat_ts = now

    # =========================
    # START MQTT LISTENER
    # =========================
    start_mqtt_listener(
        callback=on_raw_data,
        broker=config["mqtt"]["broker"],
        port=config["mqtt"]["port"],
        topic=config["mqtt"]["raw_topic"],
    )


if __name__ == "__main__":
    main()

# ==================================================
# NOTES
# - FSM provides evidence only
# - SCADA alarm/state derived ONLY from PHI
# - Sparkplug is the single source of truth for SCADA
# ==================================================
