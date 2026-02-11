import time

from config.config_loader import load_config
from core.ring_buffer import RingBufferManager
from core.l1_feature_pipeline import L1FeaturePipeline

from early_fault.trend_detector import TrendDetector
from early_fault.persistence import PersistenceChecker
from early_fault.scoring import EarlyFaultFSM
from early_fault.baseline import AdaptiveBaseline

from health.point_health_index import compute_phi
from health.state_mapping import phi_to_state

from analytics.recommendation.recommendation_engine import RecommendationEngine

from diagnostic_l2.l2_queue import L2JobQueue
from diagnostic_l2.worker import l2_worker

from publish.mqtt_publisher import MQTTPublisher
from raw_ingest.mqtt_listener import start_mqtt_listener


# =========================================================
# MAIN
# =========================================================
def main():

    # -----------------------------------------------------
    # LOAD CONFIG
    # -----------------------------------------------------
    system_cfg = load_config("config/system.yaml")
    topology_cfg = load_config("config/config.yaml")

    mqtt_cfg = system_cfg["mqtt"]
    raw_cfg = system_cfg["raw"]
    l1_cfg = system_cfg["l1_feature"]
    early_cfg = system_cfg["early_fault"] 
    l2_cfg = system_cfg["l2"]

    # -----------------------------------------------------
    # INIT CORE COMPONENTS
    # -----------------------------------------------------
    ring_buffer = RingBufferManager(
        window_size=raw_cfg["window_size"]
    )

    publisher = MQTTPublisher(
        broker=mqtt_cfg["broker"],
        port=mqtt_cfg["port"],
    )

    recommendation_engine = RecommendationEngine()

    l2_queue = L2JobQueue()

    if l2_cfg.get("enable", True):
        l2_queue.start(l2_worker)

    # -----------------------------------------------------
    # PER POINT ENGINE REGISTRY (MULTI-SITE SAFE)
    # -----------------------------------------------------
    engines = {}

    def get_point_engine(site, asset, point):

        key = f"{site}.{asset}.{point}"

        if key in engines:
            return engines[key]

        # Safe RPM lookup
        rpm = topology_cfg.get("points", {}).get(point, {}).get(
            "rpm",
            l1_cfg.get("rpm_default", 3000)
        )

        engine = {
            "baseline": AdaptiveBaseline(),
            "trend": TrendDetector(),
            "persistence": PersistenceChecker(),
            "fsm": EarlyFaultFSM(),
            "l1": L1FeaturePipeline(
                fs=l1_cfg["sampling_rate"],
                rpm=rpm,
            ),
        }

        engines[key] = engine
        return engine

    # -----------------------------------------------------
    # RAW MESSAGE CALLBACK
    # -----------------------------------------------------
    def on_raw_message(site_id, asset_id, point, raw_payload):

        # 1️⃣ Ring Buffer
        ring_buffer.add(asset_id, point, raw_payload)

        if not ring_buffer.is_window_ready(asset_id, point):
            return

        window = ring_buffer.get_window(asset_id, point)

        # 2️⃣ Get Engine
        engine = get_point_engine(site_id, asset_id, point)

        # 3️⃣ L1 Feature
        features = engine["l1"].compute(window)

        publisher.publish_l1(
            site=site_id,
            asset=asset_id,
            point=point,
            payload=features,
        )

        # 4️⃣ Early Fault (Internal Only)
        # Trend detection (raw RMS domain)
        trend = engine["trend"].update(
            asset_id,
            point,
            features
        )

        # Baseline update (adaptive, ISO style)
        engine["baseline"].update(
            asset_id,
            point,
            features,
            allow_update=(trend.level == "NORMAL")
        )

        # Persistence logic
        persistence = engine["persistence"].update(
            asset_id,
            point,
            trend
        )

        # FSM state
        early_fault = engine["fsm"].update(
            asset=asset_id,
            point=point,
            trend=trend,
            persistence=persistence,
        )

        fsm_state = early_fault.state.value
        confidence = early_fault.confidence


        # 5️⃣ PHI (Authority Layer)
        phi = compute_phi(features)

        state, fault_type, confidence = phi_to_state(
            phi,
            fsm_state,
        )

        publisher.publish_health(
            site=site_id,
            asset=asset_id,
            point=point,
            payload={
                "point_health_index": phi,
                "state": state,
                "fault_type": fault_type,
                "confidence": confidence,
                "fsm_state": fsm_state,
                "timestamp": time.time(),
            },
        )

        # 6️⃣ L2 (Async Internal)
        if state in ("WARNING", "ALARM") and l2_cfg.get("enable", True):
           l2_queue.add_job({
                "site": site_id,
                "asset": asset_id,
                "point": point,
                "l1_snapshot": features,
                "publisher": publisher,
            })

        # 7️⃣ Recommendation (Final Layer)
        recommendation = recommendation_engine.recommend(
            state=state,
            fault_type=fault_type,
            confidence=confidence,
            phi=phi,
        )

        publisher.publish_recommendation(
            site=site_id,
            asset=asset_id,
            point=point,
            payload=recommendation,
        )

    # -----------------------------------------------------
    # START RAW LISTENER
    # -----------------------------------------------------
    start_mqtt_listener(
        callback=on_raw_message,
        broker=mqtt_cfg["broker"],
        port=mqtt_cfg["port"],
        topic=mqtt_cfg["raw_topic"],
    )

    print("🚀 Vibralyzer v4 Multi-Site Engine Started")

    # Keep alive
    while True:
        time.sleep(1)


# =========================================================
if __name__ == "__main__":
    main()
