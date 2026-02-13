import time

from config.config_loader import load_config
from core.ring_buffer import RingBufferManager
from core.l1_feature_pipeline import L1FeaturePipeline

from early_fault.scoring import EarlyFaultFSM
from health.point_health_index import compute_phi
from health.state_mapping import phi_to_state

from analytics.recommendation.recommendation_engine import RecommendationEngine
from diagnostic_l2.l2_queue import L2JobQueue
from diagnostic_l2.worker import l2_worker

from publish.mqtt_publisher import MQTTPublisher
from raw_ingest.mqtt_listener import start_mqtt_listener
from early_fault.baseline import AdaptiveBaseline
from early_fault.trend_detector import TrendDetector
from early_fault.persistence import PersistenceChecker

# =========================================================
# MAIN
# =========================================================
def main():

    # -----------------------------------------------------
    # LOAD CONFIG
    # -----------------------------------------------------
    system_cfg = load_config("config/system.yaml")
    topology_cfg = load_config("config/config.yaml")

    print("DEBUG topology keys:", topology_cfg.keys())

    mqtt_cfg = system_cfg["mqtt"]
    raw_cfg = system_cfg["raw"]
    l1_cfg = system_cfg["l1_feature"]
    early_cfg = system_cfg["early_fault"]
    l2_cfg = system_cfg["l2"]

    # -----------------------------------------------------
    # CORE INIT
    # -----------------------------------------------------
    ring_buffer = RingBufferManager(
        window_size=raw_cfg["window_size"]
    )

    publisher = MQTTPublisher(
        broker=mqtt_cfg["broker"],
        port=mqtt_cfg["port"]
    )

    recommendation_engine = RecommendationEngine()

    l2_queue = L2JobQueue()

    if l2_cfg.get("enable", True):
        l2_queue.start(l2_worker)

    engines = {}

    # -----------------------------------------------------
    # PER POINT ENGINE
    # -----------------------------------------------------
    def get_point_engine(site, asset, point):

        key = f"{site}.{asset}.{point}"

        if key in engines:
            return engines[key]

        try:
            rpm = (
                topology_cfg["sites"][site]
                ["assets"][asset]
                ["points"][point]["rpm"]
            )
        except KeyError as e:
            raise KeyError(
                f"Topology mismatch for {site}/{asset}/{point} → {e}"
            )

        engine = {
            "baseline": AdaptiveBaseline(),
            "trend": TrendDetector(),
            "persistence": PersistenceChecker(
                early_cfg["watch_persistence"],
                early_cfg["warning_persistence"],
                early_cfg["alarm_persistence"],
                early_cfg["hysteresis_clear"],
            ),
            "fsm": EarlyFaultFSM(),
            "l1": L1FeaturePipeline(
                fs=l1_cfg["sampling_rate"],
                rpm=rpm
            ),
        }

        engines[key] = engine
        return engine

    # -----------------------------------------------------
    # RAW CALLBACK
    # -----------------------------------------------------
    def on_raw_message(site_id, asset_id, point, raw_payload):

      # 1️⃣ Ring Buffer
      ring_buffer.add(asset_id, point, raw_payload)

      if not ring_buffer.is_window_ready(asset_id, point):
          return

      window = ring_buffer.get_window(asset_id, point)

      # 2️⃣ L1
      engine = get_point_engine(site_id, asset_id, point)
      features = engine["l1"].compute(window)

      event_ts = features["timestamp"]

      publisher.publish_l1(
          site=site_id,
          asset=asset_id,
          point=point,
          payload=features,
      )

      # 3️⃣ PHI (Authority)
      phi = compute_phi(features)
      state = phi_to_state(phi)

      # 4️⃣ HEALTH (share same timestamp)
      publisher.publish_health(
          site=site_id,
          asset=asset_id,
          point=point,
          payload={
              "phi": phi,
              "state": state,
              "timestamp": event_ts,
          },
      )

      # 5️⃣ L2 Diagnostic (Async)
      if state in ("WARNING", "ALARM") and l2_cfg.get("enable", True):

          l2_queue.enqueue({
              "site": site_id,
              "asset": asset_id,
              "point": point,
              "features": features,
              "publisher": publisher,
              "state": state,
              "phi": phi,
              "timestamp": event_ts,
          })

      # 6️⃣ Recommendation (Deterministic)
      recommendation = recommendation_engine.recommend(
          fault_type="UNKNOWN",
          state=state,
      )

      # Industrial completion
      recommendation.update({
          "phi": phi,
          "confidence": round(phi / 100, 2),
          "timestamp": event_ts,
      })

      publisher.publish_recommendation(
          site=site_id,
          asset=asset_id,
          point=point,
          payload=recommendation,
      )

    # -----------------------------------------------------
    # START LISTENER
    # -----------------------------------------------------
    start_mqtt_listener(
        callback=on_raw_message,
        broker=mqtt_cfg["broker"],
        port=mqtt_cfg["port"],
        topic=mqtt_cfg["raw_topic"],
    )

    print("🚀 Vibralyzer v4 Industrial Engine Started")

    while True:
        time.sleep(1)


# =========================================================
if __name__ == "__main__":
    main()
