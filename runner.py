import time
import logging

from diagnostic_l2.worker import l2_worker
from publish.sparkplug.sparkplug_publisher import SparkplugPublisher

log = logging.getLogger("L2_RUNNER")
logging.basicConfig(level=logging.INFO)


class L2Runner:
    def __init__(self, publisher: SparkplugPublisher):
        self.publisher = publisher

    def run_job(self, job: dict):
        """
        Runner = orchestrator only
        - Call worker
        - Validate result
        - Publish if valid
        """

        result = l2_worker(job)

        if not result:
            log.warning("⚠️ L2 result is empty")
            return

        asset = result.get("asset")
        point = result.get("point")

        if not asset or not point:
            log.error("❌ Missing asset / point in L2 result")
            return

        metrics = result.get("metrics")
        if not metrics:
            log.info(f"ℹ️ No metrics to publish for {asset}:{point}")
            return

        # ✅ SINGLE responsibility: publish Sparkplug DDATA
        self.publisher.publish_ddata(
            asset=asset,
            point=point,
            metrics=metrics,
        )

        log.info(
            f"📡 Published DDATA | asset={asset} point={point} metrics={len(metrics)}"
        )


# ==================================================
# ENTRYPOINT (example usage / service loop)
# ==================================================
if __name__ == "__main__":
    publisher = SparkplugPublisher(
        broker="localhost",
        port=1883,
        group_id="VIBRA",
        edge_node="EDGE_01",
    )

    runner = L2Runner(publisher)

    heartbeat_counter = 0

    while True:
        # contoh dummy job
        job = {
            "asset": "PUMP_01",
            "point": "DE",
            "window": 5,
            "early_fault_event": True,
            "publisher": publisher,
        }

        runner.run_job(job)

        # ==========================================
        # L2 HEARTBEAT (TeslaSCADA health metric)
        # ==========================================
        heartbeat_counter += 1
        if heartbeat_counter % 2 == 0:
            # tiap 10 detik (sleep 5)
            publisher.publish_l2_alive()
            log.debug("💓 L2 heartbeat published")

        time.sleep(5)
