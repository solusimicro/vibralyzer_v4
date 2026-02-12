import logging

logger = logging.getLogger(__name__)


def l2_worker(job):
    """
    Industrial L2 worker.
    Uses event timestamp and full context.
    """

    site = job["site"]
    asset = job["asset"]
    point = job["point"]
    features = job["features"]
    publisher = job["publisher"]
    state = job["state"]
    phi = job["phi"]
    event_ts = job["timestamp"]

    # ------------------------------
    # Diagnostic Logic (Replace later)
    # ------------------------------
    fault_type = "GEAR_WEAR"
    confidence = 0.82

    payload = {
        "fault_type": fault_type,
        "state": state,
        "phi": phi,
        "confidence": confidence,
        "timestamp": event_ts,
    }

    publisher.publish_diagnostic(
        site=site,
        asset=asset,
        point=point,
        payload=payload,
    )




