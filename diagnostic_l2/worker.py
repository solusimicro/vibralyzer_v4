import time


def l2_worker(job):

    site = job["site"]
    asset = job["asset"]
    point = job["point"]
    publisher = job["publisher"]

    # snapshot L1
    l1_snapshot = job["l1_snapshot"]

    # ==============================
    # L2 logic (diagnostic)
    # ==============================
    result = {
        "asset": asset,
        "point": point,
        "diagnostic": "Detailed fault classification",
        "confidence": 0.82,
    }

    # ==============================
    # PUBLISH (FIXED)
    # ==============================
    publisher.publish_l2(
        site=site,
        asset=asset,
        point=point,
        payload=result,
    )



