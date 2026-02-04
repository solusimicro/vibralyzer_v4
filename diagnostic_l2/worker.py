import logging
logger = logging.getLogger("L2_WORKER")

def l2_worker(job: dict):
    asset = job["asset"]
    point = job["point"]

    logger.info(f"🔍 L2 processing | asset={asset} point={point}")

    # contoh result
    result = {
        "early_fault": True,
        "state": "ALARM",
        "confidence": 0.95
    }

    logger.info(f"✅ L2 result ready | asset={asset} point={point} keys={list(result.keys())}")
    return result
