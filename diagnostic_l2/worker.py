from diagnostic_l2.diagnostic_engine import DiagnosticEngine
from diagnostic_l2 import fault_rules, fallback, cooldown
from diagnostic_l2.feature_extractor import extract_features

engine = DiagnosticEngine()

def l2_worker(job: dict) -> dict:
    # L1
    features = extract_features(job)

    l1_snapshot = {
        "asset": job["asset"],
        "point": job["point"],
        "features": features,
    }

    # L2
    result = engine.run(l1_snapshot)

    fault = fault_rules.evaluate(result, features)
    if fault:
        result.update(fault)

    fallback.apply(result)
    cooldown.apply(result, job["asset"], job["point"])

    return result


