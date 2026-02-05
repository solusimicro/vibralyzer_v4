# diagnostic_l2/fallback.py

"""
FALLBACK NORMALIZATION (L2)
--------------------------
Ensures Sparkplug metrics are ALWAYS complete
"""

def apply(result: dict):
    # =========================
    # CORE METRICS (ALWAYS)
    # =========================
    result.setdefault("state", "NORMAL")
    result.setdefault("confidence", 0.9)

    # =========================
    # FAULT METRICS (NEUTRAL)
    # =========================
    result.setdefault("fault_code", "NO_FAULT")
    result.setdefault("fault_category", "NONE")
    result.setdefault("fault_description", "No abnormal condition detected")

    # =========================
    # ACTION METRICS
    # =========================
    result.setdefault("recommendation", "No action required")
    result.setdefault("action_priority", "LOW")

    # =========================
    # CONTEXT / TRACEABILITY
    # =========================
    result.setdefault("dominant_feature", None)
    result.setdefault("severity_rules", [])
    result.setdefault("metrics", {})
    result.setdefault("cooldown_active", False)

    return result


