# diagnostic_l2/fault_rules.py

"""
FAULT TAXONOMY RULES (L2)
------------------------
Maps severity & features into fault type + recommendation
"""

# =========================================
# PUBLIC API
# =========================================
def evaluate(result: dict, features: dict):
    """
    Evaluate fault taxonomy based on severity result & features
    Return dict or None
    """

    state = result.get("state", "NORMAL")
    severity_rules = result.get("severity_rules", [])
    dominant = result.get("dominant_feature")

    vel = features.get("overall_vel_rms_mm_s")
    env = features.get("envelope_rms")

    # =====================================
    # BEARING FAULT
    # =====================================
    if env is not None and env > 0.35:
        return {
            "fault_code": "BEARING_DEGRADATION",
            "fault_category": "BEARING",
            "fault_description": "Elevated bearing envelope vibration detected",
            "recommendation": _bearing_recommendation(state),
            "action_priority": _priority_from_state(state),
        }

    # =====================================
    # MISALIGNMENT
    # =====================================
    if vel is not None and vel > 4.5 and dominant == "overall_vel_rms_mm_s":
        return {
            "fault_code": "MISALIGNMENT",
            "fault_category": "ALIGNMENT",
            "fault_description": "High overall vibration indicative of shaft misalignment",
            "recommendation": _misalignment_recommendation(state),
            "action_priority": _priority_from_state(state),
        }

    # =====================================
    # MECHANICAL LOOSENESS
    # =====================================
    if vel is not None and vel > 6.0 and "ISO_20816_ZONE_C" in severity_rules:
        return {
            "fault_code": "MECHANICAL_LOOSENESS",
            "fault_category": "MECHANICAL",
            "fault_description": "Excessive vibration possibly caused by looseness",
            "recommendation": _looseness_recommendation(state),
            "action_priority": _priority_from_state(state),
        }

    return None


# =========================================
# INTERNAL HELPERS
# =========================================
def _priority_from_state(state: str) -> str:
    return {
        "ALARM": "HIGH",
        "WARNING": "MEDIUM",
        "NORMAL": "LOW",
    }.get(state, "LOW")


def _bearing_recommendation(state: str) -> str:
    if state == "ALARM":
        return "Inspect bearing immediately and prepare replacement"
    if state == "WARNING":
        return "Monitor bearing condition and plan inspection"
    return "Bearing condition normal"


def _misalignment_recommendation(state: str) -> str:
    if state == "ALARM":
        return "Perform alignment check and corrective maintenance"
    if state == "WARNING":
        return "Schedule alignment verification"
    return "Alignment condition normal"


def _looseness_recommendation(state: str) -> str:
    if state == "ALARM":
        return "Inspect foundation, bolts, and structural integrity immediately"
    if state == "WARNING":
        return "Check mechanical fastening and mounting"
    return "Mechanical structure stable"

