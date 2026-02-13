def asset_recommendation(state: str) -> dict:

    mapping = {
        "NORMAL": {
            "action_code": "NO_ACTION",
            "priority": "LOW",
            "text": "Asset operating normally."
        },
        "WATCH": {
            "action_code": "MONITOR_TREND",
            "priority": "LOW",
            "text": "Monitor asset trend."
        },
        "WARNING": {
            "action_code": "SCHEDULE_INSPECTION",
            "priority": "MEDIUM",
            "text": "Inspection recommended."
        },
        "ALARM": {
            "action_code": "URGENT_MAINTENANCE",
            "priority": "HIGH",
            "text": "Urgent maintenance required."
        },
        "CRITICAL": {
            "action_code": "IMMEDIATE_SHUTDOWN_EVAL",
            "priority": "CRITICAL",
            "text": "Immediate shutdown evaluation required."
        }
    }

    rec = mapping.get(state, mapping["NORMAL"])
    rec.update({"state": state})
    return rec
