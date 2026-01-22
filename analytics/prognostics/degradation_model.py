# analytics/prognostics/degradation_model.py

FAULT_BASED_DEGRADATION = {
    "BEARING_DEGRADATION": {
        "WARNING": {"rate_per_day": 0.05, "rul_days": 60},
        "ALARM":   {"rate_per_day": 0.12, "rul_days": 15},
    },
    "IMBALANCE": {
        "WARNING": {"rate_per_day": 0.02, "rul_days": 120},
    },
    "MISALIGNMENT": {
        "WARNING": {"rate_per_day": 0.03, "rul_days": 90},
    },
    "LOOSENESS": {
        "ALARM": {"rate_per_day": 0.15, "rul_days": 10},
    },
    "GENERAL_HEALTH": {
        "NORMAL": {"rate_per_day": 0.0, "rul_days": 9999},
    },
}
