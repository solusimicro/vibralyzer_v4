# diagnostic_l2/fault_rules.py

FAULT_RULES = [
    {
        "fault_type": "BEARING_DEGRADATION",
        "conditions": {
            "acc_hf_rms_g": ">",
            "envelope_rms": ">",
        },
        "severity": "ALARM",
    },
    {
        "fault_type": "IMBALANCE",
        "conditions": {
            "overall_vel_rms_mm_s": ">",
            "crest_factor": "<",
        },
        "severity": "WARNING",
    },
    {
        "fault_type": "MISALIGNMENT",
        "conditions": {
            "overall_vel_rms_mm_s": ">",
            "acc_peak_g": ">",
        },
        "severity": "WARNING",
    },
    {
        "fault_type": "LOOSENESS",
        "conditions": {
            "crest_factor": ">",
        },
        "severity": "ALARM",
    },
]


