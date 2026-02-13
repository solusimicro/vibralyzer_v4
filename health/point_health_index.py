# health/point_health_index.py

def compute_phi(l1_features: dict) -> float:
    """
    Point Health Index (0–100)
    ISO 13374 – Health Assessment layer
    Deterministic, physics-based
    """

    vel = min(l1_features["overall_vel_rms_mm_s"] / 7.1, 1.0)
    env = min(l1_features["envelope_rms"] / 0.35, 1.0)
    crest = min(l1_features["crest_factor"] / 6.0, 1.0)

    severity = (
        0.5 * vel +
        0.3 * env +
        0.2 * crest
    )

    phi = 100.0 * severity  #(1.0 - severity)
    return round(max(min(phi, 100.0), 0.0), 1)
