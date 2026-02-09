# health/state_mapping.py

def phi_to_state(phi: float) -> str:
    """
    Health state mapping.
    Single source of truth for alarm state.
    """

    if phi >= 90:
        return "NORMAL"
    elif phi >= 75:
        return "WATCH"
    elif phi >= 55:
        return "WARNING"
    else:
        return "ALARM"
