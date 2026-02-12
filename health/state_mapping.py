def phi_to_state(phi: float):
    """
    Pure industrial severity mapping.
    PHI is authority.
    """

    # PHI assumed 0–100 scale
    if phi < 30:
        state = "NORMAL"
    elif phi < 60:
        state = "WATCH"
    elif phi < 80:
        state = "WARNING"
    else:
        state = "ALARM"

    return state


