def phi_to_state(phi, fsm_state=None):
    """
    Map PHI (0–1) to ISO state.
    FSM can optionally influence fault_type / confidence.
    """

    # --- ISO Base Mapping (PHI authority)
    if phi < 0.3:
        state = "NORMAL"
    elif phi < 0.6:
        state = "WATCH"
    elif phi < 0.8:
        state = "WARNING"
    else:
        state = "ALARM"

    # --- Default outputs
    fault_type = "UNKNOWN"
    confidence = float(phi)

    # --- Optional FSM refinement
    if fsm_state is not None:

        if state in ("WARNING", "ALARM"):
            fault_type = fsm_state

        # Confidence boost if FSM confirms
        if fsm_state in ("WARNING", "ALARM"):
            confidence = min(1.0, phi + 0.1)

    return state, fault_type, confidence

