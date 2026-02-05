# diagnostic_l2/cooldown.py

"""
COOLDOWN MANAGER (L2)
--------------------
Prevents alarm/state flapping for Sparkplug & SCADA
"""

import time

# =========================================
# CONFIG (seconds)
# =========================================
COOLDOWN_SECONDS = {
    "ALARM": 120,     # tahan 2 menit sebelum turun
    "WARNING": 60,    # tahan 1 menit sebelum turun
}

# =========================================
# INTERNAL STATE TABLE
# =========================================
_STATE_TABLE = {}
# key = (asset, point)
# value = {
#   "state": "NORMAL|WARNING|ALARM",
#   "ts": epoch_seconds
# }

# =========================================
# PUBLIC API
# =========================================
def apply(result: dict, asset: str, point: str):
    """
    Apply cooldown logic to result in-place
    """
    now = time.time()
    key = (asset, point)

    new_state = result.get("state", "NORMAL")

    if key not in _STATE_TABLE:
        _STATE_TABLE[key] = {
            "state": new_state,
            "ts": now
        }
        return

    prev_state = _STATE_TABLE[key]["state"]
    prev_ts = _STATE_TABLE[key]["ts"]

    # -----------------------------------------
    # UPGRADE → always allowed
    # NORMAL → WARNING → ALARM
    # -----------------------------------------
    if _severity_rank(new_state) > _severity_rank(prev_state):
        _STATE_TABLE[key] = {
            "state": new_state,
            "ts": now
        }
        return

    # -----------------------------------------
    # SAME STATE → refresh timestamp
    # -----------------------------------------
    if new_state == prev_state:
        _STATE_TABLE[key]["ts"] = now
        return

    # -----------------------------------------
    # DOWNGRADE → apply cooldown
    # -----------------------------------------
    cooldown = COOLDOWN_SECONDS.get(prev_state, 0)
    elapsed = now - prev_ts

    if elapsed < cooldown:
        # BLOCK downgrade
        result["state"] = prev_state
        result["cooldown_active"] = True
    else:
        # Allow downgrade
        _STATE_TABLE[key] = {
            "state": new_state,
            "ts": now
        }
        result["cooldown_active"] = False


# =========================================
# INTERNAL HELPERS
# =========================================
def _severity_rank(state: str) -> int:
    return {
        "NORMAL": 0,
        "WARNING": 1,
        "ALARM": 2,
    }.get(state, 0)

