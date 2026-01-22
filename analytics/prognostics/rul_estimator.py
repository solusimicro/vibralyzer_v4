# analytics/prognostics/rul_estimator.py

from analytics.prognostics.degradation_model import FAULT_BASED_DEGRADATION


class RULEstimator:
    def estimate(self, fault_type: str, state: str, confidence: float = 1.0):
        """
        Return RUL estimation in hours
        """
        fault_cfg = FAULT_BASED_DEGRADATION.get(fault_type, {})
        state_cfg = fault_cfg.get(state)

        if not state_cfg:
            return {
                "rul_hours": None,
                "confidence": 0.0,
                "method": "N/A",
            }

        base_days = state_cfg["rul_days"]

        # Confidence-adjusted RUL (higher confidence → lower RUL)
        adj_days = base_days * max(0.3, 1.0 - confidence)

        return {
            "rul_hours": int(adj_days * 24),
            "confidence": confidence,
            "method": "fault_rule_based",
        }
