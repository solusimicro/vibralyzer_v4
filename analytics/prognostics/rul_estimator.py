# analytics/prognostics/rul_estimator.py
import numpy as np


class RULEstimator:
    def __init__(self, limit_value: float):
        """
        limit_value: ISO threshold (e.g velocity mm/s)
        """
        self.limit = limit_value

    def estimate(self, history: list[dict]) -> dict:
        """
        history item:
        {
            "timestamp": float,
            "value": float
        }
        """

        if len(history) < 6:
            return {
                "rul_days": None,
                "confidence": 0.0,
                "method": "insufficient_data",
            }

        t = np.array([h["timestamp"] for h in history])
        v = np.array([h["value"] for h in history])

        # normalize time to days
        t = (t - t[0]) / 86400.0

        slope, intercept = np.polyfit(t, v, 1)

        # stable or improving
        if slope <= 0:
            return {
                "rul_days": None,
                "confidence": 0.4,
                "degradation_rate": round(slope, 5),
                "method": "stable_trend",
            }

        remaining = (self.limit - v[-1]) / slope

        return {
            "rul_days": max(0, round(remaining, 1)),
            "confidence": min(1.0, len(history) / 30),
            "degradation_rate": round(slope, 5),
            "method": "linear_extrapolation",
        }
