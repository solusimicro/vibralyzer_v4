import time

class InterpretationEngine:
    def interpret(
        self,
        asset: str,
        point: str,
        l1_features: dict,
        trend,
        early_fault,
        phi: float,
        state: str,
    ) -> dict:

        dominant = early_fault.dominant_feature

        supporting = []

        if dominant == "energy_high":
            supporting.append({
                "name": "energy_high",
                "value": l1_features.get("energy_high"),
                "unit": "g²",
                "trend_level": trend.level,
            })

            supporting.append({
                "name": "envelope_rms",
                "value": l1_features.get("envelope_rms"),
                "unit": "g",
                "trend_level": trend.level,
            })

            suspected_faults = [
                "Bearing outer race defect",
                "Poor lubrication",
            ]

            component = f"Bearing – {point}"
            summary = (
                "High-frequency energy dominates vibration spectrum, "
                "indicating bearing-related degradation."
            )

        else:
            suspected_faults = ["General mechanical degradation"]
            component = "Rotating assembly"
            summary = "General mechanical degradation detected."

        return {
            "asset": asset,
            "point": point,

            "interpretation": {
                "summary": summary,
                "suspected_faults": suspected_faults,
                "suspected_component": component,
                "supporting_features": supporting,
                "reasoning": [
                    f"Dominant feature: {dominant}",
                    f"Trend level: {trend.level}",
                    f"PHI: {phi}",
                ],
                "confidence": early_fault.confidence,
            },

            "context": {
                "phi": phi,
                "state": state,
                "fsm_state": early_fault.state.value,
                "dominant_feature": dominant,
            },

            "timestamp": time.time(),
        }
