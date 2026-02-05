import time
from publish.sparkplug.sparkplug_b import MetricWrapper

class MetricMapper:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def _now(self):
        return int(time.time() * 1000)

    def build_runtime_metrics(self, l2_values: dict):
        metrics = []

        ts = self._now()

        # =========================
        # STATE
        # =========================
        metrics.append(
            MetricWrapper(
                name="State",
                value=l2_values.get("state", "NORMAL"),
                timestamp=ts
            )
        )

        # =========================
        # CONFIDENCE
        # =========================
        if "confidence" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="Confidence",
                    value=float(l2_values["confidence"]),
                    timestamp=ts
                )
            )

        # =========================
        # FAULT
        # =========================
        if "fault_code" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="FaultCode",
                    value=l2_values["fault_code"],
                    timestamp=ts
                )
            )

        if "fault_description" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="FaultDescription",
                    value=l2_values["fault_description"],
                    timestamp=ts
                )
            )
            
        if "fault_category" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="FaultCategory",
                    value=l2_values["fault_category"],
                    timestamp=ts
                )
            )

        # =========================
        # RECOMMENDATION
        # =========================
        if "recommendation" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="Recommendation",
                    value=l2_values["recommendation"],
                    timestamp=ts
                )
            )
 
        if "action_priority" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="ActionPriority",
                    value=l2_values["action_priority"],
                    timestamp=ts
                )
            )

        if "etf_days" in l2_values:
            metrics.append(
                MetricWrapper(
                    name="EstimatedDaysToFailure",
                    value=int(l2_values["etf_days"]),
                    timestamp=ts
                )
            )

        return metrics

