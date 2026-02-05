class DiagnosticEngine:
    def run(self, l1_snapshot):
        rules_triggered = []
        metrics = {}

        features = l1_snapshot.get("features", {})

        vel = features.get("overall_vel_rms_mm_s")
        env = features.get("envelope_rms")

        # === SEVERITY RULES ONLY ===
        if vel is not None and vel > 7.1:
            rules_triggered.append("ISO_20816_ZONE_C")
            metrics["overall_vel_rms_mm_s"] = vel

        if env is not None and env > 0.35:
            rules_triggered.append("ENVELOPE_HIGH")
            metrics["envelope_rms"] = env

        state = self._state_from_rules(rules_triggered)

        return {
            "state": state,
            "confidence": self._confidence(rules_triggered),
            "dominant_feature": self._dominant_feature(metrics),
            "severity_rules": rules_triggered,
            "metrics": metrics,
        }

    # =========================
    # INTERNAL HELPERS
    # =========================

    def _state_from_rules(self, rules):
        if "ISO_20816_ZONE_C" in rules:
            return "ALARM"
        if rules:
            return "WARNING"
        return "NORMAL"

    def _confidence(self, rules):
        if not rules:
            return 0.9
        return min(1.0, 0.6 + 0.2 * len(rules))

    def _dominant_feature(self, metrics):
        if not metrics:
            return None
        return max(metrics, key=lambda k: metrics[k])


