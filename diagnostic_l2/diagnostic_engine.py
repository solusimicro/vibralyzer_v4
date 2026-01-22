# diagnostic_l2/diagnostic_engine.py

from diagnostic_l2.fault_rules import FAULT_RULES


class DiagnosticEngine:
    def diagnose(self, features: dict, state: str):
        """
        Return standardized fault_type
        """
        for rule in FAULT_RULES:
            if rule["severity"] != state:
                continue

            match = True
            for feat, cond in rule["conditions"].items():
                if feat not in features:
                    match = False
                    break

                value = features[feat]
                if cond == ">" and value <= 0:
                    match = False
                if cond == "<" and value >= 5:
                    match = False

            if match:
                return rule["fault_type"]

        return "GENERAL_HEALTH"


