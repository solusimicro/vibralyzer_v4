# analytics/recommendation/maintenance_action.py

import yaml
from pathlib import Path


class MaintenanceActionMapper:
    def __init__(self):
        mapping_file = Path(__file__).parent / "mapping.yaml"
        with open(mapping_file, "r") as f:
            self.mapping = yaml.safe_load(f)

    def get_action(self, fault_type: str, state: str, lang="en"):
        fault_cfg = self.mapping.get(fault_type)
        if not fault_cfg:
            return None

        state_cfg = fault_cfg.get(state)
        if not state_cfg:
            return None

        return {
            "priority": state_cfg["priority"],
            "action_code": state_cfg["action_code"],
            "action_text": state_cfg["action_text"].get(lang, ""),
        }
