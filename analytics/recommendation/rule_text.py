import yaml
from pathlib import Path


class RecommendationEngine:
    def __init__(self):
        mapping_path = Path(__file__).parent / "mapping.yaml"
        with open(mapping_path, "r") as f:
            self.rules = yaml.safe_load(f)

    def get_text(self, fault_type: str, state: str, lang: str = "en"):
        return (
            self.rules
            .get(fault_type, {})
            .get(state, {})
            .get(lang, "")
        )
