import yaml
import time
from pathlib import Path


class RecommendationEngine:
    def __init__(self, mapping_file: str | None = None):
        if mapping_file is None:
            mapping_file = Path(__file__).parent / "mapping.yaml"

        with open(mapping_file, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f) or {}

        self.defaults = self.cfg.get("defaults", {})
        self.faults = self.cfg.get("faults", {})

    # ==========================================================
    # PUBLIC API (Stable Contract with runner)
    # ==========================================================
    def recommend(
        self,
        state: str,
        fault_type: str | None = None,
        confidence: float | None = None,
        phi: float | None = None,
        lang: str = "en",
    ) -> dict:
        """
        Unified recommendation object (FINAL CONTRACT)

        Compatible with runner:
            recommend(state=..., fault_type=..., confidence=..., phi=...)
        """

        fault_type = fault_type or "UNKNOWN"

        # --- resolve mapping ---
        fault_block = self.faults.get(fault_type, {})
        state_block = fault_block.get(state)

        if state_block:
            base = self._merge(self.defaults.get(state, {}), state_block)
        else:
            base = self.defaults.get(state, {})

        # --- fallback safety ---
        base = base or {}

        return {
            "fault_type": fault_type,
            "state": state,
            "level": base.get("level", state),
            "priority": base.get("priority", 0),
            "action_code": base.get("action_code", "NO_ACTION"),
            "text": self._pick_lang(base.get("text", {}), lang),

            # --- analytical context (from runner) ---
            "confidence": confidence,
            "phi": phi,

            "timestamp": time.time(),
        }

    # ==========================================================
    # INTERNAL HELPERS
    # ==========================================================
    @staticmethod
    def _pick_lang(text_block: dict, lang: str) -> str:
        if not isinstance(text_block, dict):
            return ""
        return text_block.get(lang) or text_block.get("en", "")

    @staticmethod
    def _merge(base: dict, override: dict) -> dict:
        """
        Shallow override merge.
        "text" field is deep merged.
        """
        result = dict(base or {})

        for k, v in (override or {}).items():
            if k == "text" and k in result and isinstance(result["text"], dict):
                merged_text = dict(result["text"])
                merged_text.update(v or {})
                result["text"] = merged_text
            else:
                result[k] = v

        return result

