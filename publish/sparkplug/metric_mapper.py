import yaml
from pathlib import Path
from sparkplug_b import MetricDataType, new_metric


class SparkplugMetricMapper:
    """
    Profile-based Sparkplug metric mapper
    """

    TYPE_MAP = {
        "float": MetricDataType.Float,
        "int": MetricDataType.Int32,
        "int64": MetricDataType.Int64,
        "bool": MetricDataType.Boolean,
        "string": MetricDataType.String,
    }

    def __init__(self, profile_file: str | None = None):
        if profile_file is None:
            profile_file = Path(__file__).parents[2] / "config" / "sparkplug_metric_profile.yaml"

        with open(profile_file, "r") as f:
            self.profile = yaml.safe_load(f)

        self.metrics_cfg = self.profile.get("metrics", {})

    def build_metrics(self, values: dict) -> list:
        """
        values = {
            "PHI": 87.2,
            "STATE": 2,
            "FAULT_CODE": 21,
            ...
        }
        """

        metrics = []

        for name, cfg in self.metrics_cfg.items():
            if name not in values:
                continue  # hemat tag, no spam

            datatype = self.TYPE_MAP[cfg["datatype"]]

            metrics.append(
                new_metric(
                    name=name,
                    datatype=datatype,
                    value=values[name],
                )
            )

        return metrics

