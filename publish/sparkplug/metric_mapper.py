import yaml
from .sparkplug_b_pb2 import Metric


class MetricMapper:
    """
    Convert python values → Sparkplug Metric
    Driven by mapping.yaml
    Sparkplug B + TeslaSCADA compliant
    """

    TYPE_MAP = {
        "Int8": Metric.Int8,
        "Int16": Metric.Int16,
        "Int32": Metric.Int32,
        "Int64": Metric.Int64,
        "UInt8": Metric.UInt8,
        "UInt16": Metric.UInt16,
        "UInt32": Metric.UInt32,
        "UInt64": Metric.UInt64,
        "Float": Metric.Float,
        "Double": Metric.Double,
        "Boolean": Metric.Boolean,
        "String": Metric.String,
    }

    def __init__(self, mapping_file: str):
        with open(mapping_file, "r") as f:
            self.mapping = yaml.safe_load(f)

    # ==================================================
    # INTERNAL
    # ==================================================
    def _build_metric(self, value, definition):
        """
        definition example:
        {
          name: Vibration/Velocity/RMS
          sparkplug_type: Float
          engineering_unit: mm/s
          description: Overall vibration velocity RMS
        }
        """
        metric = Metric()

        # 🔑 SCADA NAME (NOT python key)
        metric.name = definition["name"]

        sparkplug_type = definition["sparkplug_type"]
        metric.datatype = self.TYPE_MAP[sparkplug_type]

        # -------------------------------
        # VALUE ASSIGNMENT
        # -------------------------------
        if metric.datatype == Metric.Boolean:
            metric.boolean_value = bool(value)

        elif metric.datatype == Metric.String:
            metric.string_value = str(value)

        elif metric.datatype == Metric.Float:
            metric.float_value = float(value)

        elif metric.datatype == Metric.Double:
            metric.double_value = float(value)

        else:
            metric.long_value = int(value)

        # -------------------------------
        # METADATA (TeslaSCADA friendly)
        # -------------------------------
        if "engineering_unit" in definition:
            metric.properties["engUnit"].string_value = str(
                definition["engineering_unit"]
            )

        if "description" in definition:
            metric.properties["description"].string_value = str(
                definition["description"]
            )

        return metric

    # ==================================================
    # PUBLIC
    # ==================================================
    def build_runtime_metrics(self, values: dict):
        """
        Build DDATA metrics

        values:
        {
          "overall_vel_rms_mm_s": 4.2,
          "point_health_index": 78.5,
        }
        """
        metrics = []

        for key, value in values.items():
            definition = self.mapping.get(key)
            if not definition:
                continue

            metrics.append(self._build_metric(value, definition))

        return metrics

    def build_birth_metrics(self):
        """
        Build DBIRTH metrics
        - datatype + metadata only
        - NO runtime value
        """
        metrics = []

        for definition in self.mapping.values():
            metric = Metric()
            metric.name = definition["name"]
            metric.datatype = self.TYPE_MAP[definition["sparkplug_type"]]

            if "engineering_unit" in definition:
                metric.properties["engUnit"].string_value = str(
                    definition["engineering_unit"]
                )

            if "description" in definition:
                metric.properties["description"].string_value = str(
                    definition["description"]
                )

            metrics.append(metric)

        return metrics
