from .sparkplug_b_pb2 import Metric
import time


def build_nbirth_metrics():
    now = int(time.time() * 1000)
    metrics = []

    def m(name, datatype):
        metric = Metric()
        metric.name = name
        metric.timestamp = now
        metric.datatype = datatype
        return metric

    # ---- Mandatory Sparkplug Control ----
    metrics.append(m("Node Control/Rebirth", Metric.Boolean))
    metrics[-1].boolean_value = False

    # ---- Optional but Recommended ----
    metrics.append(m("Node Info/Version", Metric.String))
    metrics[-1].string_value = "vibralyzer_v4"

    metrics.append(m("Node Info/Status", Metric.String))
    metrics[-1].string_value = "RUNNING"

    return metrics
