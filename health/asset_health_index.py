def compute_asset_health(point_health_list: list[dict]) -> dict:
    """
    Aggregate multiple point health into asset-level health.
    Uses worst-case severity rule.
    """

    if not point_health_list:
        return {
            "phi": 0.0,
            "state": "UNKNOWN",
        }

    worst_point = max(point_health_list, key=lambda x: x["phi"])
    asset_phi = worst_point["phi"]
    asset_state = worst_point["state"]

    return {
        "phi": asset_phi,
        "state": asset_state,
        "source_point": worst_point.get("point_id"),
    }
