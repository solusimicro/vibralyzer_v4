def validate_raw_payload(payload: dict) -> bool:
    required = ["asset_id", "point", "timestamp", "acceleration", "temperature", "speed"]
    for key in required:
        if key not in payload:
            return False

    if len(payload["acceleration"]) < 1024:
        return False

    return True
