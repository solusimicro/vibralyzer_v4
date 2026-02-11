import time
import json
import numpy as np
import paho.mqtt.publish as publish

# ==========================================================
# MQTT CONFIG
# ==========================================================
BROKER = "localhost"
PORT = 1883

FS = 25600
WINDOW = 4096
BASE_RPM = 2980
FR = BASE_RPM / 60

# ==========================================================
# MULTI SITE CONFIG
# ==========================================================
TOPOLOGY = {
    "SITE_A": {
        "PUMP_01": {
            "rpm": 2980,
        },
        "PUMP_02": {
            "rpm": 2960,
        },
    },
    "SITE_B": {
        "PUMP_03": {
            "rpm": 3000,
        }
    }
}

POINTS = {
    "P1MT": "motor",
    "P2MT": "motor",
    "P3GX": "gearbox",
    "P4GX": "gearbox",
    "P5GX": "gearbox",
    "P6GX": "gearbox",
    "P7PP": "pump",
    "P8PP": "pump",
}

# ==========================================================
# SIGNAL GENERATORS
# ==========================================================
def motor_signal(t, fr, severity):
    sig = 0.02 * np.sin(2 * np.pi * fr * t)
    sig += severity * 0.05 * np.sin(2 * np.pi * 2 * fr * t)
    sig += 0.01 * np.random.randn(len(t))
    return sig


def gearbox_signal(t, fr, severity):
    gear_mesh = 20 * fr
    sig = 0.01 * np.sin(2 * np.pi * fr * t)
    sig += severity * 0.06 * np.sin(2 * np.pi * gear_mesh * t)
    sig += severity * 0.02 * np.random.randn(len(t))
    return sig


def pump_signal(t, fr, severity):
    sig = 0.015 * np.sin(2 * np.pi * fr * t)
    sig += severity * 0.04 * np.random.randn(len(t))
    return sig


# ==========================================================
# MAIN LOOP
# ==========================================================
def main():
    t = np.arange(WINDOW) / FS

    # severity per asset (independent degradation)
    severity_map = {}

    for site in TOPOLOGY:
        for asset in TOPOLOGY[site]:
            severity_map[(site, asset)] = 0.0

    print("🚀 Multi-Site Vibration Simulator Started")

    while True:

        for site, assets in TOPOLOGY.items():
            for asset, cfg in assets.items():

                rpm = cfg["rpm"]
                fr = rpm / 60

                # increase severity gradually per asset
                key = (site, asset)
                severity_map[key] = min(
                    severity_map[key] + 0.01, 1.0
                )
                severity = severity_map[key]

                for point, ptype in POINTS.items():

                    if ptype == "motor":
                        acc = motor_signal(t, fr, severity)

                    elif ptype == "gearbox":
                        acc = gearbox_signal(t, fr, severity)

                    else:
                        acc = pump_signal(t, fr, severity)

                    payload = {
                        "site": site,
                        "asset": asset,
                        "point": point,
                        "rpm": rpm,
                        "timestamp": time.time(),
                        "acceleration": acc.tolist(),
                    }

                    topic = f"vibration/raw/{site}/{asset}/{point}"

                    publish.single(
                        topic,
                        json.dumps(payload),
                        hostname=BROKER,
                        port=PORT,
                    )

                    print(
                        f"TX {topic} | rpm={rpm} | severity={severity:.2f}"
                    )

        time.sleep(1)


if __name__ == "__main__":
    main()

