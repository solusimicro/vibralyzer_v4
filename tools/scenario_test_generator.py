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

# ==========================================================
# MULTI SITE TOPOLOGY
# ==========================================================
TOPOLOGY = {
    "SITE_A": {
        "PUMP_01": 2980,
        "PUMP_02": 2960,
    },
    "SITE_B": {
        "PUMP_03": 3000,
    },
}

POINTS = {
    "P1MT": "unbalance",
    "P2MT": "misalignment",
    "P3GX": "gear_wear",
    "P4GX": "bearing_outer",
    "P5GX": "bearing_advanced",
    "P6GX": "gear_severe",
    "P7PP": "cavitation",
    "P8PP": "hydraulic",
}

# ==========================================================
# FAULT MODELS (RPM dynamic)
# ==========================================================
def unbalance(t, fr, s):
    return 0.02*np.sin(2*np.pi*fr*t) + s*0.05*np.sin(2*np.pi*fr*t)

def misalignment(t, fr, s):
    return 0.02*np.sin(2*np.pi*fr*t) + s*0.04*np.sin(2*np.pi*2*fr*t)

def gear_wear(t, fr, s):
    gm = 20 * fr
    return 0.01*np.sin(2*np.pi*fr*t) + s*0.06*np.sin(2*np.pi*gm*t)

def bearing_outer(t, fr, s):
    return s * 0.03 * np.random.randn(len(t))

def bearing_advanced(t, fr, s):
    return s * 0.08 * np.random.randn(len(t))

def gear_severe(t, fr, s):
    gm = 20 * fr
    return s*0.1*np.sin(2*np.pi*gm*t) + s*0.05*np.random.randn(len(t))

def cavitation(t, fr, s):
    return s*0.04*np.random.randn(len(t))

def hydraulic(t, fr, s):
    return 0.02*np.sin(2*np.pi*fr*t) + s*0.03*np.random.randn(len(t))

FAULT_MAP = {
    "unbalance": unbalance,
    "misalignment": misalignment,
    "gear_wear": gear_wear,
    "bearing_outer": bearing_outer,
    "bearing_advanced": bearing_advanced,
    "gear_severe": gear_severe,
    "cavitation": cavitation,
    "hydraulic": hydraulic,
}

# ==========================================================
# SCENARIO PHASES
# ==========================================================
SCENARIO = [
    ("NORMAL", 0.0, 20),
    ("WATCH", 0.2, 20),
    ("WARNING", 0.5, 20),
    ("ALARM", 1.0, 20),
    ("CLEAR", 0.1, 20),
]

# ==========================================================
# MAIN
# ==========================================================
def main():
    t = np.arange(WINDOW) / FS

    print("🚀 Multi-Site Test Scenario Started")

    for phase, severity, duration in SCENARIO:

        print(f"\n=== PHASE {phase} (severity={severity}) ===")

        for _ in range(duration):

            for site, assets in TOPOLOGY.items():

                for asset, rpm in assets.items():

                    fr = rpm / 60

                    for point, fault in POINTS.items():

                        acc = FAULT_MAP[fault](t, fr, severity)
                        acc += 0.005 * np.random.randn(len(t))  # noise floor

                        payload = {
                            "site": site,
                            "asset": asset,
                            "point": point,
                            "rpm": rpm,
                            "scenario_phase": phase,
                            "severity": severity,
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
                            f"TX {topic} | phase={phase} | severity={severity}"
                        )

            time.sleep(1)


if __name__ == "__main__":
    main()

