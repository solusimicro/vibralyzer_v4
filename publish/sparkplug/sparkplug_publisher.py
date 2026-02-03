# publish/sparkplug/sparkplug_publisher.py

import time
from . import sparkplug_b_pb2 # Pastikan file pb2 sudah ada

class SparkplugPublisher:
    def __init__(self, broker, port, group_id, edge_node):
        self.group_id = group_id
        self.edge_node = edge_node
        self.seq = 0 # Sequence number 0-255

    def _get_next_seq(self):
        self.seq = (self.seq + 1) % 256
        return self.seq

    def publish_dbirth(self, device_id, point_config):
        """Mendaftarkan Device (p1mt, dll) ke SCADA"""
        topic = f"spBv1.0/{self.group_id}/DBIRTH/{self.edge_node}/{device_id}"
        
        # Payload harus berisi semua definisi metrik awal
        # Contoh metrik: 'type', 'rpm_nominal', 'phi', 'velocity_rms'
        payload = {
            "timestamp": int(time.time() * 1000),
            "metrics": [
                {"name": "metadata/type", "value": point_config['type'], "type": "String"},
                {"name": "metadata/rpm_nominal", "value": point_config['rpm'], "type": "Int32"},
                {"name": "phi", "value": 100.0, "type": "Float"},
                {"name": "state", "value": "NORMAL", "type": "String"}
            ],
            "seq": self._get_next_seq()
        }
        # Kirim ke MQTT (Gunakan library paho untuk publish payload yang sudah di-serialize)
        print(f"DEBUG: DBIRTH sent for {device_id}")

    def publish_ddata(self, device_id, metrics_dict):
        """Kirim update data real-time"""
        topic = f"spBv1.0/{self.group_id}/DDATA/{self.edge_node}/{device_id}"
        # Konversi dict ke format Sparkplug Metrics
        # ...