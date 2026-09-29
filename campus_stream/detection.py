"""
Rule-based event and anomaly detection.

Rules (all thresholds are in config.py):
  CO2_HIGH / CO2_CRITICAL  CO2 stays above the limit for 2 minutes in a row
  ZSCORE_SPIKE             one reading is far away from the recent history
  TEMP_RAPID_RISE          temperature grows faster than 1.2 C per minute
  SENSOR_STUCK             all three values do not change for 12 readings
  SENSOR_OFFLINE           no message from a sensor for more than 60 s
The same alert (sensor + rule + metric) is not repeated during a cooldown time,
otherwise one long problem would fill the screen with hundreds of lines.
"""
import pandas as pd

import config


class AlertEngine:
    def __init__(self):
        self.last_alert = {}   # (sensor, rule, metric) -> time of the last alert
        self.offline = set()   # sensors that are offline right now

    def _emit(self, ts, sensor_id, room, rule, severity, metric, value, message):
        key = (sensor_id, rule, metric)
        last = self.last_alert.get(key)
        if last is not None and (ts - last).total_seconds() < config.COOLDOWN_S:
            return None
        self.last_alert[key] = ts
        return {"alert_time": ts, "sensor_id": sensor_id, "room": room, "rule": rule,
                "severity": severity, "metric": metric, "value": value, "message": message}

    def evaluate(self, rows):
        """Check the new processed rows. Returns a list of alert dicts."""
        alerts = []
        for r in rows.sort_values("timestamp").to_dict("records"):
            ts, sid, room = r["timestamp"], r["sensor_id"], r["room"]
            found = []

            co2_min = r["co2_min_2m"]
            if co2_min > config.CO2_CRITICAL:
                found.append(("CO2_CRITICAL", "critical", "co2_ppm", r["co2_ppm"],
                              f"CO2 above {config.CO2_CRITICAL} ppm for 2 min"))
            elif co2_min > config.CO2_HIGH:
                found.append(("CO2_HIGH", "warning", "co2_ppm", r["co2_ppm"],
                              f"CO2 above {config.CO2_HIGH} ppm for 2 min"))

            for m in config.METRICS:
                z = r[f"{m}_z"]
                if abs(z) > config.Z_LIMIT:
                    found.append(("ZSCORE_SPIKE", "warning", m, r[m], f"z-score = {z:.1f}"))

            rate = r["temperature_rate"]
            if rate > config.TEMP_RATE_LIMIT:
                found.append(("TEMP_RAPID_RISE", "critical", "temperature_c", r["temperature_c"],
                              f"temperature rises {rate:.1f} C/min"))

            if r["stuck"]:
                found.append(("SENSOR_STUCK", "warning", "all", r["co2_ppm"],
                              "values did not change for 2 min"))

            for rule, sev, metric, value, msg in found:
                a = self._emit(ts, sid, room, rule, sev, metric, value, msg)
                if a:
                    alerts.append(a)
        return alerts

    def check_offline(self, last_seen, rooms, stream_time):
        """Compare the last message time of every sensor with the current stream time."""
        alerts = []
        for sid, seen in last_seen.items():
            gap = (stream_time - seen).total_seconds()
            if gap > config.OFFLINE_AFTER_S and sid not in self.offline:
                self.offline.add(sid)
                alerts.append({"alert_time": stream_time, "sensor_id": sid, "room": rooms[sid],
                               "rule": "SENSOR_OFFLINE", "severity": "critical", "metric": "all",
                               "value": gap, "message": f"no data for {gap:.0f} s"})
            elif gap <= config.OFFLINE_AFTER_S and sid in self.offline:
                self.offline.discard(sid)
                alerts.append({"alert_time": stream_time, "sensor_id": sid, "room": rooms[sid],
                               "rule": "SENSOR_RECOVERED", "severity": "info", "metric": "all",
                               "value": gap, "message": "sensor is sending data again"})
        return alerts


def alerts_to_frame(alerts):
    cols = ["alert_time", "sensor_id", "room", "rule", "severity", "metric", "value", "message"]
    return pd.DataFrame(alerts, columns=cols)
