"""
The streaming pipeline: takes raw micro-batches, returns processed rows and alerts,
and writes everything to disk while the stream is running.
"""
from collections import Counter, deque

import pandas as pd

import config
import processing
from detection import AlertEngine, alerts_to_frame
from storage import CsvSink

MINUTE_GRACE_S = 30   # wait 30 s after a minute ends so late messages can still join it


class StreamPipeline:
    def __init__(self, out_dir=config.OUTPUT_DIR):
        self.windows = {}        # sensor_id -> history DataFrame
        self.latest = {}         # sensor_id -> newest processed row (for the dashboard)
        self.last_seen = {}      # sensor_id -> newest message time
        self.rooms = {}          # sensor_id -> room name
        self.stats = Counter()
        self.recent_alerts = deque(maxlen=50)
        self.recent_times = deque()   # timestamps of processed rows, for the rate indicator
        self.stream_time = None
        self.engine = AlertEngine()
        self.pending = None      # processed rows not yet added to a minute summary

        self.processed_sink = CsvSink(f"{out_dir}/processed.csv")
        self.alert_sink = CsvSink(f"{out_dir}/alerts.csv")
        self.minute_sink = CsvSink(f"{out_dir}/minute_summary.csv")

    
    def process_batch(self, raw):
        """Handle one micro-batch. Returns (processed_rows, list_of_alerts)."""
        cleaned, cs = processing.clean_batch(raw)
        for key, value in cs.items():
            self.stats[key] += value
        if cleaned.empty:
            return cleaned, []

        frames = []
        for sid, group in cleaned.groupby("sensor_id"):
            window, new_rows, info = processing.update_window(self.windows.get(sid), group)
            self.windows[sid] = window
            self.stats["duplicates"] += info["duplicates"]
            self.stats["filled_values"] += info["filled_values"]
            frames.append(new_rows)

            newest = group["timestamp"].max()
            self.last_seen[sid] = max(self.last_seen.get(sid, newest), newest)
            self.rooms[sid] = group["room"].iloc[0]

        processed = pd.concat([f for f in frames if not f.empty], ignore_index=True) \
            if any(not f.empty for f in frames) else pd.DataFrame()

        batch_max = cleaned["timestamp"].max()
        self.stream_time = batch_max if self.stream_time is None else max(self.stream_time, batch_max)

        alerts = []
        if not processed.empty:
            processed = processed.sort_values("timestamp", kind="stable").reset_index(drop=True)
            self.stats["processed"] += len(processed)
            self.stats["incomplete_rows"] += int((processed["quality"] == "incomplete").sum())
            alerts += self.engine.evaluate(processed)
            for sid, g in processed.groupby("sensor_id"):
                row = g.loc[g["timestamp"].idxmax()]
                old = self.latest.get(sid)
                if old is None or row["timestamp"] >= old["timestamp"]:
                    self.latest[sid] = row
            self.recent_times.extend(processed["timestamp"])
            self.pending = processed if self.pending is None else pd.concat(
                [self.pending, processed], ignore_index=True)

        alerts += self.engine.check_offline(self.last_seen, self.rooms, self.stream_time)

        while self.recent_times and (self.stream_time - self.recent_times[0]).total_seconds() > 60:
            self.recent_times.popleft()

        self.stats["alerts"] += len(alerts)
        self.recent_alerts.extend(alerts)

        self.processed_sink.write(processed)
        self.alert_sink.write(alerts_to_frame(alerts))
        self._flush_minutes(final=False)
        return processed, alerts

    
    def _flush_minutes(self, final):
        """Aggregate every finished minute (tumbling window) and store it."""
        if self.pending is None or self.pending.empty:
            return
        if final:
            closed, self.pending = self.pending, None
        else:
            cutoff = self.stream_time - pd.Timedelta(seconds=MINUTE_GRACE_S)
            cutoff = cutoff.floor("1min")
            is_closed = self.pending["timestamp"] < cutoff
            closed, self.pending = self.pending[is_closed], self.pending[~is_closed]
            if closed.empty:
                return
        closed = closed.assign(minute=closed["timestamp"].dt.floor("1min"))
        summary = closed.groupby(["minute", "room"]).agg(
            n_records=("co2_ppm", "size"),
            temp_mean=("temperature_c", "mean"),
            temp_max=("temperature_c", "max"),
            hum_mean=("humidity_pct", "mean"),
            co2_mean=("co2_ppm", "mean"),
            co2_min=("co2_ppm", "min"),
            co2_max=("co2_ppm", "max"),
        ).reset_index()
        self.minute_sink.write(summary)

    def finish(self):
        """Call once when the stream ends."""
        self._flush_minutes(final=True)

    
    def rate_last_minute(self):
        """Event rate indicator: records processed during the last 60 s of stream time."""
        return len(self.recent_times)

    def room_status(self):
        """Worst severity in the last 5 minutes for every sensor."""
        status = {}
        rank = {"OK": 0, "WARNING": 1, "CRITICAL": 2, "OFFLINE": 3}
        for sid in self.latest:
            status[sid] = "OK"
        if self.stream_time is None:
            return status
        for a in self.recent_alerts:
            if a["severity"] == "info":
                continue
            if (self.stream_time - a["alert_time"]).total_seconds() <= config.COOLDOWN_S:
                level = "CRITICAL" if a["severity"] == "critical" else "WARNING"
                if rank[level] > rank.get(status.get(a["sensor_id"], "OK"), 0):
                    status[a["sensor_id"]] = level
        for sid in self.engine.offline:
            status[sid] = "OFFLINE"
        return status
