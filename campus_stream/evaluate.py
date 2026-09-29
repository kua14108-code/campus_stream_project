"""
Check how well the pipeline did.

The simulator is deterministic, so we can run it again with the same seed and
get the exact list of problems that were injected (`stream.injected`). Then we
compare that list with what the pipeline cleaned and detected.

Usage (after running main.py):
    python evaluate.py
"""
import argparse
import json
from collections import Counter
from pathlib import Path

import pandas as pd

import config
from generator import SensorStream

# which alert rule should react to which injected fault, and the time it should start
FAULT_RULES = {
    "vent_fail": ("CO2_CRITICAL", "Ventilation failure (Lab-305)"),
    "temp_spike": ("TEMP_RAPID_RISE", "Heater fault (Lecture-101)"),
    "stuck": ("SENSOR_STUCK", "Frozen sensor (Library-1F)"),
    "offline": ("SENSOR_OFFLINE", "Lost connection (Canteen)"),
}


def main(out_dir, minutes, seed):
    out_dir = Path(out_dir)
    n_polls = int(minutes * 60 / config.STEP_SECONDS)
    stream = SensorStream(n_polls, seed=seed)
    for _ in stream:      # just run it to fill stream.injected
        pass
    inj = pd.DataFrame(stream.injected)
    inj["timestamp"] = pd.to_datetime(inj["timestamp"])
    counts = Counter(inj["kind"])

    proc = pd.read_csv(out_dir / "processed.csv", parse_dates=["timestamp"])
    alerts = pd.read_csv(out_dir / "alerts.csv", parse_dates=["alert_time"])
    result = {}

    # ---- 1. spikes: row level check with the z-score, for several thresholds ----
    spikes = inj[inj["kind"] == "spike"]
    zcols = [f"{m}_z" for m in config.METRICS]
    sweep = []
    for limit in (3, 4, 5, 6, 8, 10):
        hit = 0
        for r in spikes.itertuples():
            row = proc[(proc["sensor_id"] == r.sensor_id) & (proc["timestamp"] == r.timestamp)]
            if not row.empty and abs(row[f"{r.field}_z"].iloc[0]) > limit:
                hit += 1
        flagged = int((proc[zcols].abs() > limit).any(axis=1).sum())
        sweep.append({"z_limit": limit, "flagged_rows": flagged, "injected_found": hit,
                      "recall": round(hit / max(len(spikes), 1), 2),
                      "other_rows": flagged - hit})
    result["spikes"] = {"injected": len(spikes), "sweep": sweep}

    # ---- 2. faults: detection delay ----
    delays = []
    t0 = pd.Timestamp(config.START_TIME)
    for f in config.FAULTS:
        rule, label = FAULT_RULES[f["kind"]]
        start = t0 + pd.Timedelta(minutes=f["start"])
        end = t0 + pd.Timedelta(minutes=f["end"])
        if f["kind"] == "vent_fail":
            # CO2 needs a long time to build up, so we measure from the first reading over the limit
            over = proc[(proc["sensor_id"] == f["sensor_id"]) & (proc["co2_ppm"] > config.CO2_CRITICAL)]
            start = over["timestamp"].min()
        a = alerts[(alerts["sensor_id"] == f["sensor_id"]) & (alerts["rule"] == rule)
                   & (alerts["alert_time"] >= start) & (alerts["alert_time"] <= end + pd.Timedelta(minutes=5))]
        first = a["alert_time"].min() if not a.empty else pd.NaT
        delays.append({"fault": label, "rule": rule, "event_time": f"{start:%H:%M:%S}",
                       "first_alert": "-" if pd.isna(first) else f"{first:%H:%M:%S}",
                       "delay_s": None if pd.isna(first) else int((first - start).total_seconds())})
    result["faults"] = delays

    # ---- 3. data quality ----
    # what the pipeline counted is read from processed.csv / a fresh count of the raw file
    raw = pd.read_csv(out_dir / "raw_stream.csv")
    dup_removed = len(raw) - len(proc)
    result["cleaning"] = {
        "raw_records": len(raw),
        "processed_records": len(proc),
        "duplicates_injected": counts.get("duplicate", 0),
        "duplicates_removed": dup_removed,
        "sentinel_injected": counts.get("sentinel", 0),
        "missing_injected": counts.get("missing", 0),
        "late_injected": counts.get("late", 0),
    }
    result["alerts_total"] = int(len(alerts))
    result["alerts_by_rule"] = alerts["rule"].value_counts().to_dict()

    (out_dir / "evaluation.json").write_text(json.dumps(result, indent=2, default=str))

    print("SPIKES (injected = %d)" % result["spikes"]["injected"])
    print(pd.DataFrame(sweep).to_string(index=False))
    print("\nFAULTS")
    print(pd.DataFrame(delays).to_string(index=False))
    print("\nCLEANING:", result["cleaning"])
    print("\nALERTS  :", result["alerts_by_rule"])
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(config.OUTPUT_DIR))
    ap.add_argument("--minutes", type=int, default=config.DEFAULT_MINUTES)
    ap.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    a = ap.parse_args()
    main(a.out, a.minutes, a.seed)
