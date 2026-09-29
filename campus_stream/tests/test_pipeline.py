"""Small tests for the main parts of the pipeline. Run with:  python -m pytest -q"""
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
from detection import AlertEngine
from generator import SensorStream, to_frame
from pipeline import StreamPipeline
from processing import clean_batch, update_window


def make_rows(n, start="2025-03-10 09:00:00", co2=800.0, temp=22.0, hum=40.0, sensor="S01"):
    ts = pd.date_range(start, periods=n, freq="10s")
    return pd.DataFrame({
        "poll_id": range(n), "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id": sensor, "room": "Test-Room",
        "temperature_c": temp, "humidity_pct": hum, "co2_ppm": co2,
    })


def test_clean_batch_replaces_impossible_values():
    raw = make_rows(3)
    raw.loc[0, "temperature_c"] = -127.0
    raw.loc[1, "co2_ppm"] = 65535.0
    clean, stats = clean_batch(raw)
    assert stats["out_of_range"] == 2
    assert np.isnan(clean.loc[0, "temperature_c"]) and np.isnan(clean.loc[1, "co2_ppm"])


def test_clean_batch_drops_bad_timestamp():
    raw = make_rows(3)
    raw.loc[2, "timestamp"] = "not a time"
    clean, stats = clean_batch(raw)
    assert len(clean) == 2 and stats["bad_timestamp"] == 1


def test_duplicates_are_removed_across_batches():
    clean, _ = clean_batch(make_rows(5))
    window, new, info = update_window(None, clean.iloc[:3])
    window, new, info = update_window(window, clean.iloc[2:5])   # row 2 arrives twice
    assert info["duplicates"] == 1 and len(new) == 2


def test_short_gap_is_filled_long_gap_is_not():
    raw = make_rows(8)
    raw.loc[2:4, "co2_ppm"] = np.nan      # three missing in a row, limit is two
    clean, _ = clean_batch(raw)
    _, new, info = update_window(None, clean)
    assert info["filled_values"] == 2
    assert new.loc[4, "quality"] == "incomplete"
    assert new.loc[3, "quality"] == "filled"


def test_moving_average_of_constant_signal():
    clean, _ = clean_batch(make_rows(40, co2=900.0))
    _, new, _ = update_window(None, clean)
    assert np.isclose(new["co2_ppm_ma"].iloc[-1], 900.0)


def test_zscore_finds_single_spike():
    raw = make_rows(40)
    raw["co2_ppm"] += np.random.default_rng(1).normal(0, 5, 40)
    raw.loc[35, "co2_ppm"] += 700
    clean, _ = clean_batch(raw)
    _, new, _ = update_window(None, clean)
    assert abs(new.loc[35, "co2_ppm_z"]) > config.Z_LIMIT
    assert (new.loc[:34, "co2_ppm_z"].abs().dropna() < config.Z_LIMIT).all()


def test_stuck_sensor_flag():
    raw = make_rows(30)
    raw[config.METRICS] += np.random.default_rng(2).normal(0, 1, (30, 3))
    raw.loc[10:, config.METRICS] = raw.loc[10, config.METRICS].values   # freeze from row 10
    clean, _ = clean_batch(raw)
    _, new, _ = update_window(None, clean)
    assert new["stuck"].iloc[-1] and not new["stuck"].iloc[5]


def test_co2_alert_needs_two_minutes_and_has_cooldown():
    with tempfile.TemporaryDirectory() as tmp:
        pipe = StreamPipeline(out_dir=tmp)
        n = 60
        raw = make_rows(n, co2=1200.0)
        all_alerts = []
        for i in range(n):
            _, a = pipe.process_batch(raw.iloc[[i]])
            all_alerts += a
    rules = [a["rule"] for a in all_alerts]
    assert rules.count("CO2_HIGH") == 2      # first after 2 min, second after the 5 min cooldown
    assert "CO2_CRITICAL" not in rules
    first = min(a["alert_time"] for a in all_alerts)
    assert (first - pd.Timestamp("2025-03-10 09:00:00")).total_seconds() == 110


def test_offline_and_recovered():
    eng = AlertEngine()
    t0 = pd.Timestamp("2025-03-10 09:00:00")
    rooms = {"S01": "Room"}
    assert eng.check_offline({"S01": t0}, rooms, t0 + pd.Timedelta(seconds=30)) == []
    a = eng.check_offline({"S01": t0}, rooms, t0 + pd.Timedelta(seconds=90))
    assert a[0]["rule"] == "SENSOR_OFFLINE"
    assert eng.check_offline({"S01": t0}, rooms, t0 + pd.Timedelta(seconds=120)) == []   # only once
    b = eng.check_offline({"S01": t0 + pd.Timedelta(seconds=125)}, rooms, t0 + pd.Timedelta(seconds=130))
    assert b[0]["rule"] == "SENSOR_RECOVERED"


def test_generator_is_reproducible():
    a = pd.concat([to_frame(b) for b in SensorStream(30, seed=7)], ignore_index=True)
    b = pd.concat([to_frame(b) for b in SensorStream(30, seed=7)], ignore_index=True)
    c = pd.concat([to_frame(b) for b in SensorStream(30, seed=8)], ignore_index=True)
    pd.testing.assert_frame_equal(a, b)
    assert not a.equals(c)
