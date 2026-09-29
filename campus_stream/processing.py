"""
Cleaning and real-time indicators (Pandas + NumPy).

The pipeline keeps a short history ("window") for every sensor. When a new
micro-batch arrives we:
  1. clean it (parse time, check ranges),
  2. add it to the sensor window and drop duplicates,
  3. fill short gaps,
  4. recalculate the rolling indicators,
  5. return only the NEW rows (with their indicators) to the next stage.
"""
import numpy as np
import pandas as pd

import config
from config import METRICS


def clean_batch(raw):
    """Basic cleaning of one micro-batch. Returns (clean_df, counters)."""
    stats = {"received": len(raw), "bad_timestamp": 0, "missing_values": 0, "out_of_range": 0}
    df = raw.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    bad_ts = df["timestamp"].isna()
    stats["bad_timestamp"] = int(bad_ts.sum())
    df = df[~bad_ts].copy()

    for col in METRICS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    stats["missing_values"] = int(df[METRICS].isna().sum().sum())

    # values that can not be real (for example -127 C or 65535 ppm) become NaN
    for col, (lo, hi) in config.VALID_RANGES.items():
        outside = df[col].notna() & ~df[col].between(lo, hi)
        stats["out_of_range"] += int(outside.sum())
        df.loc[outside, col] = np.nan

    keep = ["timestamp", "sensor_id", "room"] + METRICS
    return df[keep].reset_index(drop=True), stats


def compute_indicators(ts, filled):
    """Rolling indicators for one sensor. `filled` has the 3 metric columns."""
    cols = {}   # collect the columns first and build the DataFrame once (much faster)

    # right after a long gap the window is stale, so the z-score is not trusted
    stale = ts.diff().dt.total_seconds() > config.OFFLINE_AFTER_S

    for m in METRICS:
        s = filled[m]
        # statistics of the PREVIOUS readings, so a spike can not hide itself
        prev = s.shift(1).rolling(config.WINDOW, min_periods=config.MIN_PERIODS)
        spread = np.maximum(prev.std(), config.STD_FLOOR[m])
        roll = s.rolling(config.WINDOW, min_periods=config.MIN_PERIODS)

        cols[f"{m}_ma"] = roll.mean()                                     # moving average
        cols[f"{m}_std"] = roll.std()                                     # rolling standard deviation
        cols[f"{m}_z"] = ((s - prev.mean()) / spread).mask(stale)         # z-score against recent history

    # rate of change per minute (uses real timestamps, so gaps are handled).
    # A median of 3 readings is used first, otherwise one single spike would
    # look like a very fast change.
    minutes = (ts - ts.shift(config.RATE_LAG)).dt.total_seconds() / 60
    for m in ("temperature_c", "co2_ppm"):
        smooth = filled[m].rolling(3, min_periods=3).median()
        cols[f"{m.split('_')[0]}_rate"] = (smooth - smooth.shift(config.RATE_LAG)) / minutes

    co2 = filled["co2_ppm"]
    cols["co2_min_2m"] = co2.rolling(config.SUSTAIN_N, min_periods=config.SUSTAIN_N).min()
    cols["co2_max_5m"] = co2.rolling(config.WINDOW, min_periods=config.MIN_PERIODS).max()

    # stuck sensor: max == min over the last FLAT_N readings for all metrics
    flat = np.ones(len(filled), dtype=bool)
    for m in METRICS:
        r = filled[m].rolling(config.FLAT_N, min_periods=config.FLAT_N)
        flat &= ((r.max() - r.min()) == 0).to_numpy()
    cols["stuck"] = flat
    return pd.DataFrame(cols, index=filled.index)


def update_window(window, batch):
    """
    Add a clean batch (rows of ONE sensor) to that sensor's window.

    Returns (new_window, new_rows, info). `new_rows` holds only the rows that
    came with this batch, together with all indicators.
    """
    batch = batch.assign(is_new=True)
    combined = batch if window is None or window.empty else pd.concat([window, batch], ignore_index=True)

    # a message with an already known timestamp is a duplicate (window rows come first)
    dup = combined.duplicated(subset="timestamp", keep="first")
    n_dup = int((dup & combined["is_new"]).sum())
    combined = combined[~dup].sort_values("timestamp", kind="stable").reset_index(drop=True)

    # short gaps are filled with the last known value, long gaps stay empty
    filled = combined[METRICS].ffill(limit=config.MAX_FILL)
    was_filled = combined[METRICS].isna() & filled.notna()
    still_missing = filled.isna().any(axis=1)

    ind = compute_indicators(combined["timestamp"], filled)

    quality = pd.Series(np.where(still_missing, "incomplete",
                                 np.where(was_filled.any(axis=1), "filled", "ok")),
                        index=combined.index, name="quality")
    result = pd.concat([combined[["timestamp", "sensor_id", "room"]], filled, quality, ind,
                        combined["is_new"]], axis=1)

    new_rows = result[result["is_new"]].drop(columns="is_new").reset_index(drop=True)
    info = {"duplicates": n_dup, "filled_values": int(was_filled[combined["is_new"]].sum().sum())}

    # keep only the raw (not filled) values in the stored window, so the
    # fill limit works correctly on the next call
    kept = combined.drop(columns="is_new").tail(config.MAX_WINDOW_ROWS).copy()
    kept["is_new"] = False
    return kept, new_rows, info
