"""
Data generator for the Smart Campus stream.

There is no live building to connect to, so this module simulates six
sensors (one per room). Every 10 simulated seconds the gateway does a
"poll" and collects the messages that arrived. The CO2 level follows a
simple physical model:

    dCO2/dt = generation(people) - ventilation * (CO2 - outdoor)

Temperature and humidity move slowly towards a target that depends on the
number of people. On top of that we add some noise, a few injected faults
(see config.FAULTS) and some dirty data (missing values, error codes,
duplicates, late messages), because real sensor data is never clean.

Run this file directly to save a dataset:
    python generator.py --out data/sample_raw_stream.csv
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import config

OUTDOOR_CO2 = 420.0


class SensorStream:
    """Iterate over it to get one list of raw messages (dicts) per poll."""

    def __init__(self, n_polls, seed=config.DEFAULT_SEED, rooms=None, faults=None,
                 glitch=None, start=config.START_TIME, step_seconds=config.STEP_SECONDS):
        self.n_polls = n_polls
        self.rng = np.random.default_rng(seed)
        self.rooms = rooms or config.ROOMS
        self.faults = faults if faults is not None else config.FAULTS
        self.glitch = glitch or config.GLITCH_RATES
        self.start = pd.Timestamp(start)
        self.step = step_seconds
        self.injected = []   # log of everything we break on purpose (used by evaluate.py)

    # -- helpers 
    def _people(self, room, minute):
        for s, e, n in room["sessions"]:
            if s <= minute < e:
                return n
        return 0

    def _fault_active(self, sensor_id, kind, minute):
        return any(f["sensor_id"] == sensor_id and f["kind"] == kind
                   and f["start"] <= minute < f["end"] for f in self.faults)

    def _log(self, rec, kind, field=""):
        self.injected.append({"timestamp": rec["timestamp"], "sensor_id": rec["sensor_id"],
                              "kind": kind, "field": field})

    def _apply_glitches(self, rec):
        """Add dirty data to one record (in place). Returns True if it should be duplicated."""
        g, rng = self.glitch, self.rng
        if rng.random() < g["missing"]:
            field = str(rng.choice(config.METRICS))
            rec[field] = np.nan
            self._log(rec, "missing", field)
        if rng.random() < g["sentinel"]:
            field = str(rng.choice(config.METRICS))
            if not np.isnan(rec[field]):
                rec[field] = {"temperature_c": -127.0, "humidity_pct": 255.0, "co2_ppm": 65535.0}[field]
                self._log(rec, "sentinel", field)
        if rng.random() < g["spike"]:
            field = str(rng.choice(config.METRICS))
            jump = {"temperature_c": rng.uniform(4, 6), "humidity_pct": rng.uniform(15, 25),
                    "co2_ppm": rng.uniform(600, 1000)}[field]
            if not np.isnan(rec[field]) and rec[field] not in (-127.0, 255.0, 65535.0):
                rec[field] = round(rec[field] + jump, 2)
                self._log(rec, "spike", field)
        dup = rng.random() < g["duplicate"]
        if dup:
            self._log(rec, "duplicate")
        return dup

    # -- main loop 
    def __iter__(self):
        rng = self.rng
        state = {}
        for r in self.rooms:
            state[r["sensor_id"]] = {
                "co2": OUTDOOR_CO2 + 30, "temp": r["base_temp"], "hum": r["base_hum"],
                "offset": 0.0, "last": None, "frozen": None,
            }
        delayed = []   # (release_poll, record)

        for poll in range(self.n_polls):
            ts = self.start + pd.Timedelta(seconds=poll * self.step)
            minute = poll * self.step / 60
            batch = []

            # records that were held back earlier are released now
            still_waiting = []
            for release, rec in delayed:
                if release <= poll:
                    batch.append({**rec, "poll_id": poll})
                else:
                    still_waiting.append((release, rec))
            delayed = still_waiting

            for room in self.rooms:
                sid = room["sensor_id"]
                st = state[sid]
                people = self._people(room, minute)

                # ---- physics ----
                vent = room["vent_rate"]
                if self._fault_active(sid, "vent_fail", minute):
                    vent *= 0.05
                generation = room["ppm_pp"] * room["vent_rate"] * people
                st["co2"] += self.step * (generation - vent * (st["co2"] - OUTDOOR_CO2))
                st["co2"] += rng.normal(0, 1.5)
                st["temp"] += self.step / 600 * ((room["base_temp"] + 0.03 * people) - st["temp"])
                st["hum"] += self.step / 900 * ((room["base_hum"] + 0.15 * people) - st["hum"])
                if self._fault_active(sid, "temp_spike", minute):
                    st["offset"] += 0.25
                else:
                    st["offset"] *= 0.93

                measured = (
                    st["temp"] + st["offset"] + rng.normal(0, 0.08),
                    st["hum"] + rng.normal(0, 0.4),
                    st["co2"] + rng.normal(0, 6),
                )

                # ---- faults that change what is sent ----
                if self._fault_active(sid, "offline", minute):
                    continue
                stuck = self._fault_active(sid, "stuck", minute)
                if stuck:
                    if st["frozen"] is None:
                        st["frozen"] = st["last"] or measured
                    measured = st["frozen"]
                else:
                    st["frozen"] = None
                    st["last"] = measured

                rec = {
                    "poll_id": poll,
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "sensor_id": sid,
                    "room": room["room"],
                    "temperature_c": round(measured[0], 2),
                    "humidity_pct": round(measured[1], 1),
                    "co2_ppm": float(round(measured[2])),
                }

                dup = False
                if not stuck:
                    dup = self._apply_glitches(rec)
                if dup:
                    delayed.append((poll + 1, dict(rec)))
                if rng.random() < self.glitch["late"]:
                    self._log(rec, "late")
                    delayed.append((poll + int(rng.integers(2, 5)), rec))
                else:
                    batch.append(rec)

            yield batch

        # send whatever is still waiting at the end of the stream
        if delayed:
            yield [{**rec, "poll_id": self.n_polls} for _, rec in delayed]


def to_frame(batch):
    """Turn a list of dicts into a DataFrame with a fixed column order."""
    return pd.DataFrame(batch, columns=config.RAW_COLUMNS)


def save_dataset(path, minutes=config.DEFAULT_MINUTES, seed=config.DEFAULT_SEED):
    """Run the whole simulation at once and write all raw messages to a CSV file."""
    n_polls = int(minutes * 60 / config.STEP_SECONDS)
    frames = [to_frame(b) for b in SensorStream(n_polls, seed=seed)]
    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate a raw sensor dataset")
    ap.add_argument("--out", default=str(config.DATA_DIR / "sample_raw_stream.csv"))
    ap.add_argument("--minutes", type=int, default=config.DEFAULT_MINUTES)
    ap.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    args = ap.parse_args()
    data = save_dataset(args.out, args.minutes, args.seed)
    print(f"Saved {len(data)} raw records to {args.out}")
