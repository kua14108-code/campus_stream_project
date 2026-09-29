"""
Incremental data acquisition.

Both sources below give the pipeline the same thing: a generator that
yields one small DataFrame (a micro-batch) at a time. The pipeline does not
care where the batches come from, so the same code works for the simulator
and for replaying a saved file.
"""
import time

import pandas as pd

import config
from generator import SensorStream, to_frame


def simulated_source(minutes, seed=config.DEFAULT_SEED, delay=0.0):
    """Live-like source. `delay` = seconds to wait between polls (0 = as fast as possible)."""
    n_polls = int(minutes * 60 / config.STEP_SECONDS)
    for batch in SensorStream(n_polls, seed=seed):
        if delay > 0:
            time.sleep(delay)
        yield to_frame(batch)


def csv_replay_source(path, delay=0.0, chunksize=1000):
    """
    Read a raw CSV file piece by piece and hand it out one poll at a time.

    The file is never loaded completely: we read `chunksize` rows, keep the
    last (maybe unfinished) poll for the next round, and yield the finished ones.
    """
    leftover = None
    for chunk in pd.read_csv(path, chunksize=chunksize):
        if leftover is not None:
            chunk = pd.concat([leftover, chunk], ignore_index=True)
        last_id = chunk["poll_id"].iloc[-1]
        leftover = chunk[chunk["poll_id"] == last_id]
        ready = chunk[chunk["poll_id"] != last_id]
        for _, batch in ready.groupby("poll_id", sort=False):
            if delay > 0:
                time.sleep(delay)
            yield batch.reset_index(drop=True)
    if leftover is not None and len(leftover):
        yield leftover.reset_index(drop=True)


def make_source(mode, minutes, seed, delay, input_path=None):
    if mode == "replay":
        if input_path is None:
            input_path = config.DATA_DIR / "sample_raw_stream.csv"
        return csv_replay_source(input_path, delay=delay)
    return simulated_source(minutes, seed=seed, delay=delay)
