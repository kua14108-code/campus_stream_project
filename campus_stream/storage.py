"""Storing results. Every sink appends to a CSV file as data arrives."""
from pathlib import Path


class CsvSink:
    def __init__(self, path, fresh=True):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if fresh and self.path.exists():
            self.path.unlink()   # start every run with a clean file

    def write(self, df):
        if df is None or df.empty:
            return
        header = not self.path.exists() or self.path.stat().st_size == 0
        num = df.select_dtypes("number").columns
        df = df.assign(**{c: df[c].round(3) for c in num})   # round numbers only, not timestamps
        df.to_csv(self.path, mode="a", header=header, index=False)
