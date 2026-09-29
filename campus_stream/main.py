"""
Smart Campus air quality monitoring: real-time data collection and stream processing.

Examples:
    python main.py                                  # simulate 2 hours, fast
    python main.py --delay 0.2 --refresh 30         # slower "live" demo
    python main.py --mode replay                    # replay data/sample_raw_stream.csv
"""
import argparse
import os
import sys

import config
from acquisition import make_source
from dashboard import alert_line, render
from generator import to_frame
from pipeline import StreamPipeline
from storage import CsvSink


def parse_args():
    ap = argparse.ArgumentParser(description="Real-time campus air quality stream processor")
    ap.add_argument("--mode", choices=["simulate", "replay"], default="simulate",
                    help="simulate = built-in generator, replay = read a saved raw CSV")
    ap.add_argument("--input", default=None, help="raw CSV for replay mode")
    ap.add_argument("--minutes", type=int, default=config.DEFAULT_MINUTES,
                    help="length of the simulated stream in minutes (simulate mode)")
    ap.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    ap.add_argument("--delay", type=float, default=0.0,
                    help="seconds to wait between polls (0 = as fast as possible)")
    ap.add_argument("--refresh", type=int, default=60,
                    help="print the dashboard every N polls (0 = only at the end)")
    ap.add_argument("--clear", action="store_true", help="clear the screen before each dashboard")
    ap.add_argument("--out", default=str(config.OUTPUT_DIR), help="output folder")
    ap.add_argument("--no-plots", action="store_true", help="skip the figures at the end")
    return ap.parse_args()


def main():
    args = parse_args()
    pipeline = StreamPipeline(args.out)
    raw_sink = CsvSink(f"{args.out}/raw_stream.csv") if args.mode == "simulate" else None

    print(f"Starting in '{args.mode}' mode. Results go to {args.out}\n")
    source = make_source(args.mode, args.minutes, args.seed, args.delay, args.input)

    poll_no = 0
    try:
        for raw in source:
            poll_no += 1
            if raw_sink is not None:
                raw_sink.write(raw)   # keep an exact copy of what arrived

            _, alerts = pipeline.process_batch(raw)

            for a in alerts:
                print(alert_line(a))
            if args.refresh and poll_no % args.refresh == 0:
                if args.clear:
                    print("\033[H\033[J", end="")
                print(render(pipeline, poll_no))
    except KeyboardInterrupt:
        print("\nStopped by user.")

    pipeline.finish()
    print("\nFINAL STATE")
    print(render(pipeline, poll_no))

    if not args.no_plots and pipeline.stats["processed"] > 0:
        from visualize import make_all
        fig_dir = make_all(args.out)
        print(f"\nFigures saved to {fig_dir}")
    print(f"Processed data: {args.out}/processed.csv, alerts.csv, minute_summary.csv")


if __name__ == "__main__":
    sys.exit(main())
