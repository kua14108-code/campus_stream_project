# Smart Campus Air Quality Monitor

Real-time data collection and stream processing system (Midterm Project, Weeks 1-4).
It reads a stream of sensor messages from six rooms, cleans the data, calculates
rolling indicators with Pandas and NumPy, detects problems with simple rules, shows
the results in the console and stores everything in CSV files.

## What the system does

* Six sensors (one per room) send temperature, humidity and CO2 every 10 seconds.
* A generator simulates the stream (there is no real building to connect to). It also
  adds dirty data: missing values, error codes like -127, duplicates, late messages,
  and four faults: ventilation failure, heater fault, frozen sensor, lost connection.
* The pipeline processes the data poll by poll (one poll = messages that arrived
  together), not all at once.
* Indicators per sensor: moving average, rolling std, z-score, rate of change per
  minute, rolling min/max, event rate (records per minute), minute summaries.
* Detection rules: `CO2_HIGH`, `CO2_CRITICAL`, `ZSCORE_SPIKE`, `TEMP_RAPID_RISE`,
  `SENSOR_STUCK`, `SENSOR_OFFLINE`.

## Project structure

| File | Purpose |
| --- | --- |
| `main.py` | Entry point (command line options) |
| `config.py` | All settings: rooms, faults, thresholds, window sizes |
| `generator.py` | Stream simulator and dataset writer |
| `acquisition.py` | Incremental sources: simulator and CSV replay |
| `processing.py` | Cleaning and rolling indicators (Pandas, NumPy) |
| `detection.py` | Rule-based alert engine |
| `pipeline.py` | Connects everything, keeps state, writes results |
| `storage.py` | CSV sink |
| `dashboard.py` | Console output |
| `visualize.py` | Figures |
| `evaluate.py` | Compares results with the faults that were injected |
| `tests/` | Unit tests (pytest) |
| `data/sample_raw_stream.csv` | Input dataset (seed 42, 2 hours, 4298 raw records) |
| `output/` | Results of the last run |
| `docs/Midterm_Project_Report.docx` | Technical report |

## Installation (VS Code, Windows / macOS / Linux)

1. Install Python 3.10 or newer and open the project folder in VS Code (`File > Open Folder`).
2. Open a terminal in VS Code (`Terminal > New Terminal`) and create a virtual environment:

   ```
   python -m venv .venv
   ```
3. Activate it:
   * Windows (PowerShell): `.venv\Scripts\Activate.ps1`
   * macOS / Linux: `source .venv/bin/activate`
4. Install the libraries:

   ```
   pip install -r requirements.txt
   ```
5. In VS Code press `Ctrl+Shift+P`, choose `Python: Select Interpreter` and pick the `.venv` one.

## How to run

```
python main.py                      # simulate 2 hours of data as fast as possible (about 1 minute)
python main.py --minutes 20 --no-plots   # short test run
python main.py --delay 0.1 --refresh 30 --clear   # slower live demo, dashboard every 30 polls
python main.py --mode replay        # process data/sample_raw_stream.csv as a stream
python evaluate.py                  # check the detections against the injected faults
python -m pytest -q                 # run the tests
python generator.py --seed 5        # write a new dataset with another seed
```

In VS Code you can also use the `Run and Debug` panel, the configurations are in
`.vscode/launch.json`.

Options of `main.py`:

| Option | Meaning | Default |
| --- | --- | --- |
| `--mode` | `simulate` or `replay` | simulate |
| `--input` | raw CSV for replay | `data/sample_raw_stream.csv` |
| `--minutes` | length of the simulated stream | 120 |
| `--seed` | random seed | 42 |
| `--delay` | seconds to wait between polls | 0 |
| `--refresh` | show the dashboard every N polls (0 = only at the end) | 60 |
| `--clear` | clear the screen before every dashboard | off |
| `--out` | output folder | `output` |
| `--no-plots` | do not create figures | off |

Both modes give exactly the same results for the same data (checked with `cmp` on the
output files), so a run can always be reproduced.

## Output files

| File | Content |
| --- | --- |
| `output/raw_stream.csv` | Exact copy of the messages as they arrived |
| `output/processed.csv` | Cleaned values and all indicators, one row per reading |
| `output/alerts.csv` | Every alert with time, room, rule, severity and value |
| `output/minute_summary.csv` | One row per room and minute (count, mean, min, max) |
| `output/evaluation.json` | Result of `evaluate.py` |
| `output/figures/*.png` | Four figures used in the report |

### Raw message format

| Column | Meaning |
| --- | --- |
| `poll_id` | number of the gateway polling cycle in which the message arrived |
| `timestamp` | time of the measurement |
| `sensor_id`, `room` | sensor and its room |
| `temperature_c` | temperature, degrees Celsius |
| `humidity_pct` | relative humidity, % |
| `co2_ppm` | CO2 concentration, ppm |

## Detection rules (thresholds are in `config.py`)

| Rule | Condition | Severity |
| --- | --- | --- |
| `CO2_HIGH` | CO2 above 1000 ppm for 2 minutes in a row | warning |
| `CO2_CRITICAL` | CO2 above 1500 ppm for 2 minutes in a row | critical |
| `ZSCORE_SPIKE` | abs(z-score) above 6 compared with the last 30 readings | warning |
| `TEMP_RAPID_RISE` | temperature rises faster than 1.2 C per minute | critical |
| `SENSOR_STUCK` | temperature, humidity and CO2 identical for 12 readings | warning |
| `SENSOR_OFFLINE` | no message for more than 60 seconds | critical |

The same alert is not repeated for 5 minutes (cooldown).

## Known limitations

* The data is simulated, so thresholds were tuned on it. Real sensors need new tuning.
* Windows are counted in readings, not in time. If many readings are missing, a window
  covers a longer time than 5 minutes.
* A message that arrives after its minute was already summarized is in `processed.csv`
  but not in `minute_summary.csv`.
* The state is kept in memory. If the program stops, the windows start again empty.
