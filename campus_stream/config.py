"""
Settings for the Smart Campus air quality streaming project.

All the numbers that we might want to tweak (thresholds, window sizes,
room profiles, injected faults) are kept here so the other modules stay clean.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

 
# Stream settings
 
START_TIME = "2025-03-10 09:00:00"   # simulated clock start
STEP_SECONDS = 10                    # every sensor sends one reading per 10 s
DEFAULT_MINUTES = 120                # length of the simulated session
DEFAULT_SEED = 42

METRICS = ["temperature_c", "humidity_pct", "co2_ppm"]
RAW_COLUMNS = ["poll_id", "timestamp", "sensor_id", "room"] + METRICS

# physically possible values, everything outside is treated as a sensor error
VALID_RANGES = {
    "temperature_c": (-10.0, 50.0),
    "humidity_pct": (0.0, 100.0),
    "co2_ppm": (300.0, 5000.0),
}

 
# Processing settings (all windows are counted in readings, 1 reading = 10 s)
 
WINDOW = 30          # rolling window = 30 readings = 5 minutes
MIN_PERIODS = 10     # need at least this many values before stats are used
RATE_LAG = 6         # rate of change is measured over 6 readings = 1 minute
SUSTAIN_N = 12       # "sustained" means 12 readings in a row = 2 minutes
FLAT_N = 12          # a sensor is "stuck" if nothing changes for 12 readings
MAX_FILL = 2         # forward-fill at most 2 missing readings in a row
MAX_WINDOW_ROWS = 120  # history kept in memory per sensor (20 minutes)

# minimal std used in the z-score, otherwise a very calm signal gives huge z
STD_FLOOR = {"temperature_c": 0.10, "humidity_pct": 0.50, "co2_ppm": 10.0}


# Detection rules

CO2_HIGH = 1000          # ppm, common comfort limit for classrooms
CO2_CRITICAL = 1500      # ppm
Z_LIMIT = 6.0            # spike if |z| is bigger than this (see the sweep in evaluate.py)
TEMP_RATE_LIMIT = 1.2    # degrees C per minute
OFFLINE_AFTER_S = 60     # no data for this long -> sensor offline
COOLDOWN_S = 300         # do not repeat the same alert for 5 minutes


# Rooms (one sensor per room)
# sessions = (start_minute, end_minute, number_of_people)
# ppm_pp = how many ppm of CO2 one person adds when ventilation is normal

ROOMS = [
    {"sensor_id": "S01", "room": "Lecture-101", "base_temp": 21.5, "base_hum": 38.0,
     "vent_rate": 1 / 700, "ppm_pp": 18, "sessions": [(5, 80, 40), (95, 120, 30)]},
    {"sensor_id": "S02", "room": "Lab-201", "base_temp": 22.0, "base_hum": 40.0,
     "vent_rate": 1 / 500, "ppm_pp": 20, "sessions": [(0, 60, 18), (70, 120, 22)]},
    {"sensor_id": "S03", "room": "Lab-305", "base_temp": 22.5, "base_hum": 42.0,
     "vent_rate": 1 / 900, "ppm_pp": 20, "sessions": [(10, 120, 15)]},
    {"sensor_id": "S04", "room": "Library-1F", "base_temp": 21.0, "base_hum": 36.0,
     "vent_rate": 1 / 600, "ppm_pp": 12, "sessions": [(0, 120, 25)]},
    {"sensor_id": "S05", "room": "Canteen", "base_temp": 23.0, "base_hum": 45.0,
     "vent_rate": 1 / 300, "ppm_pp": 8, "sessions": [(20, 50, 60), (60, 100, 70)]},
    {"sensor_id": "S06", "room": "Study-Hall", "base_temp": 21.5, "base_hum": 37.0,
     "vent_rate": 1 / 600, "ppm_pp": 15, "sessions": [(30, 110, 30)]},
]

# Faults that we inject into the simulation so the detectors have something to find
# (start and end are in minutes from the start of the stream)
FAULTS = [
    {"sensor_id": "S03", "kind": "vent_fail", "start": 15, "end": 75},   # ventilation broken
    {"sensor_id": "S01", "kind": "temp_spike", "start": 55, "end": 60},  # heater fault
    {"sensor_id": "S04", "kind": "stuck", "start": 80, "end": 95},       # sensor freezes
    {"sensor_id": "S05", "kind": "offline", "start": 100, "end": 108},   # no messages
]

# Probabilities of "dirty data" per reading
GLITCH_RATES = {
    "missing": 0.010,    # one field is empty
    "sentinel": 0.004,   # error code such as -127 (typical for DS18B20 sensors)
    "spike": 0.003,      # single short jump that is still inside the valid range
    "duplicate": 0.005,  # same message is sent again in the next poll
    "late": 0.010,       # message arrives 2-4 polls too late
}
