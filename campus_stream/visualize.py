"""
Figures made from the stored results (output/processed.csv and output/alerts.csv).
Run alone with:  python visualize.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")   # save to files, no window needed
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import config


def _time_axis(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.grid(alpha=0.3)


def make_all(out_dir=config.OUTPUT_DIR):
    out_dir = Path(out_dir)
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    proc = pd.read_csv(out_dir / "processed.csv", parse_dates=["timestamp"])
    alerts_path = out_dir / "alerts.csv"
    alerts = pd.read_csv(alerts_path, parse_dates=["alert_time"]) if alerts_path.exists() \
        else pd.DataFrame(columns=["alert_time", "room", "rule", "value", "metric"])

    # 1) CO2 in all rooms
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for room, g in proc.groupby("room"):
        ax.plot(g["timestamp"], g["co2_ppm_ma"], label=room, linewidth=1.5)
    ax.axhline(config.CO2_HIGH, color="orange", linestyle="--", label="high (1000)")
    ax.axhline(config.CO2_CRITICAL, color="red", linestyle="--", label="critical (1500)")
    ax.set_title("CO2 moving average (5 min) in all rooms")
    ax.set_ylabel("CO2, ppm")
    ax.legend(ncol=4, fontsize=8)
    _time_axis(ax)
    fig.tight_layout()
    fig.savefig(fig_dir / "fig1_co2_all_rooms.png", dpi=150)
    plt.close(fig)

    # 2) Lab-305: ventilation failure
    g = proc[proc["room"] == "Lab-305"]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(g["timestamp"], g["co2_ppm"], color="lightsteelblue", label="CO2 reading")
    ax.plot(g["timestamp"], g["co2_ppm_ma"], color="navy", label="moving average (5 min)")
    ax.axhline(config.CO2_HIGH, color="orange", linestyle="--")
    ax.axhline(config.CO2_CRITICAL, color="red", linestyle="--")
    a = alerts[(alerts["room"] == "Lab-305") & (alerts["rule"].str.startswith("CO2"))]
    for rule, color in (("CO2_HIGH", "orange"), ("CO2_CRITICAL", "red")):
        x = a[a["rule"] == rule]
        ax.scatter(x["alert_time"], x["value"], color=color, zorder=5, s=50, label=rule)
    ax.set_title("Lab-305: CO2 during the ventilation failure")
    ax.set_ylabel("CO2, ppm")
    ax.legend(fontsize=8)
    _time_axis(ax)
    fig.tight_layout()
    fig.savefig(fig_dir / "fig2_lab305_co2_alerts.png", dpi=150)
    plt.close(fig)

    # 3) Lecture-101: temperature spike and rate of change
    g = proc[proc["room"] == "Lecture-101"]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5.5), sharex=True)
    ax1.plot(g["timestamp"], g["temperature_c"], color="tab:red", label="temperature")
    ax1.plot(g["timestamp"], g["temperature_c_ma"], color="black", linewidth=1, label="moving average")
    ax1.set_ylabel("Temperature, C")
    ax1.legend(fontsize=8)
    ax2.plot(g["timestamp"], g["temperature_rate"], color="tab:purple")
    ax2.axhline(config.TEMP_RATE_LIMIT, color="red", linestyle="--", label="limit (1.2 C/min)")
    a = alerts[(alerts["room"] == "Lecture-101") & (alerts["rule"] == "TEMP_RAPID_RISE")]
    ax2.scatter(a["alert_time"], [config.TEMP_RATE_LIMIT] * len(a), color="red", zorder=5, label="alert")
    ax2.set_ylabel("Rate, C/min")
    ax2.legend(fontsize=8)
    ax1.set_title("Lecture-101: temperature and its rate of change")
    _time_axis(ax1)
    _time_axis(ax2)
    fig.tight_layout()
    fig.savefig(fig_dir / "fig3_lecture101_temperature.png", dpi=150)
    plt.close(fig)

    # 4) alerts by rule and room
    if not alerts.empty:
        table = alerts.pivot_table(index="room", columns="rule", values="value",
                                   aggfunc="count", fill_value=0)
        ax = table.plot(kind="bar", stacked=True, figsize=(10, 4.5), colormap="tab20")
        ax.set_title("Number of alerts by room and rule")
        ax.set_ylabel("alerts")
        ax.set_xlabel("")
        plt.xticks(rotation=20)
        ax.legend(fontsize=8)
        plt.tight_layout()
        plt.savefig(fig_dir / "fig4_alerts_by_room.png", dpi=150)
        plt.close()

    return fig_dir


if __name__ == "__main__":
    print("Figures saved to", make_all())
