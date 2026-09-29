"""Console dashboard: builds a text snapshot of the current state of the stream."""
import pandas as pd


def render(pipeline, poll_no):
    if pipeline.stream_time is None or not pipeline.latest:
        return "waiting for data..."

    status = pipeline.room_status()
    rows = []
    for sid, r in sorted(pipeline.latest.items()):
        rows.append({
            "Room": r["room"],
            "Temp C": r["temperature_c"],
            "Hum %": r["humidity_pct"],
            "CO2 ppm": r["co2_ppm"],
            "CO2 avg 5m": r["co2_ppm_ma"],
            "CO2 ppm/min": r["co2_rate"],
            "Status": status.get(sid, "OK"),
            "Last data": f"{r['timestamp']:%H:%M:%S}",
        })
    table = pd.DataFrame(rows).round(1).to_string(index=False)

    s = pipeline.stats
    lines = [
        "=" * 88,
        f" SMART CAMPUS AIR QUALITY MONITOR   stream time {pipeline.stream_time:%Y-%m-%d %H:%M:%S}   poll #{poll_no}",
        "=" * 88,
        table,
        "-" * 88,
        (f" received={s['received']}  processed={s['processed']}  duplicates={s['duplicates']}  "
         f"out_of_range={s['out_of_range']}  missing={s['missing_values']}  filled={s['filled_values']}"),
        f" records in last 60 s: {pipeline.rate_last_minute()}   alerts so far: {s['alerts']}",
    ]
    last = list(pipeline.recent_alerts)[-4:]
    if last:
        lines.append(" last alerts:")
        for a in last:
            lines.append(f"   {a['alert_time']:%H:%M:%S}  {a['severity'].upper():8} "
                         f"{a['room']:12} {a['rule']:16} {a['message']}")
    lines.append("=" * 88)
    return "\n".join(lines)


def alert_line(a):
    """One line for the running alert log."""
    return (f"[{a['alert_time']:%H:%M:%S}] {a['severity'].upper():8} {a['room']:12} "
            f"{a['rule']:16} {a['message']}")
