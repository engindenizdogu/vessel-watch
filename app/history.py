"""History page: the harbor at a chosen time on a cleaned NOAA day, and vessels per hour for that day."""

from datetime import date, datetime, time, timedelta

import streamlit as st

from app.charts import for_display, gate_overlay, hourly_chart, vessel_map
from app.queries import (
    available_days, gate_rates, latest_positions, load_shutdown_spots, local_to_utc, parquet_glob, vessels_per_hour,
)
from pipeline.config import load_config

cfg = load_config()
dash = cfg["dashboard"]
TZ = dash["timezone"]
GLOB = parquet_glob(cfg["paths"]["clean"])
STEP = timedelta(minutes=dash["history_step_minutes"])
SPOTS = load_shutdown_spots(cfg["paths"]["shutdown_spots"])
GATES = gate_overlay(cfg["gates"]["lines"], gate_rates(cfg["paths"]["crossings_hourly"], cfg["gates"]["target"]["groups"]))

st.title("New York Harbor: History")
st.markdown(
    "Pick a day and a time to see where vessels were, or press **Play** to step through the day. The map "
    f"shows each vessel's latest report in the {dash['recent_minutes']} minutes before the chosen time, and "
    "the chart below counts vessels per hour. Days come from "
    "[NOAA's AIS archive](https://hub.marinecadastre.gov/pages/vesseltraffic) (2023 and July to December "
    "2024) and are cleaned with the same Spark code as the live feed. Times are New York time."
)
days = available_days(cfg["paths"]["clean"])
if not days:
    st.info("No cleaned NOAA days yet. Run `make history`.")
    st.stop()

c1, c2 = st.columns([1, 3], vertical_alignment="bottom")
day = c1.selectbox("Day (NOAA archive)", days, index=len(days) - 1, format_func=lambda d: f"{d:%a %Y-%m-%d}")
playing = c2.toggle(
    "Play", help=f"Advance {STEP.seconds // 60} minutes every {dash['play_seconds']} s, looping over the day"
)
st.session_state.setdefault("t", time(12, 0))


@st.fragment(run_every=dash["play_seconds"] if playing else None)
def day_view(day: date) -> None:
    if playing:  # step before the slider is drawn; .time() wraps past midnight back to 00:00
        st.session_state.t = (datetime.combine(day, st.session_state.t) + STEP).time()
    last = (datetime.combine(day, time(0)) - STEP).time()
    t = st.slider("Time (New York)", min_value=time(0), max_value=last, step=STEP, key="t", format="HH:mm")

    minutes = dash["recent_minutes"]
    df = latest_positions(GLOB, minutes, local_to_utc(day, t, TZ))
    m1, m2 = st.columns(2)
    m1.metric(f"Vessels (last {minutes} min)", len(df))
    m2.metric("Time", f"{day:%Y-%m-%d} {t:%H:%M}")
    if df.empty:
        st.info(
            f"No reports in the {minutes} minutes before {t:%H:%M}. NOAA files are UTC days, so late evening "
            "in New York needs the next day's file, which may not be in the sample."
        )
        vessel_map(df, cfg["harbor_bbox"], SPOTS, GATES)
    else:
        shown = for_display(df, TZ)
        vessel_map(shown, cfg["harbor_bbox"], SPOTS, GATES)

    hourly = vessels_per_hour(GLOB, local_to_utc(day, time(0), TZ), local_to_utc(day + timedelta(days=1), time(0), TZ))
    local = hourly["hour"].dt.tz_localize("UTC").dt.tz_convert(TZ)
    hourly = hourly.assign(hour=local.dt.strftime("%H"), period=local.dt.strftime("%H:00-%H:59"))
    st.subheader("Vessels per Hour")
    st.altair_chart(hourly_chart(hourly, f"{t:%H}"), width="stretch")

    with st.expander("Data", expanded=True):
        if not df.empty:
            st.dataframe(shown.sort_values("ts", ascending=False), hide_index=True, width="stretch")
        st.dataframe(hourly[["period", "vessels"]], hide_index=True)


day_view(day)
