"""Live page: each vessel's latest position from the live feed or a replay (serving Parquet), refreshed on a timer."""

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from app.charts import for_display, gate_overlay, vessel_map
from app.queries import gate_rates, latest_positions, latest_report_time, load_shutdown_spots, parquet_glob
from pipeline.config import load_config

cfg = load_config()
dash = cfg["dashboard"]
GLOB = parquet_glob(cfg["paths"]["positions"])
REPLAY = cfg["paths"]["replay_serving"] / "positions"
SPOTS = load_shutdown_spots(cfg["paths"]["shutdown_spots"])
RATES = gate_rates(cfg["paths"]["crossings_hourly"], cfg["gates"]["target"]["groups"])
GATES = gate_overlay(cfg["gates"]["lines"], RATES)

st.title("New York Harbor: Live Vessel Traffic")
st.markdown(
    "Vessel Watch follows ship traffic in New York Harbor using AIS, the position reports that vessels "
    "broadcast every few seconds to minutes. Live reports from [AISStream](https://aisstream.io/) flow "
    "through Kafka into Spark Structured Streaming, which cleans them and writes them to Parquet about every "
    f"{cfg['stream']['trigger_seconds']} seconds. This page reads the Parquet with DuckDB, refreshes every "
    f"{dash['refresh_seconds']} seconds, and shows each vessel's latest position from the last "
    f"{dash['recent_minutes']} minutes. The **History** page replays days from "
    "[NOAA's AIS archive](https://hub.marinecadastre.gov/pages/vesseltraffic), cleaned with the same Spark "
    "code, and the **Data Dictionary** page explains every column."
)
t = cfg["gates"]["target"]
target = RATES[(RATES["gate"] == t["gate"]) & (RATES["direction"] == t["direction"])]["ships"] if not RATES.empty else []
per_day = f" (about {target.iloc[0]:.0f} a day in the NOAA history)" if len(target) else ""
st.markdown(
    "The orange lines on the map are **gates** across the harbor's entrances: the sea entrance between Sandy "
    "Hook and Rockaway Point, the Narrows under the Verrazzano Bridge, the East River at Throgs Neck, and the "
    "Hudson at the George Washington Bridge. Each time a vessel's track crosses one, it is counted in or out. "
    f"The forecast will predict how many cargo ships and tankers enter from the sea each hour{per_day}."
)
st.caption("AISStream → Kafka → Spark Structured Streaming → Parquet (DuckDB for querying) → Streamlit")
feed = st.radio(
    "Feed", ["Live", "Replay"], horizontal=True,
    help="Replay sends NOAA history through the same Kafka topic and Spark job at an accelerated speed "
    "(`make stream RUN=replay`, then `make replay`); the map's clock is then the latest replayed report.",
)


@st.fragment(run_every=dash["refresh_seconds"])
def live_map(feed: str) -> None:
    minutes = dash["recent_minutes"]
    if feed == "Live":
        glob, as_of, hint = GLOB, datetime.now(timezone.utc).replace(tzinfo=None), "`make live` and `make stream`"
    else:
        glob, as_of, hint = parquet_glob(REPLAY), latest_report_time(REPLAY), "`make stream RUN=replay` and `make replay`"
    df = latest_positions(glob, minutes, as_of) if as_of else pd.DataFrame()
    if df.empty:
        st.info(f"No position reports in the last {minutes} minutes. Are {hint} running?")
        vessel_map(df, cfg["harbor_bbox"], SPOTS, GATES)
        return

    c1, c2 = st.columns(2)
    c1.metric(f"Vessels (last {minutes} min)", len(df))
    latest = df["ts"].max().tz_localize("UTC").tz_convert(dash["timezone"])
    c2.metric("Latest report", latest.strftime("%H:%M:%S %Z" if feed == "Live" else "%Y-%m-%d %H:%M:%S %Z"))

    shown = for_display(df, dash["timezone"])
    vessel_map(shown, cfg["harbor_bbox"], SPOTS, GATES)
    st.dataframe(shown.sort_values("ts", ascending=False), hide_index=True, width="stretch")


live_map(feed)
