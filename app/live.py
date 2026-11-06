"""Live page: each vessel's latest position from the live feed (serving Parquet), refreshed on a timer."""

from datetime import datetime, timezone

import streamlit as st

from app.charts import vessel_map, with_local_time
from app.queries import latest_positions, parquet_glob
from pipeline.config import load_config

cfg = load_config()
dash = cfg["dashboard"]
GLOB = parquet_glob(cfg["paths"]["positions"])

st.title("New York Harbor: Live Vessel Traffic")
st.markdown(
    "Vessel Watch follows ship traffic in New York Harbor using AIS, the position reports that vessels "
    "broadcast every few seconds to minutes. Live reports from [AISStream](https://aisstream.io/) flow "
    "through Kafka into Spark Structured Streaming, which cleans them and writes Parquet; this page reads "
    "the Parquet with DuckDB and refreshes every 30 seconds. The **History** page replays days from "
    "[NOAA's AIS archive](https://hub.marinecadastre.gov/pages/vesseltraffic), cleaned with the same Spark "
    "code. The **Data Dictionary** page explains every column."
)
st.caption("AISStream → Kafka → Spark Structured Streaming → Parquet (DuckDB for querying) → Streamlit")


@st.fragment(run_every=dash["refresh_seconds"])
def live_map() -> None:
    minutes = dash["recent_minutes"]
    df = latest_positions(GLOB, minutes, datetime.now(timezone.utc).replace(tzinfo=None))
    if df.empty:
        st.info(f"No position reports in the last {minutes} minutes. Is `make live` and `make stream` running?")
        return

    c1, c2 = st.columns(2)
    c1.metric(f"Vessels (last {minutes} min)", len(df))
    latest = df["ts"].max().tz_localize("UTC").tz_convert(dash["timezone"])
    c2.metric("Latest report", latest.strftime("%H:%M:%S %Z"))

    shown = with_local_time(df, dash["timezone"])
    vessel_map(shown, cfg["harbor_bbox"])
    st.dataframe(shown.sort_values("ts", ascending=False), hide_index=True, width="stretch")


live_map()
