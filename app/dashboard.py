"""Streamlit dashboard: live map of vessels in the harbor, read from serving Parquet through DuckDB."""

from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

from pipeline.config import load_config

cfg = load_config()
POSITIONS_GLOB = str(cfg["paths"]["positions"] / "**" / "*.parquet")


def latest_positions(minutes: int) -> pd.DataFrame:
    """Return each vessel's latest report from the last `minutes` minutes (UTC)."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=minutes)
    try:
        return duckdb.execute(
            f"""
            select mmsi, ts, lat, lon, sog, cog, heading, vessel_type, status
            from read_parquet('{POSITIONS_GLOB}', hive_partitioning = true)
            where ts >= ?
            qualify row_number() over (partition by mmsi order by ts desc) = 1
            """,
            [cutoff],
        ).df()
    except duckdb.IOException:  # no Parquet written yet
        return pd.DataFrame()


st.set_page_config(page_title="Vessel Watch", layout="wide")
st.title("New York Harbor: live vessel traffic")


@st.fragment(run_every=cfg["dashboard"]["refresh_seconds"])
def live_map() -> None:
    minutes = cfg["dashboard"]["recent_minutes"]
    df = latest_positions(minutes)
    if df.empty:
        st.info(f"No position reports in the last {minutes} minutes. Is `make live` and `make stream` running?")
        return

    latest = df["ts"].max()
    c1, c2 = st.columns(2)
    c1.metric(f"Vessels (last {minutes} min)", len(df))
    local = latest.tz_localize("UTC").tz_convert(cfg["dashboard"]["timezone"])
    c2.metric("Latest report", local.strftime("%H:%M:%S %Z"))

    df["ts"] = df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")  # pydeck tooltips need plain values
    box = cfg["harbor_bbox"]
    st.pydeck_chart(pdk.Deck(
        layers=[pdk.Layer(
            "ScatterplotLayer", data=df, get_position=["lon", "lat"],
            get_fill_color=[0, 120, 220, 200], get_radius=120, radius_min_pixels=3, pickable=True,
        )],
        initial_view_state=pdk.ViewState(
            latitude=(box["lat_min"] + box["lat_max"]) / 2,
            longitude=(box["lon_min"] + box["lon_max"]) / 2,
            zoom=10,
        ),
        tooltip={"text": "MMSI {mmsi}\nSOG {sog} kn\nCOG {cog}\n{ts} UTC"},
    ))
    st.dataframe(df.sort_values("ts", ascending=False), hide_index=True, width="stretch")


live_map()
