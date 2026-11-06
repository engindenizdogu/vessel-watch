"""Map and chart components shared by the dashboard pages."""

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

# One hue (blue) from the dataviz reference ramp: the selected hour is the darker step, the rest recede.
SELECTED = "#2a78d6"
OTHER = "#86b6ef"


def for_display(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    """Copy of `df` with `ts` as local time text and missing names filled (pydeck tooltips need plain values)."""
    local = df["ts"].dt.tz_localize("UTC").dt.tz_convert(tz)
    return df.assign(ts=local.dt.strftime("%Y-%m-%d %H:%M:%S %Z"), name=df["name"].fillna("(no name)"))


def vessel_map(df: pd.DataFrame, box: dict[str, float]) -> None:
    """Draw the tracked area's outline and one dot per vessel (name, MMSI, speed, course, time on hover).

    `df` may be empty; the outline is drawn either way so the map always shows the area covered.
    """
    corners = [(box["lon_min"], box["lat_min"]), (box["lon_max"], box["lat_min"]),
               (box["lon_max"], box["lat_max"]), (box["lon_min"], box["lat_max"])]
    layers = [pdk.Layer(
        "PathLayer", data=[{"path": corners + corners[:1]}], get_path="path",
        get_color=[128, 128, 128, 220], width_min_pixels=2,  # mid gray reads on light and dark base maps
    )]
    if not df.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=df, get_position=["lon", "lat"],
            get_fill_color=[42, 120, 214, 200], get_radius=120, radius_min_pixels=3, pickable=True,
        ))
    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=(box["lat_min"] + box["lat_max"]) / 2,
            longitude=(box["lon_min"] + box["lon_max"]) / 2,
            zoom=10,
        ),
        tooltip={"text": "{name}\nMMSI {mmsi}\nSOG {sog} kn\nCOG {cog}\n{ts}"},
    ))
    st.caption(
        f"Gray outline: the area tracked, New York Harbor and its approaches ({box['lat_min']:.2f} to "
        f"{box['lat_max']:.2f}° N, {-box['lon_max']:.2f} to {-box['lon_min']:.2f}° W). "
        "Hover over a vessel for its name, speed, and course."
    )


def hourly_chart(df: pd.DataFrame, selected: str) -> alt.Chart:
    """Bars of `vessels` per `hour` label ("00" to "23"), with the `selected` hour darker; `period` is the tooltip."""
    hours = [f"{h:02d}" for h in range(24)]  # fixed domain, so hours without data show as gaps
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("hour:O", title="Hour (New York time)", scale=alt.Scale(domain=hours, paddingInner=0.2),
                    axis=alt.Axis(labelAngle=0)),
            y=alt.Y("vessels:Q", title="Vessels"),
            color=alt.when(alt.datum.hour == selected).then(alt.value(SELECTED)).otherwise(alt.value(OTHER)),
            tooltip=[alt.Tooltip("period:N", title="Hour"), alt.Tooltip("vessels:Q", title="Vessels")],
        )
        .properties(height=220)
    )
