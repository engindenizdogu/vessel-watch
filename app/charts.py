"""Map and chart components shared by the dashboard pages."""

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

# One hue (blue) from the dataviz reference ramp: the selected hour is the darker step, the rest recede.
SELECTED = "#2a78d6"
OTHER = "#86b6ef"
# Map colors: categorical slots 1-2 (vessels blue, gates orange), each with its light- and dark-map step;
# both sets pass the palette validator. Shutdown spots are neutral gray rings: context, not a category.
MAP_COLORS = {
    "light": {"vessel": [42, 120, 214], "gate": [235, 104, 52], "spot": [110, 110, 110]},
    "dark": {"vessel": [57, 135, 229], "gate": [217, 89, 38], "spot": [190, 190, 190]},
}


def _map_colors() -> dict[str, list[int]]:
    """Colors for the viewer's theme (Streamlit picks a dark base map in dark mode)."""
    return MAP_COLORS["dark" if st.context.theme.type == "dark" else "light"]


def for_display(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    """Copy of `df` with `ts` as local time text and missing names filled (pydeck tooltips need plain values)."""
    local = df["ts"].dt.tz_localize("UTC").dt.tz_convert(tz)
    return df.assign(ts=local.dt.strftime("%Y-%m-%d %H:%M:%S %Z"), name=df["name"].fillna("(no name)"))


def _text(values: pd.Series, unit: str = "") -> pd.Series:
    """Values as tooltip text, "n/a" when missing."""
    return values.map(lambda v: "n/a" if pd.isna(v) else f"{v:g}{unit}")


def gate_overlay(lines: list[dict], rates: pd.DataFrame) -> list[dict]:
    """Gate lines (`gates.lines` in config.yaml) with hover text from `gate_rates` (per-day crossings)."""
    rows = []
    for g in lines:
        r = rates[rates["gate"] == g["name"]].set_index("direction") if not rates.empty else pd.DataFrame()
        tip = f"Gate: {g['label']}"
        if {"in", "out"} <= set(r.index):
            tip += (f"\nPer day in the NOAA history: {r.at['in', 'ships']:.1f} cargo ships and tankers in, "
                    f"{r.at['out', 'ships']:.1f} out\nAll vessels: {r.at['in', 'all']:.1f} in, {r.at['out', 'all']:.1f} out")
        (ax, ay), (bx, by) = g["line"]
        rows.append({"path": g["line"], "label": g["label"].split(" (")[0], "tip": tip, "mid": [(ax + bx) / 2, (ay + by) / 2]})
    return rows


def vessel_map(
    df: pd.DataFrame, box: dict[str, float], spots: pd.DataFrame | None = None, gates: list[dict] | None = None,
) -> None:
    """Draw the tracked area's outline, labeled gates, shutdown spots as rings, and one dot per vessel.

    `df` (from `for_display`) and `spots` may be empty; the outline is drawn either way so the map always
    shows the area covered. `gates` come from `gate_overlay`. Hover shows details.
    """
    c = _map_colors()
    corners = [(box["lon_min"], box["lat_min"]), (box["lon_max"], box["lat_min"]),
               (box["lon_max"], box["lat_max"]), (box["lon_min"], box["lat_max"])]
    layers = [pdk.Layer(
        "PathLayer", data=[{"path": corners + corners[:1]}], get_path="path",
        get_color=[128, 128, 128, 220], width_min_pixels=2,  # mid gray reads on light and dark base maps
    )]
    gates = gates or []
    if gates:
        layers.append(pdk.Layer(
            "PathLayer", data=gates, get_path="path", get_color=c["gate"] + [255], width_min_pixels=5,
            cap_rounded=True, pickable=True,
        ))
        layers.append(pdk.Layer(  # label just above the gate's middle
            "TextLayer", data=gates, get_position="mid", get_text="label", get_size=13, get_color=[40, 40, 40, 255],
            get_alignment_baseline="'bottom'", get_pixel_offset=[0, -6], background=True,
            get_background_color=[255, 255, 255, 200],
        ))
    has_spots = spots is not None and not spots.empty
    if has_spots:
        tips = (
            "Shutdown spot: " + spots["vessels"].astype(str) + " vessels went silent here "
            + spots["silences"].astype(str) + " times\nMedian silence " + _text(spots["median_silence_h"].round(1), " h")
        )
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=spots.assign(tip=tips), get_position=["lon", "lat"], get_radius=110,
            radius_min_pixels=5, get_fill_color=c["spot"] + [40], stroked=True, get_line_color=c["spot"] + [230],
            line_width_min_pixels=2, pickable=True,
        ))
    if not df.empty:
        tips = (
            df["name"] + "\nMMSI " + df["mmsi"].astype(str) + "\nSOG " + _text(df["sog"], " kn")
            + "\nCOG " + _text(df["cog"], "°") + "\n" + df["ts"]
        )
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=df.assign(tip=tips), get_position=["lon", "lat"],
            get_fill_color=c["vessel"] + [200], get_radius=80, radius_min_pixels=2, pickable=True,
        ))
    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=(box["lat_min"] + box["lat_max"]) / 2,
            longitude=(box["lon_min"] + box["lon_max"]) / 2,
            zoom=10,
        ),
        tooltip={"text": "{tip}"},
    ))
    spot_note = (
        " Gray rings: shutdown spots, berths where several vessels have switched AIS off for an hour or more "
        "and reappeared in place (learned from the NOAA history), so a vessel going silent there is routine."
        if has_spots else ""
    )
    gate_note = " Orange lines: gates across the harbor's entrances, where vessels are counted in and out." if gates else ""
    st.caption(
        f"Gray outline: the area tracked, New York Harbor and its approaches ({box['lat_min']:.2f} to "
        f"{box['lat_max']:.2f}° N, {-box['lon_max']:.2f} to {-box['lon_min']:.2f}° W).{gate_note}{spot_note} "
        "Hover over any of them for details."
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
