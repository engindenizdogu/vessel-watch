"""Streamlit entry point: navigation between the live map and the NOAA history pages."""

import streamlit as st

st.set_page_config(page_title="Vessel Watch", layout="wide")
live = st.Page("live.py", title="Live", icon=":material/sensors:", default=True)
history = st.Page("history.py", title="History", icon=":material/history:")
dictionary = st.Page("dictionary.py", title="Data Dictionary", icon=":material/menu_book:")
pages = [live, history, dictionary]
current = st.navigation(pages, position="hidden")

# Large page buttons instead of the small header links; the current page is the filled one.
for col, page in zip(st.columns(len(pages), width=560), pages):
    kind = "primary" if page.title == current.title else "secondary"
    if col.button(page.title, icon=page.icon, type=kind, width="stretch"):
        st.switch_page(page)

current.run()
