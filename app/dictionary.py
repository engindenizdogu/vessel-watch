"""Data dictionary page: renders docs/DATA_DICTIONARY.md, so the site and the repo share one copy."""

import streamlit as st

from pipeline.config import load_config

st.markdown(load_config()["paths"]["data_dictionary"].read_text())
