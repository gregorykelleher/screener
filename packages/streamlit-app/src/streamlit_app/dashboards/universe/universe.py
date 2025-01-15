# universe.py

import streamlit as st

from dashboards.universe.utils import render_equities

st.title("Universe")

st.divider()

# Create your database connection
conn = st.connection("equities", type="sql")

# Render the company profiles
render_equities(conn)
