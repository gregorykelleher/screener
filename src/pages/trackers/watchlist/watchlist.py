# trackers/watchlist.py

import streamlit as st
from page_utils import register_page


@register_page(section="Trackers", title="Watchlist", icon="🔍")
def generate_watchlist(config):
    """
    Generate the Watchlist page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Watchlist")
    st.divider()
