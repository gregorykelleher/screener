# trackers/portfolio.py

import streamlit as st
from page_utils import register_page


@register_page(section="Trackers", title="Portfolio", icon="🧰")
def generate_portfolio(config):
    """
    Generate the Portfolio page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Portfolio")
    st.divider()
