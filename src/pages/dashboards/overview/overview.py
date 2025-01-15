# overview/overview.py

import streamlit as st

from page_utils import register_page

from .utils import create_layout


@register_page(section="Dashboards", title="Overview", icon="📊", default=True)
def generate_overview(config):
    """
    Generate the Overview page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Overview")
    st.divider()

    # Create the layout
    create_layout(config)
