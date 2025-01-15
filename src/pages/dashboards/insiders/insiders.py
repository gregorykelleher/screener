# insiders/insiders.py

import streamlit as st

from page_utils import register_page


@register_page(section="Dashboards", title="Insiders", icon="👀")
def generate_insiders(config):
    """
    Generate the Insiders page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Insiders")
    st.divider()
