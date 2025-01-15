# trackers/favourites.py

import streamlit as st

from page_utils import register_page


@register_page(section="Trackers", title="Favourites", icon="⭐")
def generate_favourites(config):
    """
    Generate the Favourites page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Favourites")
    st.divider()
