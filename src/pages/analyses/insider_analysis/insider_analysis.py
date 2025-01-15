# analyses/insider_analysis.py

import streamlit as st

from page_utils import register_page


@register_page(section="Analyses", title="Insider Analysis", icon="👀")
def generate_insider_analysis(config):
    """
    Generate the Insiders Analysis page using the provided configuration.

    Args:
        config (dict): The configuration dictionary.
    """
    st.title("Insiders Analysis")
    st.divider()
