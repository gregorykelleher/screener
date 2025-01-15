# overview/overview.py

import streamlit as st

import src.dashboards.overview.utils as ut

st.title("Overview")
st.divider()

# Retrieve the config from session_state
_config = st.session_state.get("config")
if _config is None:
    st.warning("config not found in session state. Using default overview config.")
    from config.main_config import config as default_config
    _config = default_config.overview_config

def retrieve_fmp():
    """
    Retrieve 'fmp_client' from session state.
    """
    fmp = st.session_state.get('fmp_client')
    if fmp is None:
        st.error("fmp client is not initialized. Please set 'fmp_client' in session state.")
        st.stop()
    return fmp

def create_layout(config):
    """
    Create the layout for the overview page with exactly four columns,
    each rendering a table as defined in the configuration.
    """
    cols = st.columns(4, gap="small")
    
    # Iterate over each column and corresponding table config and render the table.
    for col, table_cfg in zip(cols, config.overview_config):
        ut.render_table(col, table_cfg)
    st.divider()

# Create the layout
create_layout(_config)