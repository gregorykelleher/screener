# src/services/service_provider.py

import streamlit as st
from .fmp import setup_fmp_client
from .database import setup_database_client


def initialise_services():
    """
    Checks if each client is already in session_state;
    if not, sets it up and stores it.
    """
    if "fmp_client" not in st.session_state:
        st.session_state["fmp_client"] = setup_fmp_client()

    if "database_client" not in st.session_state:
        st.session_state["database_client"] = setup_database_client()
