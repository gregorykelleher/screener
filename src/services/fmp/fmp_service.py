# services/fmp_service.py

import streamlit as st
from fmp_lib import Client


def _load_api_key() -> str:
    """
    Loads the FMP API key from Streamlit secrets.

    Returns:
        str: The FMP API key.
    """
    try:
        return st.secrets["fmp"]["api_key"]
    except KeyError:
        st.error("API key for FMP not found. Please set it in the secrets.")
        st.stop()


@st.cache_resource
def setup_fmp_client() -> Client:
    """
    Initializes the FMP Client and stores it in session state.
    """
    api_key = _load_api_key()
    try:
        client: Client = Client(api_key)
    except ValueError as ve:
        st.error(f"Initialization Error: {ve}")
        st.stop()
    except Exception as e:
        st.error(f"Unexpected Error: {e}")
        st.stop()
    return client


def retrieve_fmp_client():
    """
    Retrieve 'fmp_client' from session state.
    """
    return st.session_state.get("fmp_client")
