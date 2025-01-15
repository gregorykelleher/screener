# movers/fetch.py

"""
Data Fetching Module for FMP Library.

Provides functions to retrieve data using the FMP API client.
Each function is cached to optimize performance.
"""

import streamlit as st


@st.cache_data
def fetch_exchanges(_fmp_client):
    """
    Retrieve all available stock exchanges.

    Args:
        _fmp_client (FMPClient): Initialized FMP API client.

    Returns:
        list of dict: all available stock exchanges.
    """
    return _fmp_client.stock_market.exchanges()
