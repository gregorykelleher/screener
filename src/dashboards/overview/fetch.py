# fetch.py

"""
Data Fetching Module for FMP Library.

Provides functions to retrieve data using the FMP API client.
Each function is cached to optimize performance.
"""

import streamlit as st


@st.cache_data
def fetch_daily_gainers(_fmp_client):
    """
    Retrieve today's top gaining stocks.

    Args:
        _fmp_client (FMPClient): Initialized FMP API client.

    Returns:
        list of dict: Data of daily gainers.
    """
    return _fmp_client.stock_market.gainers()


@st.cache_data
def fetch_daily_losers(_fmp_client):
    """
    Retrieve today's top losing stocks.

    Args:
        _fmp_client (FMPClient): Initialized FMP API client.

    Returns:
        list of dict: Data of daily losers.
    """
    return _fmp_client.stock_market.losers()


@st.cache_data
def fetch_sector_performance(_fmp_client):
    """
    Get performance data for market sectors.

    Args:
        _fmp_client (FMPClient): Initialized FMP API client.

    Returns:
        list of dict: Sector performance details.
    """
    return _fmp_client.stock_market.sectors_performance()


@st.cache_data
def fetch_commodities_performance(_fmp_client):
    """
    Get performance data for commodities.

    Args:
        _fmp_client (FMPClient): Initialized FMP API client.

    Returns:
        list of dict: Commodities performance details.
    """
    return _fmp_client.commodities.commodities_performance()
