# src/services/database_service.py

import streamlit as st


def setup_database_client():
    """
    Set up a single DB connection and cache it in session state.
    """
    return st.connection("stocks_universe", type="sql")


def retrieve_database_client():
    """
    Retrieves the DB connection from session state.
    Assumes setup_db_connection() has been called at least once.
    """
    return st.session_state.get("database_client")
