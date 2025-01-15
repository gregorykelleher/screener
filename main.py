# main.py

import sys

import streamlit as st

from src import main_config, initialise_services
from tests import mock_config

from page_utils import create_navigation_mapping


def _set_app_configuration():
    """
    Determines if the app is in test mode and sets the configuration accordingly.

    Returns:
        config: The configuration object (either mock_config or main_config).
    """
    # Determine if in test mode
    is_test_mode = "test" in sys.argv

    # Set the configuration based on mode selection
    if is_test_mode:
        config = mock_config
    else:
        config = main_config
        initialise_services()

    # Display warning if in test mode
    if is_test_mode:
        st.sidebar.warning(
            "Test mode: Mock data is being used instead of real data.", icon="⚠️"
        )

    return config


def main():
    """
    The main function that sets up and runs the Streamlit app.
    """

    # Set the page configuration
    st.set_page_config(
        page_title="Screener App",
        page_icon="🏠",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Set the app configuration based on mode selection
    config = _set_app_configuration()

    # Create the navigation mapping based on the configuration
    navigation_mapping = create_navigation_mapping(config)

    # Run the navigation pages
    try:
        st.navigation(navigation_mapping).run()
    except Exception as e:
        st.error(f"An error occurred while running the app: {e}")


if __name__ == "__main__":
    main()
