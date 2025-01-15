# movers/movers.py

import streamlit as st
from page_utils import register_page
from src.services import retrieve_fmp_client


@register_page(section="Dashboards", title="Movers", icon="📈")
def generate_movers(config):
    """
    Generate the Movers page using the provided configuration.

    Args:
        config (dict or object): The configuration dictionary or object.
    """
    # Page title
    st.title("Movers")
    st.divider()

    # Retrieve the configuration
    _config = config

    fmp_client = retrieve_fmp_client()

    # Make the API call to retrieve the list of stock exchanges.
    # (Assuming that the API returns a list of strings.)
    try:
        # The fetch_func is assumed to return a list of exchanges.
        data = _config.movers_config[0].fetch_func(fmp_client)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        data = []  # Fallback to an empty list if the API call fails

    with st.container():
        # Define the main columns with a width ratio
        # Using API data as options for the exchange dropdown.
        exchange_column, sector_column, time_frame_column = st.columns([1, 1, 2])

        with exchange_column:
            st.selectbox(
                "Exchange Selection",
                options=data if data else ["No exchanges available"],
                index=0
                if data
                else None,  # Set default index only if data is available.
                label_visibility="visible",
            )

        with sector_column:
            st.selectbox(
                "Sector Selection",
                ["Option A", "Option B", "Option C"],
                index=0,
                placeholder="Choose a sector",
                label_visibility="visible",
            )

        with time_frame_column:
            # Create nested columns inside time frame column to align the segmented control column to the right
            padding_column, segmented_control_column = st.columns([3, 1])

            with segmented_control_column:
                options = ["1D", "1W", "1M", "1Y"]
                st.segmented_control(
                    "Time Frame",
                    options,
                    selection_mode="single",
                    default="1D",
                    label_visibility="hidden",
                )
