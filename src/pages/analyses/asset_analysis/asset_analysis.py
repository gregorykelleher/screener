# analyses/asset_analysis.py

import streamlit as st

from page_utils import register_page

from .utils import render_tradingview_widget, get_symbol_to_show


@register_page(section="Analyses", title="Asset Analysis", icon="📝")
def generate_asset_analysis(config):
    """
    Generate the Asset Analysis page
    """

    def search_callback():
        """
        Search callback when the text input changes.
        Invokes the chart render with the new query symbol.
        """
        symbol = st.session_state.asset_analysis_search_symbol
        if symbol:
            render_tradingview_widget(symbol, chart_placeholder)

    # Initialize session state for search symbol if not already set
    if "asset_analysis_search_symbol" not in st.session_state:
        st.session_state.asset_analysis_search_symbol = ""

    # Define columns for title and search input.
    title_col, search_col = st.columns([3, 1])

    symbol_to_show = get_symbol_to_show()

    with title_col:
        st.title(f"{symbol_to_show} Analysis" if symbol_to_show else "Asset Analysis")

    with st.popover("Save"):
        st.button("Add to Watchlist", on_click=None, type="secondary", icon="🔍")
        st.button("Add to Favourites", on_click=None, type="secondary", icon="⭐")

    with search_col:
        st.text_input(
            "Search",
            placeholder="Enter asset symbol 🔍",
            label_visibility="hidden",
            key="asset_analysis_search_symbol",
            on_change=search_callback,
        )

    st.divider()

    # Create a placeholder for the chart.
    chart_placeholder = st.empty()

    if symbol_to_show:
        render_tradingview_widget(symbol_to_show, chart_placeholder)
    else:
        st.warning("No asset selected. Please select an asset, or search for a symbol.")
