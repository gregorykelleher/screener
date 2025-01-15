# analyses/asset_analysis.py

import streamlit as st
import streamlit.components.v1 as components
import src.analyses.asset_analysis.utils as ut

def render_tradingview_widget(symbol: str, placeholder):
    """
    Renders the TradingView widget for the given symbol into the provided placeholder.
    """
    valid_symbol = ut.map_commodity_symbol(symbol)
    tradingview_html = f"""
    <!-- TradingView Widget BEGIN -->
    <div style="height:900px;width:100%;overflow:hidden;">
      <div class="tradingview-widget-container" style="height:100%;width:100%;">
        <div class="tradingview-widget-container__widget" style="height:calc(100% - 32px);width:100%;"></div>
        <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js" async>
        {{
          "symbol": "{valid_symbol}",
          "timezone": "Europe/Vienna",
          "theme": "light",
          "style": "1",
          "locale": "en",
          "hide_legend": false,
          "withdateranges": true,
          "range": "60M",
          "allow_symbol_change": false,
          "save_image": false,
          "calendar": false,
          "studies": ["STD;RSI"]
        }}
        </script>
      </div>
    </div>
    <!-- TradingView Widget END -->
    """
    placeholder.empty()
    with placeholder:
        components.html(tradingview_html, height=1000, scrolling=False)

def search_callback():
    """
    Callback invoked when the search text input changes.
    Renders the TradingView widget using the search query.
    """
    symbol = st.session_state.asset_analysis_search_symbol
    if symbol:
        render_tradingview_widget(symbol, chart_placeholder)


def get_symbol_to_show():
    """
    Returns the symbol to show.
    Priority is given to the search symbol over the asset_symbol stored in session state.
    """
    search_symbol = st.session_state.get("asset_analysis_search_symbol", "").strip()
    if search_symbol:
        return search_symbol
    
    # If no search symbol is provided, default to asset_symbol.
    return st.session_state.get("asset_symbol")

# Create two columns for header: one for the title and one for the search box.
title_col, search_col = st.columns([3, 1])

# Use the helper function to choose the symbol.
symbol_to_show = get_symbol_to_show()

with title_col:
    if symbol_to_show:
        st.title(f"{symbol_to_show} Analysis")
    else:
        st.title("Asset Analysis")

with search_col:
    st.text_input(
        "Search",
        placeholder="Enter asset symbol 🔍",
        label_visibility="hidden",
        key="asset_analysis_search_symbol",
        on_change=search_callback
    )

st.divider()

# Create the placeholder to display the chart below the title and divider.
chart_placeholder = st.empty()

# Render the chart if a symbol is available.
if symbol_to_show:
    render_tradingview_widget(symbol_to_show, chart_placeholder)
else:
    st.warning("No asset selected. Please select an asset, or search for a symbol.")
