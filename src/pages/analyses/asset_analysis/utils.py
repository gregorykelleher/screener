# analyses/utils.py

import streamlit as st
import streamlit.components.v1 as components


def _map_commodity_symbol(incoming_symbol: str) -> str:
    """
    Maps an incoming commodity asset symbol to a valid symbol for TradingView.

    If the asset symbol is recognized as one having an invalid format for TradingView,
    the method returns the corrected symbol. Otherwise, it returns the incoming symbol.

    :param incoming_symbol: The asset symbol as received.
    :return: A valid asset symbol for TradingView.
    """
    COMMODITY_SYMBOL_MAPPING = {
        "ZTUSD": "CBOT:ZT1!",  # 2-Year T-Note Futures
        "ZNUSD": "CBOT:ZN1!",  # 10-Year T-Note Futures
        "ALIUSD": "COMEX:ALI1!",  # Aluminum Futures
        "HGUSD": "COMEX:HG1!",  # Copper Futures
        "GCUSD": "COMEX:GC1!",  # Gold Futures
        "SIUSD": "COMEX:SI1!",  # Silver Futures
        "RBUSD": "NYMEX:RB1!",  # Gasoline RBOB Futures
        "CLUSD": "NYMEX:CL1!",  # Crude Oil Futures
        "NGUSD": "NYMEX:NG1!",  # Natural Gas Futures
        "BZUSD": "NYMEX:BB1!",  # Brent Crude Oil Futures
        "KEUSX": "CBOT:KE1!",  # Wheat Futures
        "ZCUSX": "CBOT:ZC1!",  # Corn Futures
        "LBUSD": "CME:LBR1!",  # Lumber Futures
        "ZOUSX": "CBOT:ZO1!",  # Oat Futures
        "KCUSX": "NYMEX:KT1!",  # Coffee Futures
    }

    return COMMODITY_SYMBOL_MAPPING.get(incoming_symbol, incoming_symbol)


def _generate_tradingview_html(symbol: str) -> str:
    """
    Generate the HTML code for embedding the TradingView widget.

    Args:
        symbol (str): The symbol to render.

    Returns:
        str: HTML string for the TradingView widget.
    """
    valid_symbol = _map_commodity_symbol(symbol)
    html = f"""
    <!-- TradingView Widget BEGIN -->
    <div style="height:900px; width:100%; overflow:hidden;">
      <div class="tradingview-widget-container" style="height:100%; width:100%;">
        <div class="tradingview-widget-container__widget" style="height:calc(100% - 32px); width:100%;"></div>
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
    return html


def render_tradingview_widget(symbol: str, placeholder):
    """
    Render the TradingView widget for the given symbol into the provided placeholder.

    Args:
        symbol (str): The asset symbol to visualize.
        placeholder: A Streamlit placeholder (e.g., st.empty()).
    """
    tradingview_html = _generate_tradingview_html(symbol)
    placeholder.empty()
    with placeholder:
        components.html(tradingview_html, height=1000, scrolling=False)


def get_symbol_to_show() -> str:
    """
    Determine and return the symbol to show.

    Priority is given to the search symbol stored in session_state.

    Returns:
        str: Symbol to display.
    """
    search_symbol = st.session_state.get("asset_analysis_search_symbol", "").strip()
    if search_symbol:
        return search_symbol
    return st.session_state.get("asset_symbol", "")
