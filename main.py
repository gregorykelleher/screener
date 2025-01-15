# main.py

import sys
import streamlit as st
from src.fmp_client import setup_fmp_client

from config import main_config, mock_config

# Set the page configuration
st.set_page_config(
    page_title="Screener App",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():

    # Setup and store the FMP Client
    st.session_state.fmp_client = setup_fmp_client()

    # Set Overview as the default page
    pages = {
        "Dashboards": [
            st.Page("src/dashboards/overview/overview.py", title="Overview", icon="📊", default=True)
        ],
        "Universe": [
            st.Page("src/universe/universe.py", title="Universe", icon="🌎")
        ],
        "Portfolio": [
            st.Page("src/portfolio/portfolio.py", title="Portfolio", icon="🧰")
        ],
        "Analyses": [
            st.Page("src/analyses/asset_analysis/asset_analysis.py", title="Asset Analysis", icon="📝")
        ],
    }

    # Run the pages
    pg = st.navigation(pages)
    pg.run()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() == "test":
        st.session_state["config"] = mock_config
        st.sidebar.warning('Test mode: Mock data is being used instead of real data.', icon="⚠️")
    else:
        st.session_state["config"] = main_config
    main()