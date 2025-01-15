# streamlit_app/main.py

import streamlit as st


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

    pages = st.navigation(
        {
            "Dashboards": [
                st.Page(
                    "dashboards/universe/universe.py",
                    title="Universe",
                    icon="🌍",
                    default=True,
                )
            ],
            "Analyses": [
                st.Page(
                    "analyses/equity_analysis.py", title="Equity Analysis", icon="📝"
                )
            ],
        }
    )

    pages.run()


if __name__ == "__main__":
    main()
