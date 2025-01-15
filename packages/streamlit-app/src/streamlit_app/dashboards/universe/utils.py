# universe/utils.py

import pandas as pd
from typing import Dict, Any
from st_aggrid import GridOptionsBuilder, JsCode, GridUpdateMode, AgGrid

# Custom JS for styling the "Change %" column
CHANGE_PERCENTAGE_JS = JsCode("""
function(params) {
    return params.value > 0 
        ? {'color': 'green'} 
        : (params.value < 0 ? {'color': 'red'} : {})
}
""")


def format_market_cap(value):
    if value is None:
        return "N/A"
    if value >= 1e9:
        return f"{value / 1e9:.2f}B"
    elif value >= 1e6:
        return f"{value / 1e6:.2f}M"
    elif value >= 1e3:
        return f"{value / 1e3:.2f}K"
    else:
        return str(value)


def _load_equities(conn) -> pd.DataFrame:
    """
    Fetch data from the 'equity_identities' table,
    select relevant columns, and rename them.
    """
    equities = conn.query("SELECT * FROM equity_identities")

    df = equities[
        [
            "isin",
            "name",
            "symbol",
        ]
    ].copy()

    # Format the Market Cap column to a human-readable string
    # df["market_cap"] = df["market_cap"].apply(format_market_cap)

    df = df.rename(
        columns={
            "isin": "ISIN",
            "name": "Name",
            "symbol": "Symbol",
        }
    )
    return df


def _configure_aggrid_options(df) -> Dict[str, Any]:
    """
    Build and return the grid options for AgGrid.
    """
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=True, paginationPageSize=50)
    gb.configure_selection("single")
    gb.configure_default_column(
        editable=False,
        sortable=True,
        resizable=True,
        suppressHeaderFilterButton=True,
        suppressHeaderMenuButton=True,
    )
    # Apply JS-based cell styling to the "Change %" column
    gb.configure_column("Change %", cellStyle=CHANGE_PERCENTAGE_JS)
    return gb.build()


def _display_aggrid(df, grid_options) -> None:
    """
    Display the DataFrame with AgGrid and the provided grid options.
    """

    AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=600,
    )


def render_equities(conn) -> None:
    """
    Fetch equities from the 'canonical_equities' table and render them using AgGrid.
    """

    # Load company profiles
    df = _load_equities(conn)

    # Configure AgGrid options
    grid_options = _configure_aggrid_options(df)

    # Display the AgGrid
    _display_aggrid(df, grid_options)
