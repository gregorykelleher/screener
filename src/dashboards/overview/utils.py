# overview/utils.py

import streamlit as st
import pandas as pd

from st_aggrid import (
    AgGrid,
    GridUpdateMode,
    GridOptionsBuilder,
    JsCode
)

CHANGE_PERCENTAGE_JS = JsCode("""
function(params) {
    return params.value > 0 
        ? {'color': 'green'} 
        : (params.value < 0 ? {'color': 'red'} : {})
}
""")

def _retrieve_fmp():
    """
    Retrieve 'fmp_client' from session state.
    """
    fmp = st.session_state.get('fmp_client')
    if fmp is None:
        st.error("fmp client is not initialized. Please set 'fmp_client' in session state.")
        st.stop()
    return fmp

def _ensure_numeric_changes_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures that 'changesPercentage' is a numeric column by converting values as needed.
    """
    if 'changesPercentage' in df.columns:
        df['changesPercentage'] = pd.to_numeric(df['changesPercentage'], errors='coerce')
    return df

def _get_sort_order(config, default_col: str = "changesPercentage", default_order: str = "desc") -> str:
    """
    Extracts the sort order from the configuration instance.
    """
    # Use the default_sort defined in the TableModel. If not present, fall back to the defaults.
    sort_config = config.default_sort or [{ "colId": default_col, "sort": default_order }]
    return sort_config[0].get("sort", default_order)

def _build_grid_options(df: pd.DataFrame, config) -> dict:
    """
    Build grid options from a DataFrame and TableModel configuration.
    
    :param df: The data as a DataFrame.
    :param config: A TableModel instance.
    :return: Grid options as a dict.
    """
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_selection('single')

    # Set column names based on the columns_mapping from the configuration
    for col_key, header in config.columns_mapping.items():
        gb.configure_column(col_key, headerName=header)
    
    # Hide columns as specified in the configuration
    for col_to_hide in config.columns_to_hide:
        gb.configure_column(col_to_hide, hide=True)

    # Ensure all columns are sortable by default
    gb.configure_default_column(sortable=True)

    sort_order = _get_sort_order(config)
    cp_column_header = config.columns_mapping.get("changesPercentage", "changesPercentage")

    gb.configure_column(
        "changesPercentage",
        headerName=cp_column_header,
        cellStyle=CHANGE_PERCENTAGE_JS,
        sort=sort_order
    )

    return gb.build()

def _navigate_to_analysis(selected_data: dict) -> None:
    """
    Set the selected asset in session state and navigate to the analysis page for the asset.
    """
    st.session_state["asset_symbol"] = selected_data.get("symbol")
    st.switch_page("src/analyses/asset_analysis/asset_analysis.py")

def render_table(col, config) -> None:
    """
    Render a table in the given Streamlit column using the provided TableModel configuration.
    
    :param col: The Streamlit column to render the table.
    :param config: A TableModel instance.
    """
    with col:
        st.subheader(f"{config.title} {config.icon}")
        try:
            # Fetch data using the TableModel's fetch_func
            fmp_client = _retrieve_fmp()
            data = config.fetch_func(fmp_client)

            # Convert the data to a DataFrame and ensure 'changesPercentage' is numeric
            df = pd.DataFrame(data)
            df = _ensure_numeric_changes_percentage(df)

            if df.empty:
                st.info(f"No {config.title} data available.")
                return

            grid_options = _build_grid_options(df, config)

            grid_response = AgGrid(
                df,
                gridOptions=grid_options,
                theme='streamlit',
                height=600,
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=True,
                update_mode=GridUpdateMode.SELECTION_CHANGED
            )

            selected_rows = grid_response.get("selected_rows")
            if selected_rows is not None:
                # Use the first selected row for navigation
                _navigate_to_analysis(selected_rows.iloc[0])
        except Exception as e:
            st.error(f"Error fetching {config.title.lower()} data: {e}")
