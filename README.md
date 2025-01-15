# Screener

## Create Virtual Environment

`uv venv screener-venv --python 3.12`

### Activate Virtual Environment

`source screener-venv/bin/activate`

### Dectivate Virtual Environment

`source deactivate`

### Install Packages

`uv tool install ruff`
`uv pip install -r pyproject.toml`

### Setup Streamlit Secrets

`mkdir .streamlit`
`touch ./streamlit/secrets.toml`
`echo fmp_api_key="12345678" > secrets.toml`

### Run main Screener application

`streamlit run main.py`

### Run Screener application with mock data

`streamlit run main.py test`

### Run individual package

`uv run --package example-package main`

### Run dependency to individual package

`uv add --package example-package dependency`

### Run the formatter

`uv run ruff format`

### Run the linter

`uv run ruff check`

### Initialise database

`python db/database.py`

### query database

```bash
sqlite3 data/stocks_universe.db
.tables
SELECT * FROM Student;
```
