# Screener

## Create Virtual Environment

`uv venv screener-venv --python 3.12`

## Activate Virtual Environment

`source screener-venv/bin/activate`

## Install Packages

`uv pip install -r pyproject.toml`

## Setup Streamlit Secrets

`mkdir .streamlit`
`touch secrets.toml`
`echo fmp_api_key="12345678" > secrets.toml`

## Run main Screener application

`streamlit run main.py`

## Run Screener application with mock data

`streamlit run main.py test`

## Run individual package

`uv run --package example-package main`

## Run dependency to individual package

`uv add --package example-package dependency`
