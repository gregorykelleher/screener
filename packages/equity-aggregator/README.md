# Equities Aggregator

A Python package that retrieves equity data from multiple sources (Euronext, LSE, Xetra), normalizes them into a unified schema (`EquityProfile`). Provides support to persist in database.

- **Data Vendors**  
  Each exchange fetch script lives in `sources/<exchange>/fetch_equities.py`, calling the exchange’s API.  

- **Resolvers**  
  Wrap vendor fetches and convert raw data into `EquityProfile` objects.  

- **Aggregator**  
  - Gathers data from resolvers.  
  - Uses a currency converter to normalize all prices to USD.  
  - Groups & merges items by ISIN.  
  - Exposes a function `aggregate_and_normalise_equities()` that returns the final list of merged, USD‐normalized equities asychronously.  
  
- **Database Integration**  
  - Converts `EquityProfile` into SQLAlchemy models (`EquityIdentityDB`, `FinancialEquityDB`).  
  - Persists them via `SessionLocal`.  
  - The top‐level `populate_equities_db()` runs the full pipeline (fetch → normalize → merge → persist).

Core directory highlights:
- `currency_converter/` handles fetching live exchange rates (USD as base).  
- `equities_aggregator/aggregator.py` houses core transformation logic.  
- `equities_aggregator/db_integration.py` handles SQLAlchemy interactions.
