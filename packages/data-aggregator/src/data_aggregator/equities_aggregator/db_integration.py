# equities_aggregator/db_integration.py

import asyncio
from sqlite3 import IntegrityError
from typing import List

from database import (
    CanonicalEquity as CanonicalEquityDB,
    FinancialEquity as FinancialEquityDB,
    SessionLocal,
)

from .schemas import EquityData
from .aggregator import aggregate_and_normalise_equities_async


def _to_db_record(equity: EquityData) -> CanonicalEquityDB:
    """
    Convert an EquityData instance into a CanonicalEquityDB record,
    with its associated FinancialEquityDB record attached via relationship.
    """
    canon = equity.canonical
    canonical_record = CanonicalEquityDB(
        isin=canon.isin,
        name=canon.name,
        symbol=canon.symbol,
    )
    if equity.financial:
        mics_list = equity.financial.mics or []
        mics_str = ",".join(mics_list) if mics_list else None
        financial_record = FinancialEquityDB(
            isin=canon.isin,
            mics=mics_str,
            currency=equity.financial.currency,
            last_price=equity.financial.last_price,
        )
        canonical_record.financial = financial_record
    return canonical_record


def _persist_equities(db_records: List[CanonicalEquityDB]) -> None:
    """
    Persist a list of CanonicalEquityDB records into the database.
     (with cascaded FinancialEquity) to the database.
    """
    with SessionLocal() as session:
        try:
            session.add_all(db_records)
            session.commit()
            print("[DEBUG] Successfully persisted equity records")
        except IntegrityError:
            session.rollback()
        except Exception as e:
            session.rollback()
            print("[ERROR] Failed to persist equity records:", e)
            raise


def populate_equities_db() -> None:
    """
    Orchestrates equity data population in the database:
      1) Aggregate & normalize equity data,
      2) Convert equtiy data to DB models,
      3) Persist in the database.
    """
    # 1) Get the domain-level equities from aggregator
    aggregated_equities = asyncio.run(aggregate_and_normalise_equities_async())

    # 2) Convert each domain object to DB model
    db_records = [_to_db_record(eq) for eq in aggregated_equities]

    # 3) Persist the final result
    _persist_equities(db_records)
