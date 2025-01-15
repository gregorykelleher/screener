# equities_aggregator/db_integration.py

# import asyncio
# from sqlite3 import IntegrityError
# from typing import List

# from database import (
#     EquityIdentity as EquityIdentityDB,
#     FinancialEquity as FinancialEquityDB,
#     SessionLocal,
# )

# from equity_aggregator.schemas import EquityProfile
# from equity_aggregator.pipeline import aggregate_and_normalise_equities


# def _to_db_record(equity: EquityProfile) -> EquityIdentityDB:
#     """
#     Convert an EquityProfile instance into a EquityIdentityDB record,
#     with its associated FinancialEquityDB record attached via relationship.
#     """
#     canon = equity.equity_identity
#     equity_identity_record = EquityIdentityDB(
#         isin=canon.isin,
#         name=canon.name,
#         symbol=canon.symbol,
#     )
#     if equity.financial:
#         mics_list = equity.financial.mics or []
#         mics_str = ",".join(mics_list) if mics_list else None
#         financial_record = FinancialEquityDB(
#             isin=canon.isin,
#             mics=mics_str,
#             currency=equity.financial.currency,
#             last_price=equity.financial.last_price,
#         )
#         equity_identity_record.financial = financial_record
#     return equity_identity_record


# def _persist_equities(db_records: List[EquityIdentityDB]) -> None:
#     """
#     Persist a list of EquityIdentityDB records into the database.
#      (with cascaded FinancialEquity) to the database.
#     """
#     with SessionLocal() as session:
#         try:
#             session.add_all(db_records)
#             session.commit()
#             print("[DEBUG] Successfully persisted equity records")
#         except IntegrityError:
#             session.rollback()
#         except Exception as e:
#             session.rollback()
#             print("[ERROR] Failed to persist equity records:", e)
#             raise


# def populate_equities_db() -> None:
#     """
#     Orchestrates equity data population in the database:
#       1) Aggregate & normalise equity data,
#       2) Convert equtiy data to DB models,
#       3) Persist in the database.
#     """
#     # 1) Get the domain-level equities from aggregator
#     aggregated_equities = asyncio.run(aggregate_and_normalise_equities())

#     # 2) Convert each domain object to DB model
#     db_records = [_to_db_record(eq) for eq in aggregated_equities]

#     # 3) Persist the final result
#     _persist_equities(db_records)
