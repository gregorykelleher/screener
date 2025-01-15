# db/schemas.py

from sqlalchemy import (
    Column,
    String,
    Numeric,
    ForeignKey,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class EquityIdentity(Base):
    """
    Database table for the core equity identity data.

    Fields:
      - isin: 12-character unique identifier (primary key)
      - name: Equity name
      - symbol: Equity symbol
    """

    __tablename__ = "equity_identities"

    isin = Column(String(12), primary_key=True)
    name = Column(String(255), nullable=False)
    symbol = Column(String(50), nullable=False, index=True)

    # One-to-one relationship with FinancialEquity
    financial = relationship(
        "FinancialEquity",
        back_populates="equity_identity",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<EquityIdentity(isin={self.isin}, name={self.name}, symbol={self.symbol})>"


class FinancialEquity(Base):
    """
    Database table for the financial supplementary data.

    Fields:
      - isin: Foreign key from equity identity table (primary key)
      - mics: A string (or comma-separated list) of market identifier codes.
      - currency: Currency code in which the equity's price is denominated.
      - last_price: The latest trade price as a Numeric type.
      - market_cap: The latest market capitalisation as a Numeric type.
    """

    __tablename__ = "financial_equities"

    isin = Column(String(12), ForeignKey("equity_identities.isin"), primary_key=True)
    mics = Column(String(255))
    currency = Column(String(10))
    last_price = Column(Numeric)
    market_cap = Column(Numeric)

    # One-to-one relationship with EquityIdentity
    equity_identity = relationship("EquityIdentity", back_populates="financial")

    def __repr__(self):
        return (
            f"<FinancialEquity(isin={self.isin}, mics={self.mics}, "
            f"currency={self.currency}, last_price={self.last_price}, "
            f"market_cap={self.market_cap})>"
        )
