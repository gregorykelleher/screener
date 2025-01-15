# database/db.py

import logging
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

from .schemas import Base

# Set the root logger to INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set SQLAlchemy engine logger to only show warnings or higher
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# Define the database URL
DATABASE_URL = "sqlite:///data/equities.db"

# Create the engine
engine = db.create_engine(DATABASE_URL, echo=False)

# Create the session factory
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Create or update the database schema
with engine.begin() as conn:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Base.metadata.create_all(conn)


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
