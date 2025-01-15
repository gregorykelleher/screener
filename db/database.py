# db/database.py

import sqlalchemy as db

# Import your schema objects
from schema import metadata, Student

# Database URL from secrets.toml
DATABASE_URL = "sqlite:///data/stocks_universe.db"

# Create the engine
engine = db.create_engine(DATABASE_URL, echo=True)

# Create or update the database schema and insert seed data
with engine.begin() as conn:
    # Create tables if they do not exist
    metadata.create_all(conn)

    # Insert data
    query = db.insert(Student)
    values_list = [
        {"Id": 2, "Name": "Nisha", "Major": "Science", "Pass": False},
        {"Id": 3, "Name": "Natasha", "Major": "Math", "Pass": True},
        {"Id": 4, "Name": "Ben", "Major": "English", "Pass": False},
    ]
    conn.execute(query, values_list)
