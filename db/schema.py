# db/schema.py

import sqlalchemy as db

# Define the metadata object
metadata = db.MetaData()

# Define the Student table
Student = db.Table(
    "Student",
    metadata,
    db.Column("Id", db.Integer(), primary_key=True, autoincrement=True),
    db.Column("Name", db.String(255), nullable=False),
    db.Column("Major", db.String(255), default="Math"),
    db.Column("Pass", db.Boolean(), default=True),
)
