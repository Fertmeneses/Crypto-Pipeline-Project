# main2.py
# Main code to load postgres database with local information.

# Import libraries:

import os
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, JSON, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# WARNING: if you want to run the script directly and not using docker, 
# ... then uncomment the following lines

# from dotenv import load_dotenv # (pip install if not installed)
# load_dotenv("../../.env")

# Get local environment variables:
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1") # Database host connection
PASSWORD = os.getenv("POSTGRES_PASSWORD") # Postgres password
DATABASE_URL = f"postgresql://postgres:{PASSWORD}@{DB_HOST}:5432/postgres" # Database url (docker)

# WARNING: if you want to run the script directly and not using docker, 
# ... then uncomment the following line

# DATABASE_URL = f"postgresql://postgres:{PASSWORD}@127.0.0.1:5432/postgres" # Database url (python)

# Create the SQLAlchemy Engine, which manages the database connection pool
engine = create_engine(DATABASE_URL)

# Optional: Check if the connection works before proceeding
try:
    with engine.connect() as connection:
        print("✅ Successfully connected to the PostgreSQL database.")
except Exception as e:
    print(f"❌ Failed to connect to the database: {e}")
    raise SystemExit(1)

# Create a configured "Session" class and a Base class for defining ORM models:
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class CoinDailyData(Base):
    """
    SQLAlchemy ORM model for daily cryptocurrency data.
    --- Inputs ---
    {Base} [DeclarativeMeta]: SQLAlchemy declarative base that this model inherits from.

    --- Returns ---
    ORM-mapped class representing the 'crypto_daily_data' table.
    """
    # Define table name:
    __tablename__ = "crypto_daily_data"

    # Define schema:
    id = Column(Integer, primary_key=True, autoincrement=True)
    coin_id = Column(String(64), nullable=False)
    price_usd = Column(Numeric)
    date = Column(Date, nullable=False)
    response_json = Column(JSON, nullable=False)

    # Enforce one record per (coin_id, date)
    __table_args__ = (UniqueConstraint('coin_id', 'date', name='unique_coin_date'),)

def extract_coin_and_date(filename):
    """
    Extract coin_id and date from a filename.
    --- Inputs ---
    {filename} [string]: path to file, assumes the following format: "coinid_YYYY_MM_DD.json"

    ---Returns---
    {coin_id} [str]: Coin identifier.
    {record_date} [date object]: Date in YYYY-MM-DD format.
    """
    # Normalize to basename and strip extension:
    basename = os.path.basename(filename)
    name, _ = os.path.splitext(basename)

    # Expected 4 tokens: coin_id, YYYY, MM, DD:
    coin_id, y, m, d = name.split('_')

    # Build a date object from tokens:
    record_date = datetime.strptime(f"{y}_{m}_{d}", "%Y_%m_%d").date()

    return coin_id, record_date

def extract_price_usd(json_data):
    """
    Extract USD price from a CoinGecko history JSON data.
    --- Inputs ---
    {json_data} [dict]: Parsed JSON object returned by the API.

    --- Returns ---
    [float | None] : USD price if available; otherwise None. 
    """
    try:
        return json_data.get("market_data", {}).get("current_price", {}).get("usd")
    except Exception:
        return None

def populate_crypto_daily_data(data_folder):
    """
    Populate the 'crypto_daily_data' table from local JSON files.
    --- Inputs ---
    {data_folder} [string]: path to data folder which stores the .json files

    --- Returns ---
    None: Performs inserts into the database and prints an import summary.
    """

    # Initiate:
    session = SessionLocal()
    file_count = 0

    # Iterate all files in the folder; only process *.json
    for filename in os.listdir(data_folder):
        if filename.endswith('.json'):
            # Get full path:
            full_path = os.path.join(data_folder, filename)

            # Derive metadata from filename:
            coin_id, record_date = extract_coin_and_date(filename)

            # Read data:
            with open(full_path, 'r') as f:
                data = json.load(f)

            # Extract USD price information:
            price_usd = extract_price_usd(data)

            # Build ORM instance for insertion:
            entry = CoinDailyData(
                coin_id=coin_id,
                price_usd=price_usd,
                date=record_date,
                response_json=data
            )

            # Insert row; if duplicate or other error, rollback and skip:
            try:
                session.add(entry)
                session.commit()
                file_count += 1
            except Exception as e:
                session.rollback()
                print(f"Skipping {filename}: {e}")

    # Close session:
    session.close()
    # Final log:
    print(f"Imported {file_count} records into crypto_daily_data.")

if __name__ == "__main__":
    # WARNING: Leave only the lines that works for your setup:

    # If running from docker:
    data_folder_path = os.path.join("/app", "codes", "1_task1", "crypto_datafiles")

    # If running directly from python (remember also to change lines at the top of the script):
    # data_folder_path = "../1_task1/crypto_datafiles/"

    populate_crypto_daily_data(data_folder_path)