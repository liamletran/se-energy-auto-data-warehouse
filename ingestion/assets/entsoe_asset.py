from dagster import asset
from ingestion.clients.entsoe_client import ENTSOEClient
import pandas as pd
import duckdb
import os

client = ENTSOEClient()

DB_PATH = "duckdb/warehouse.duckdb"


def _window() -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Fetch rolling 2-day window ending at midnight today (Stockholm time).
    """
    end = pd.Timestamp.now(tz="Europe/Stockholm").normalize()
    start = end - pd.Timedelta(days=2)
    return start, end


def _ensure_db():
    os.makedirs("duckdb", exist_ok=True)
