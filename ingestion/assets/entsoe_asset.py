from dagster import asset
from ingestion.clients.entsoe_client import ENTSOEClient
from utils.ensure_db import ensure_db
import pandas as pd
import duckdb


client = ENTSOEClient()
DB_PATH = ensure_db()


def _window() -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Fetch rolling 2-day window ending at midnight today (Stockholm time).
    """
    end = pd.Timestamp.now(tz="Europe/Stockholm").normalize()
    start = end - pd.Timedelta(days=2)
    return start, end


@asset
def entsoe_generation_raw() -> str:
    start, end = _window()
    all_frames = []

    for zone in client.bidding_zones.keys():
        df = client.fetch_generation(start=start, end=end, zone=zone)
        if df.empty:
            print(f"  [WARN] No generation data for zone {zone}")
            print(f"({start.date()} → {end.date()})")
            continue
        all_frames.append(df)
        print(f"  [OK] {zone}: {len(df)} rows fetched")

    if not all_frames:
        raise RuntimeError(
            "No generation data returned for any zone. Check API token and data availability."
            f"({start.date()} → {end.date()})"
        )

    combined = pd.concat(all_frames, ignore_index=True)

    con = duckdb.connect(DB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS entsoe_generation_raw (
            datetime TIMESTAMPTZ,
            production_type VARCHAR,
            actual_generation_mw DOUBLE,
            bidding_zone VARCHAR,
            ingested_at TIMESTAMPTZ
        )
        """
    )

    # Idempotent upsert: delete window data firstm then insert fresh data
    # Safe to run multiple times a day without duplicates
    con.execute(
        """
    DELETE FROM entsoe_generation_raw WHERE datetime >= ? AND datetime < ?
    """,
        [start, end],
    )

    con.execute("INSERT INTO entsoe_generation_raw SELECT * FROM combined")

    row_count = con.execute(
        "SELECT COUNT(*) FROM entsoe_generation_raw"
        " WHERE datetime >= ? AND datetime < ?",
        [start, end],
    ).fetchone()[0]

    con.close()
    print(f"  [DB] {row_count} rows loaded into entsoe_generation_raw")
    return DB_PATH


@asset
def entsoe_prices_raw() -> str:
    # DB_PATH = ensure_db()
    start, end = _window()
    all_frames = []

    for zone in client.bidding_zones.keys():
        df = client.fetch_day_ahead_prices(start=start, end=end, zone=zone)
        if df.empty:
            print(f"  [WARN] No price data for zone {zone}")
            print(f"({start.date()} → {end.date()})")
            continue
        all_frames.append(df)
        print(f"  [OK] {zone}: {len(df)} rows fetched")

    if not all_frames:
        raise RuntimeError(
            "No price data returned for any zone. Check API token and data availability."
            f"({start.date()} → {end.date()})"
        )

    combined = pd.concat(all_frames, ignore_index=True)

    con = duckdb.connect(DB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS entsoe_prices_raw (
            datetime TIMESTAMPTZ,
            price_eur_mwh DOUBLE,
            bidding_zone VARCHAR,
            ingested_at TIMESTAMPTZ
        )
        """
    )

    # Idempotent upsert: delete window data firstm then insert fresh data
    # Safe to run multiple times a day without duplicates
    con.execute(
        """
    DELETE FROM entsoe_prices_raw
    WHERE datetime >= ? AND datetime < ?
    """,
        [start, end],
    )

    con.execute("INSERT INTO entsoe_prices_raw SELECT * FROM combined")

    row_count = con.execute(
        "SELECT COUNT(*) FROM entsoe_prices_raw WHERE datetime >= ? AND datetime < ?",
        [start, end],
    ).fetchone()[0]

    con.close()
    print(f"  [DB] {row_count} rows loaded into entsoe_prices_raw")
    return DB_PATH
