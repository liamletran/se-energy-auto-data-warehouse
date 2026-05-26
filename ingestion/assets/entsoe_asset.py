import pandas as pd
from dagster import asset, AssetExecutionContext, Config
from dagster_duckdb import DuckDBResource
from ingestion.clients.entsoe_client import ENTSOEClient


client = ENTSOEClient()


class ENTSOEAnaConfig(Config):
    start_date: str = None
    end_date: str = None


def _get_default_window() -> tuple[pd.Timestamp, pd.Timestamp]:
    end = pd.Timestamp.now(tz="Europe/Stockholm").normalize()
    start = end - pd.Timedelta(days=2)
    return start, end


def _ensure_entsoe_tables(con):

    con.execute("""
        CREATE TABLE IF NOT EXISTS entsoe_generation_raw (
            datetime                TIMESTAMPTZ,
            production_type         VARCHAR,
            actual_generation_mw    DOUBLE,
            bidding_zone            VARCHAR,
            ingested_at             TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS entsoe_prices_raw (
            datetime                TIMESTAMPTZ,
            price_eur_mwh           DOUBLE,
            bidding_zone            VARCHAR,
            ingested_at             TIMESTAMPTZ
        )
    """)


@asset(group_name="entsoe")
def entsoe_generation_raw(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    config: ENTSOEAnaConfig,
) -> None:

    if config.start_date and config.end_date:
        context.log.info(
            f"BACKFILL MODE IS ACTIVATED: {config.start_date} -> {config.end_date}"
        )
        start = pd.Timestamp(config.start_date, tz="Europe/Stockholm")
        end = pd.Timestamp(config.end_date, tz="Europe/Stockholm")
    else:
        context.log.info("DAILY RUN MODE IS ACTIVATED")
        start, end = _get_default_window()

    all_frames = []
    for zone in client.bidding_zones.keys():
        df = client.fetch_generation(start=start, end=end, zone=zone)
        if df.empty:
            context.log.warning(f"No generation data for zone {zone}")
            continue
        all_frames.append(df)
        context.log.info(f"Fetched {zone}: {len(df)} rows")

    if not all_frames:
        raise RuntimeError("No generation data returned.")

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined[
        [
            "datetime",
            "production_type",
            "actual_generation_mw",
            "bidding_zone",
            "ingested_at",
        ]
    ]

    with duckdb.get_connection() as con:
        _ensure_entsoe_tables(con)
        con.execute(
            "DELETE FROM entsoe_generation_raw WHERE datetime >= ? AND datetime < ?",
            [start, end],
        )
        con.execute("INSERT INTO entsoe_generation_raw BY NAME SELECT * FROM combined")
        row_count = con.execute(
            "SELECT COUNT(*) FROM entsoe_generation_raw WHERE datetime >= ? AND datetime < ?",
            [start, end],
        ).fetchone()[0]

    context.add_output_metadata({"rows_loaded": row_count})


@asset(group_name="entsoe")
def entsoe_prices_raw(
    context: AssetExecutionContext, duckdb: DuckDBResource, config: ENTSOEAnaConfig
) -> None:
    if config.start_date and config.end_date:
        start = pd.Timestamp(config.start_date, tz="Europe/Stockholm")
        end = pd.Timestamp(config.end_date, tz="Europe/Stockholm")
    else:
        start, end = _get_default_window()

    all_frames = []
    for zone in client.bidding_zones.keys():
        df = client.fetch_day_ahead_prices(start=start, end=end, zone=zone)
        if df.empty:
            continue
        all_frames.append(df)

    if not all_frames:
        raise RuntimeError("No price data returned.")
    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined[["datetime", "price_eur_mwh", "bidding_zone", "ingested_at"]]

    with duckdb.get_connection() as con:
        _ensure_entsoe_tables(con)
        con.execute(
            "DELETE FROM entsoe_prices_raw WHERE datetime >= ? AND datetime < ?",
            [start, end],
        )
        con.execute("INSERT INTO entsoe_prices_raw BY NAME SELECT * FROM combined")
        row_count = con.execute(
            "SELECT COUNT(*) FROM entsoe_prices_raw WHERE datetime >= ? AND datetime < ?",
            [start, end],
        ).fetchone()[0]

    context.add_output_metadata({"rows_loaded": row_count})
