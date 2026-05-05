from utils.ensure_db import ensure_db
from dagster_duckdb import DuckDBResource
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)
from ingestion.assets.entsoe_asset import entsoe_generation_raw, entsoe_prices_raw
from ingestion.assets.frankfurter_asset import fx_rates_daily, fx_rates_backfill


DB_PATH = ensure_db()
duckdb_resource = DuckDBResource(database=DB_PATH)


# ENTSO-E daily job
entsoe_daily_job = define_asset_job(
    "entsoe_daily_ingest", selection=[entsoe_generation_raw, entsoe_prices_raw]
)
entsoe_daily_schedule = ScheduleDefinition(
    job=entsoe_daily_job,
    cron_schedule="30 14 * * *",  # 14:30 UTC
    execution_timezone="Europe/Stockholm",
)


# Frankfurter daily job
fx_daily_job = define_asset_job("fx_daily_ingest", selection=[fx_rates_daily])
fx_daily_schedule = ScheduleDefinition(
    job=fx_daily_job,
    cron_schedule="30 14 * * *",  # 14:30 UTC
    execution_timezone="Europe/Stockholm",
)


# Definitions for all jobs
defs = Definitions(
    assets=[
        entsoe_generation_raw,
        entsoe_prices_raw,
        fx_rates_daily,
        fx_rates_backfill,
    ],
    schedules=[entsoe_daily_schedule, fx_daily_schedule],
    resources={
        "duckdb": duckdb_resource,
    },
    executor=in_process_executor,
)
