from dotenv import load_dotenv

load_dotenv()
from dagster import Definitions, ScheduleDefinition, define_asset_job
from ingestion.assets.entsoe_asset import entsoe_generation_raw, entsoe_prices_raw


entsoe_daily_job = define_asset_job(
    "entsoe_daily_ingest", selection=[entsoe_generation_raw, entsoe_prices_raw]
)
entsoe_daily_schedule = ScheduleDefinition(
    job=entsoe_daily_job,
    cron_schedule="30 14 * * *",  # 14:30 UTC
    execution_timezone="Europe/Stockholm",
)

defs = Definitions(
    assets=[entsoe_generation_raw, entsoe_prices_raw], schedules=[entsoe_daily_schedule]
)
