import os
import pandas as pd
from dotenv import load_dotenv
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
    EnvVar,
    run_status_sensor,
    RunStatusSensorContext,
    DagsterRunStatus,
    AssetExecutionContext,
    AssetIn,
    asset,
)
from dagster_slack import SlackResource
from dagster_duckdb import DuckDBResource
from .clients.scb_client import SCBClient
from .assets import entsoe_asset, frankfurter_asset, scb_asset
from pathlib import Path
from dagster_dbt import DbtProject, dbt_assets, DbtCliResource
from utils import ensure_db


DB_PATH = ensure_db()


# ENTSO-E daily job and schedule
entsoe_daily_job = define_asset_job(
    "entsoe_daily_ingest", selection=AssetSelection.groups("entsoe")
)
entsoe_daily_schedule = ScheduleDefinition(
    job=entsoe_daily_job,
    cron_schedule="30 14 * * *",
    execution_timezone="Europe/Stockholm",
)


# Frankfurter daily job and schedule
fx_daily_job = define_asset_job(
    "fx_daily_ingest", selection=AssetSelection.groups("forex")
)
fx_daily_schedule = ScheduleDefinition(
    job=fx_daily_job, cron_schedule="00 18 * * *", execution_timezone="Europe/Stockholm"
)


# SCB job and sensor
scb_ingest_job = define_asset_job(
    "scb_ingest_job", selection=AssetSelection.groups("scb")
)


@sensor(
    job=scb_ingest_job, minimum_interval_seconds=14400
)  # Change later to slower changing time interval. Ideally 4 days= 14400s
def scb_new_data_sensor(context: SensorEvaluationContext):
    client = SCBClient()
    try:
        latest_period = client.get_latest_period()
    except Exception as e:
        context.log.error(f"Cannot check latest date updated from SCB API: {e}")
        return
    last_processed_period = context.cursor

    if latest_period != last_processed_period:
        context.log.info(f"Detected new data for {latest_period}.")
        context.update_cursor(latest_period)
        yield RunRequest(run_key=latest_period, run_config={})
    else:
        context.log.info(
            f"There is no new data. Currently updated month is still {latest_period}."
        )


# FAILURE ALERT VIA SLACK MESSAGES

load_dotenv()
SLACK_TOKEN = os.getenv("SLACK_TOKEN")


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def slack_on_failure_sensor(context: RunStatusSensorContext, slack: SlackResource):
    message = (
        f"🔴 *Run failed!* \n"
        f"Job: {context.dagster_run.job_name}\n"
        f"Run ID: `{context.dagster_run.run_id}`\n"
        f"Failed run alert! Check it out here: "
        f"http://localhost:3000/runs/{context.dagster_run.run_id}"
    )
    slack.get_client().chat_postMessage(channel="#dev-alerts", text=message)


# dbt assets
DBT_PROJECT_PATH = Path(__file__).joinpath("..", "..", "transform").resolve()
dbt_project = DbtProject(project_dir=DBT_PROJECT_PATH)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(ins={"fx_raw": AssetIn(key_prefix=["main"])})
def fx_report(fx_raw: pd.DataFrame):
    print(f"{len(fx_raw)}")


# Automatically group all @asset in those files together
python_assets = load_assets_from_modules([entsoe_asset, frankfurter_asset, scb_asset])
all_assets = [*python_assets, dbt_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[entsoe_daily_job, fx_daily_job, scb_ingest_job],
    schedules=[entsoe_daily_schedule, fx_daily_schedule],
    sensors=[scb_new_data_sensor, slack_on_failure_sensor],
    resources={
        "duckdb": DuckDBResource(database=DB_PATH),
        "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
