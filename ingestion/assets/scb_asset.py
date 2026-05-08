from dagster import asset, Config, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ingestion.clients.scb_client import SCBClient

#
client = SCBClient()


class SCBConfig(Config):
    top_n_months: int = 36


def _ensure_scb_tables(con):
    """
    Hàm helper đảm bảo Schema luôn đúng.
    Tên bảng thống nhất là scb_ipi_raw.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS scb_ipi_raw (
            nace_code       VARCHAR, 
            period          VARCHAR, 
            calendar_adj    DOUBLE, 
            not_adj         DOUBLE, 
            season_adj      DOUBLE, 
            trend           DOUBLE, 
            mom_pct         DOUBLE, 
            yoy_pct         DOUBLE, 
            ingested_at     TIMESTAMPTZ
        )
    """)


@asset(
    group_name="scb",
    description="Fetch Industrial Production Index (IPI) from SCB API and store in DuckDB",
)
def scb_ipi_sensor(
    context: AssetExecutionContext, config: SCBConfig, duckdb: DuckDBResource
):

    with duckdb.get_connection() as con:
        _ensure_scb_tables(con)

        context.log.info(
            f"Fetching data for top {config.top_n_months} months from SCB API"
        )

        df = client.get_ipi_df(top_n_months=config.top_n_months)

        if df.empty:
            context.log.warning("No data returned from SCB API.")
            return

        periods = df["period"].unique().tolist()
        context.log.info(f"Processing periods: {periods}")

        for p in periods:
            con.execute("DELETE FROM scb_ipi_raw WHERE period = ?", [p])

        con.execute(
            "INSERT INTO scb_ipi_raw (nace_code, period, calendar_adj, not_adj, season_adj, trend, mom_pct, yoy_pct, ingested_at) SELECT * FROM df"
        )
