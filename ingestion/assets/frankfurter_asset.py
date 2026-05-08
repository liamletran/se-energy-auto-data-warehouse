import pandas as pd
from dagster import asset, AssetExecutionContext, Config
from dagster_duckdb import DuckDBResource
from ingestion.clients.frankfurter_client import FrankfurterClient


client = FrankfurterClient()


class FXConfig(Config):
    base_currency: str = "EUR"
    target_currency: str = "SEK"
    backfill_start_date: str = "2020-01-01"


def _ensure_fx_table(con):
    """Hàm helper đảm bảo bảng tồn tại, dùng chung cho các assets"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates_raw (
            date            DATE,
            base            VARCHAR,
            quote           VARCHAR,
            rate            DOUBLE,
            ingested_at     TIMESTAMPTZ
        )
    """)


@asset(
    group_name="forex", description="Backfill historical FX rates from Frankfurter API"
)
def fx_rates_backfill(
    context: AssetExecutionContext, config: FXConfig, duckdb: DuckDBResource
):
    """
    Asset thực hiện nạp dữ liệu lịch sử.
    Sử dụng DuckDBResource để quản lý kết nối tự động.
    """

    with duckdb.get_connection() as con:
        _ensure_fx_table(con)

        existing = con.execute(
            "SELECT COUNT(*) FROM fx_rates_raw WHERE date < '2024-01-01'"
        ).fetchone()[0]

        if existing > 0:
            context.log.info(
                f"Backfill already done — {existing} historical rows exist"
            )
            return

        context.log.info(
            f"Fetching {config.base_currency}/{config.target_currency} from {config.backfill_start_date}..."
        )

        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        df = client.fetch_timeseries(config.backfill_start_date, end_date)

        if not df.empty:
            con.execute(
                "INSERT INTO fx_rates_raw (date, base, quote, rate, ingested_at) SELECT * FROM df"
            )

            row_count = con.execute("SELECT COUNT(*) FROM fx_rates_raw").fetchone()[0]

            context.add_output_metadata(
                {
                    "total_rows": row_count,
                    "start_date": config.backfill_start_date,
                    "currency_pair": f"{config.base_currency}/{config.target_currency}",
                }
            )


@asset(group_name="forex", description="Fetch latest daily FX rates")
def fx_rates_daily(context: AssetExecutionContext, duckdb: DuckDBResource):
    """
    Asset nạp dữ liệu tỷ giá hàng ngày.
    """
    yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    with duckdb.get_connection() as con:
        _ensure_fx_table(con)

        con.execute("DELETE FROM fx_rates_raw WHERE date = ?", [yesterday])

        df = client.fetch_latest()

        if df.empty or df["date"].iloc[0] != yesterday:
            context.log.warning(f"No ECB data for {yesterday} (weekend or holiday)")
            return

        con.execute(
            "INSERT INTO fx_rates_raw (date, base, quote, rate, ingested_at) SELECT * FROM df"
        )

        row_count = con.execute(
            "SELECT COUNT(*) FROM fx_rates_raw WHERE date = ?", [yesterday]
        ).fetchone()[0]

        context.log.info(f"Successfully appended {row_count} row for {yesterday}")

        context.add_output_metadata({"ingested_date": yesterday, "status": "success"})
