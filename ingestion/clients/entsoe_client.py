import time
from entsoe import EntsoePandasClient
import pandas as pd
import os


def _retry(fn, max_attempts=3, wait_seconds=30):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if "503" in str(e) or "Service Unavailable" in str(e):
                if attempt < max_attempts:
                    print(
                        f"  [RETRY] 503 from ENTSO-E — "
                        f"attempt {attempt}/{max_attempts}, "
                        f"waiting {wait_seconds}s..."
                    )
                    time.sleep(wait_seconds)
                else:
                    raise RuntimeError(
                        f"ENTSO-E API unavailable after {max_attempts} attempts. "
                        f"Will retry at next scheduled run."
                    ) from e
            else:
                raise  # Non-503 errors fail immediately


class ENTSOEClient:
    def __init__(self):
        entsoe_token = os.getenv("ENTSOE_TOKEN")
        if not entsoe_token:
            raise ValueError("ENTSOE_TOKEN not found. Set ENTSOE_TOKEN in .env file.")
        self.client = EntsoePandasClient(api_key=entsoe_token)
        self.bidding_zones = {
            "SE1": "10Y1001A1001A44P",  # North Sweden
            "SE2": "10Y1001A1001A45N",  # North-central
            "SE3": "10Y1001A1001A46L",  # Central (Stockholm)
            "SE4": "10Y1001A1001A47J",  # South (Malmö)
        }

    def fetch_generation(
        self, start: pd.Timestamp, end: pd.Timestamp, zone: str
    ) -> pd.DataFrame:
        zone_id = self.bidding_zones[zone]

        # Wrap API call with retry
        df = _retry(lambda: self.client.query_generation(zone_id, start=start, end=end))

        if df is None or df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " - ".join(filter(None, [str(c) for c in col])).strip()
                for col in df.columns
            ]

        df = df.reset_index()
        print(f"  [DEBUG] {zone} columns: {df.columns.tolist()}")

        time_col = df.columns[0]
        df = df.rename(columns={time_col: "datetime"})

        df = df.melt(
            id_vars=["datetime"],
            var_name="production_type",
            value_name="actual_generation_mw",
        )
        df = df.dropna(subset=["actual_generation_mw"])
        df["bidding_zone"] = zone
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")

        return df.sort_values(["datetime", "production_type"]).reset_index(drop=True)

    def fetch_day_ahead_prices(
        self, start: pd.Timestamp, end: pd.Timestamp, zone: str
    ) -> pd.DataFrame:
        zone_id = self.bidding_zones[zone]

        series = _retry(
            lambda: self.client.query_day_ahead_prices(zone_id, start=start, end=end)
        )

        if series is None or series.empty:
            return pd.DataFrame()

        df = series.reset_index()
        df.columns = ["datetime", "price_eur_mwh"]
        df["bidding_zone"] = zone
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")

        return df.sort_values("datetime").reset_index(drop=True)
