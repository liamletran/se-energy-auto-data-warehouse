from entsoe import EntsoePandasClient
import pandas as pd
import os


class ENTSOEClient:
    def __init__(self):

        entsoe_token = os.getenv("ENTSOE_TOKEN")
        if not entsoe_token:
            raise ValueError(
                "ENTSOE_TOKEN not found in environment variables. Please set it in the .env file."
            )

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
        df = self.client.query_generation(zone_id, start=start, end=end)
        if df is None or df.empty:
            return pd.DataFrame()

        return df
