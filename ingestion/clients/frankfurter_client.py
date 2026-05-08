import os
import sys
import requests
import pandas as pd
from utils import retry


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))


class FrankfurterClient:
    BASE_URL = os.getenv("FRANKFURTER_API")

    def _get(self, endpoint: str, params: dict = {}) -> any:
        url = f"{self.BASE_URL}/{endpoint}"
        response = retry(lambda: requests.get(url, params=params, timeout=30))
        response.raise_for_status()
        return response.json()

    def fetch_latest(self, base="EUR", target="SEK") -> pd.DataFrame:
        data = self._get(f"rate/{base}/{target}")
        df = pd.DataFrame([data])
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")
        return df

    def fetch_timeseries(
        self, start_date: str, end_date: str, base="EUR", target="SEK"
    ) -> pd.DataFrame:

        data = self._get(
            "rates",
            {
                "from": start_date,
                "to": end_date,
                "base": base,
                "quotes": target,
            },
        )

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")

        return df
