import os
import sys
import requests
import pandas as pd


from utils.fetch_api_retry import retry

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))


class FrankfurterClient:
    BASE_URL = "https://api.frankfurter.dev/v2"

    def _get(self, endpoint: str, params: dict = {}) -> any:
        url = f"{self.BASE_URL}/{endpoint}"
        response = retry(lambda: requests.get(url, params=params, timeout=30))
        response.raise_for_status()
        return response.json()

    def fetch_latest(self, base="EUR", target="SEK") -> pd.DataFrame:
        data = self._get(f"rate/{base}/{target}")
        return pd.DataFrame(
            [
                {
                    "date": pd.to_datetime(data["date"]),
                    "base_currency": data["base"],
                    "target_currency": data["quote"],
                    "rate": data["rate"],
                    "ingested_at": pd.Timestamp.now(tz="UTC"),
                }
            ]
        )

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

        df = df.rename(columns={"base": "base_currency", "quote": "target_currency"})
        return df.sort_values("date").reset_index(drop=True)
