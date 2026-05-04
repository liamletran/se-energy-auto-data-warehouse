from dotenv import load_dotenv
import os
import requests
from utils.fetch_api_retry import retry
import pandas as pd

load_dotenv()

# live?access_key=YOUR_KEY&symbols=USD,EUR
# historical?access_key=YOUR_KEY&date=2022-01-01


class ExchangeRateClient:
    BASE_URL = "https://api.exchangerate.host/"

    def __init__(self):
        self.api_key = os.getenv("EXCHANGERATE_KEY")
        if not self.api_key:
            raise ValueError("EXCHANGERATE_KEY not found in environment")
        self.headers = {"apikey": self.api_key}

    def _get(self, endpoint: str, params: dict):
        url = f"{self.BASE_URL}{endpoint}"
        response = retry(
            lambda: requests.get(url, headers=self.headers, params=params, timeout=30)
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("success", True):
            raise RuntimeError(f"API error: {data}")
        return data

    def fetch_latest(self, base="EUR", symbols="SEK"):
        params = {"base": base, "symnbols": symbols}
        data = self._get("latest", params)
        df = pd.DataFrame(
            {
                "date": data["date"],
                "base": base,
                "target": symbols,
                "rate": data["rates"][symbols],
            }
        )
        return df

    def fetch_timeseries(
        self, start_date: str, end_date: str, base="EUR", symbols="SEK"
    ):
        return None
