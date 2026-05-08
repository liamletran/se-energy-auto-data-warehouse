import os
import requests
import pandas as pd
from utils import retry
from typing import Optional, List, Dict


class SCBClient:
    BASE_URL = os.getenv("SCB_API")
    IPI_ENDPOINT = os.getenv("IPI_ENDPOINT")
    # Transport equipment (broad) + Motor vehicles (specific)
    AUTOMOTIVE_NACE = [
        "28",
        "29",
        "29-30",
    ]
    CONTENTS_CODES_MAPPING = {
        "NV0402AJ": "calendar_adj",  # Calendar adjusted
        "NV0402AK": "not_adj",  # Not calendar adjusted
        "NV0402AL": "season_adj",  # Calendar adjusted and seasonally adjusted
        "NV0402AM": "trend",  # Trend
        "NV0402AY": "mom_pct",  # Monthly development (%)
        "NV0402AZ": "yoy_pct",  # Annual development (%)
    }

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.full_url = f"{self.base_url}/{self.IPI_ENDPOINT}"

    def _get_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json"}

    def get_metadata(self) -> Dict:
        return retry(
            lambda: requests.get(self.full_url, timeout=30).json(),
            # provider_name="SCB_Metadata",
        )

    def fetch_ipi_automotive_raw(self, nace_codes=None, top_n_months=36):
        payload = {
            "query": [
                {
                    "code": "SNI2007",
                    "selection": {
                        "filter": "item",
                        "values": nace_codes or self.AUTOMOTIVE_NACE,
                    },
                },
                {
                    "code": "ContentsCode",
                    "selection": {
                        "filter": "item",
                        "values": list(self.CONTENTS_CODES_MAPPING.keys()),
                    },
                },
                {
                    "code": "Tid",
                    "selection": {
                        "filter": "top",
                        "values": [str(top_n_months)],
                    },
                },
            ],
            "response": {"format": "json"},
        }

        response = retry(lambda: requests.post(self.full_url, json=payload, timeout=30))

        response.raise_for_status()
        return response.json()

    def get_ipi_df(
        self, nace_codes: List[str] = None, top_n_months: int = 36
    ) -> pd.DataFrame:
        raw_data = self.fetch_ipi_automotive_raw(nace_codes, top_n_months)
        data_list = []
        for item in raw_data.get("data", []):
            nace = item["key"][0]
            period = item["key"][1]
            values = item["values"]

            row = {"nace_code": nace, "period": period}
            for i, col_name in enumerate(self.CONTENTS_CODES_MAPPING.values()):
                try:
                    val = float(values[i]) if values[i] != ".." else None
                    row[col_name] = val
                except (IndexError, ValueError):
                    row[col_name] = None

            row["ingested_at"] = pd.Timestamp.now(tz="UTC")
            data_list.append(row)
        return pd.DataFrame(data_list)

    # For SCB sensor
    def get_latest_period(self) -> str:
        metadata = self.get_metadata()
        # Default value is None
        found_periods = None
        for var in metadata.get("variables", []):
            if var.get("code") == "Tid":
                found_periods = var.get("values", [])
                break
        if found_periods:
            return max(found_periods)

        print(
            f"DEBUG: Metadata keys found: {[v.get('code') for v in metadata.get('variables', [])]}"
        )
        raise ValueError("The time variable 'Tid' was not found in the SCB data.")
