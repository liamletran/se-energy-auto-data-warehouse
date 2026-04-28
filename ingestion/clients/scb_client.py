import requests

BASE_URL = ""


class SCBClient:
    def get_metadata(self):
        url = f"{BASE_URL}/"
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        return res.json()

    def query_ipi_automotive(self, top_n_months):
        return None
