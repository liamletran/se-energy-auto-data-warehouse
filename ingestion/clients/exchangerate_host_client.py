from dotenv import load_dotenv
import os
import requests

BASE_URL = "https://api.exchangerate.host/"

load_dotenv()

# historical?access_key=YOUR_KEY&date=2022-01-01


class ExchangeRateClient:
    def __init__(self):
        self.exchangerate_key = os.getenv("EXCHANGERATE_KEY")
        self.endpoint = {
            "live": "&symbols=",
            "historical": "&date=",
        }

    def fetch_live_exchangerate():
        return None

    def fetch_historical_exchangerate():
        return None
