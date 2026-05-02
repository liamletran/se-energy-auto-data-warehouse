from dagster import Definitions
from dotenv import load_dotenv
from ingestion.assets.entsoe_asset import entsoe_generation_raw, entsoe_prices_raw

load_dotenv()

defs = Definitions(assets=[entsoe_generation_raw, entsoe_prices_raw])
