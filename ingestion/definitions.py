from dagster import Definitions
from ingestion.assets.entsoe_asset import (
    entsoe_generation_raw,
)

defs = Definitions(assets=[entsoe_generation_raw])
