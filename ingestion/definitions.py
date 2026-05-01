# from dagster import Definitions
# from ingestion.assets.scb_asset import scb_production_index

# defs = Definitions(assets=[scb_production_index])


from dagster import Definitions
from ingestion.assets.entsoe_assets import (
    entsoe_generation_raw,
)

defs = Definitions(assets=[entsoe_generation_raw])
