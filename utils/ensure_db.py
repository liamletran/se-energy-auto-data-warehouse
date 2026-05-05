import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()
DB_PATH = os.getenv("DB_PATH")


def ensure_db() -> str:
    db_exists = os.path.exists(DB_PATH)

    if db_exists:
        return DB_PATH

    logger.warning(
        f"[WARN] DuckDB file not found at '{DB_PATH}'. "
        "This is expected on:"
        "_ First run or running first time on a new machine"
        "_ Creating new database - existing data (if any) has been lost"
    )
    os.makedirs("duckdb", exist_ok=True)
    return DB_PATH
