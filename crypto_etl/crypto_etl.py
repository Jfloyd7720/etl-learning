import os
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ── Extract ────────────────────────────────────────────────────────────────────

def extract(vs_currency: str = "usd", top_n: int = 50) -> list[dict]:
    """Fetch top N coins by market cap from CoinGecko."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_coins = []
    per_page = 50
    pages = (top_n + per_page - 1) // per_page

    for page in range(1, pages + 1):
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": False,
            "price_change_percentage": "1h,24h,7d"
        }
        logger.info(f"Fetching page {page} of {pages}")
        response = requests.get(url, params=params, timeout=15)

        if response.status_code == 429:
            logger.warning("Rate limited — waiting 60s")
            time.sleep(60)
            response = requests.get(url, params=params, timeout=15)

        response.raise_for_status()
        all_coins.extend(response.json())

        if page < pages:
            time.sleep(1.5)

    logger.info(f"Extracted {len(all_coins)} coins")
    return all_coins


# ── Transform ──────────────────────────────────────────────────────────────────

def transform(raw: list[dict]) -> pd.DataFrame:
    """Clean, cast, and enrich raw CoinGecko data."""
    df = pd.DataFrame(raw)

    cols = {
        "id":                                       "coin_id",
        "symbol":                                   "symbol",
        "name":                                     "name",
        "current_price":                            "price_usd",
        "market_cap":                               "market_cap_usd",
        "market_cap_rank":                          "market_cap_rank",
        "total_volume":                             "volume_24h_usd",
        "high_24h":                                 "high_24h_usd",
        "low_24h":                                  "low_24h_usd",
        "price_change_percentage_1h_in_currency":   "pct_change_1h",
        "price_change_percentage_24h_in_currency":  "pct_change_24h",
        "price_change_percentage_7d_in_currency":   "pct_change_7d",
        "circulating_supply":                       "circulating_supply",
        "ath":                                      "all_time_high_usd",
        "ath_change_percentage":                    "pct_from_ath",
        "last_updated":                             "last_updated_at",
    }
    df = df[list(cols.keys())].rename(columns=cols)

    df["last_updated_at"] = pd.to_datetime(df["last_updated_at"], utc=True)
    df["symbol"] = df["symbol"].str.upper()

    before = len(df)
    df = df.dropna(subset=["price_usd", "market_cap_usd"])
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df)} rows missing price/market cap")

    # Derived columns
    df["daily_range_pct"] = (
        (df["high_24h_usd"] - df["low_24h_usd"]) / df["price_usd"] * 100
    ).round(2)

    df["volume_to_mcap_ratio"] = (
        df["volume_24h_usd"] / df["market_cap_usd"]
    ).round(4)

    df["volume_spike"] = df["volume_to_mcap_ratio"] > 0.3

    df["ath_distance_bucket"] = df["pct_from_ath"].apply(ath_bucket)

    df["ingested_at"] = datetime.now(timezone.utc)

    logger.info(
        f"Transformed {len(df)} rows | "
        f"{df['volume_spike'].sum()} volume spikes | "
        f"{df['ath_distance_bucket'].value_counts().to_dict()}"
    )
    return df


def ath_bucket(pct: float) -> str:
    if pd.isna(pct):
        return "unknown"
    elif pct >= -10:
        return "near_ath"
    elif pct >= -50:
        return "mid_range"
    else:
        return "deep_discount"


# ── Load ───────────────────────────────────────────────────────────────────────

def get_engine():
    return create_engine(os.getenv("DB_URL"))


def create_table_if_not_exists(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS raw_coin_prices (
        id                   BIGSERIAL PRIMARY KEY,
        coin_id              TEXT NOT NULL,
        symbol               TEXT NOT NULL,
        name                 TEXT NOT NULL,
        price_usd            NUMERIC,
        market_cap_usd       NUMERIC,
        market_cap_rank      INTEGER,
        volume_24h_usd       NUMERIC,
        high_24h_usd         NUMERIC,
        low_24h_usd          NUMERIC,
        pct_change_1h        NUMERIC,
        pct_change_24h       NUMERIC,
        pct_change_7d        NUMERIC,
        circulating_supply   NUMERIC,
        all_time_high_usd    NUMERIC,
        pct_from_ath         NUMERIC,
        last_updated_at      TIMESTAMPTZ,
        daily_range_pct      NUMERIC,
        volume_to_mcap_ratio NUMERIC,
        volume_spike         BOOLEAN,
        ath_distance_bucket  TEXT,
        ingested_at          TIMESTAMPTZ NOT NULL
    );
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    logger.info("Table raw_coin_prices ready")


def load(df: pd.DataFrame, engine) -> None:
    df.to_sql(
        name="raw_coin_prices",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    logger.info(f"Loaded {len(df)} rows into raw_coin_prices")


# ── Pipeline ───────────────────────────────────────────────────────────────────

def run() -> None:
    logger.info("=== Crypto ETL starting ===")
    try:
        engine = get_engine()
        create_table_if_not_exists(engine)
        raw = extract(top_n=50)
        df = transform(raw)
        load(df, engine)
        logger.info("=== Pipeline complete ===")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run()