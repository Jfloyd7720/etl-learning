import pandas as pd
from crypto_etl import ath_bucket, transform


def test_ath_near():
    assert ath_bucket(-5) == "near_ath"

def test_ath_mid():
    assert ath_bucket(-30) == "mid_range"

def test_ath_deep():
    assert ath_bucket(-75) == "deep_discount"

def test_ath_null():
    assert ath_bucket(float("nan")) == "unknown"


def make_raw(price=50000, high=52000, low=48000,
             market_cap=1_000_000_000, volume=400_000_000,
             pct_from_ath=-15):
    return [{
        "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
        "current_price": price, "market_cap": market_cap,
        "market_cap_rank": 1, "total_volume": volume,
        "high_24h": high, "low_24h": low,
        "price_change_percentage_1h_in_currency": 0.5,
        "price_change_percentage_24h_in_currency": 2.1,
        "price_change_percentage_7d_in_currency": -3.4,
        "circulating_supply": 19_000_000,
        "ath": 69000, "ath_change_percentage": pct_from_ath,
        "last_updated": "2024-01-01T12:00:00.000Z",
    }]


def test_symbol_uppercase():
    df = transform(make_raw())
    assert df["symbol"].iloc[0] == "BTC"

def test_volume_spike_true():
    df = transform(make_raw(market_cap=1_000_000_000, volume=400_000_000))
    assert df["volume_spike"].iloc[0] == True

def test_volume_spike_false():
    df = transform(make_raw(market_cap=1_000_000_000, volume=50_000_000))
    assert df["volume_spike"].iloc[0] == False

def test_drops_null_price():
    raw = make_raw()
    raw[0]["current_price"] = None
    df = transform(raw)
    assert len(df) == 0

def test_daily_range():
    df = transform(make_raw(price=50000, high=52000, low=48000))
    assert df["daily_range_pct"].iloc[0] == 8.0