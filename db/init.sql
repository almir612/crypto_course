CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.crypto_prices(
    symbol String,
    price_usd Float64,
    timestamp DateTime,
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, timestamp);

CREATE TABLE IF NOT EXISTS crypto.daily_avg_prices(
    date Date,
    symbol String,
    avg_price Float64,
    calculated_at DateTime
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (date, symbol);