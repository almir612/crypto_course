CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.crypto_prices(
    symbol String,
    price_usd Float64,
    timestamp DateTime,
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, timestamp);