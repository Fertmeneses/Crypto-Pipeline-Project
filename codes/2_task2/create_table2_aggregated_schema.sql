CREATE TABLE IF NOT EXISTS crypto_aggregated_info (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(64) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    max_price_usd NUMERIC,
    min_price_usd NUMERIC,
    UNIQUE (coin_id, year, month)
);

