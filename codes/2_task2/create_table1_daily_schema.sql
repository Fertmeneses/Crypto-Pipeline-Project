CREATE TABLE IF NOT EXISTS crypto_daily_data (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(64) NOT NULL,
    price_usd NUMERIC,
    date DATE NOT NULL,
    response_json JSONB NOT NULL,
    UNIQUE (coin_id, date)
);
