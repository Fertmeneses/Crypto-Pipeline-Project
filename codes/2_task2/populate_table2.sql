INSERT INTO crypto_aggregated_info (coin_id, year, month, max_price_usd, min_price_usd)
SELECT
    coin_id,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    MAX(price_usd) AS max_price_usd,
    MIN(price_usd) AS min_price_usd
FROM crypto_daily_data
GROUP BY coin_id, year, month
ON CONFLICT (coin_id, year, month) DO NOTHING;
