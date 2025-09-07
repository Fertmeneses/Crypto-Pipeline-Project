WITH coins_month AS (
	SELECT
		coin AS coin_id,
		EXTRACT(YEAR FROM date) AS year,
		EXTRACT(MONTH FROM date) AS month,
		price AS price_usd
	FROM coin_data
)
SELECT
	coin_id,
	year,
	month,
	AVG(price_usd) AS avg_price_usd
FROM coins_month
GROUP BY coin_id, year, month 
ORDER BY coin_id, year, month DESC
LIMIT 10;
