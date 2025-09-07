-- Full task: identify coins with price drops for more than 3 consecutive days, and include the recovery day --

-- Optional parameters to limit the analysis to a specific date range --

-- Pass these with psql like:
-- psql -h 127.0.0.1 -U postgres -d postgres -v init_date='2025-02-01' -v final_date='2025-06-30' -f SQL_streaks_3days_drop_crypto_daily_data.sql
-- If you want to omit any of these parameters, you can choose to set them as an empty string '', for example:
-- psql -h 127.0.0.1 -U postgres -d postgres -v init_date='' -v final_date='' -f SQL_streaks_3days_drop_crypto_daily_data.sql

WITH params AS (
    SELECT
        NULLIF(:'init_date','')::date AS init_date,
        NULLIF(:'final_date','')::date AS final_date
),
filtered_data AS (
    SELECT c.*
    FROM crypto_daily_data AS c
    CROSS JOIN params AS p
    WHERE (p.init_date IS NULL OR c.date >= p.init_date)
      AND (p.final_date IS NULL OR c.date <= p.final_date)
),

-- Identify price changes and market cap information--
price_usd_changes AS (
    SELECT
    coin_id,
    date,
    price_usd,
    price_usd - LAG(price_usd) OVER (
        PARTITION BY coin_id
        ORDER BY date
    ) AS price_change_1day_usd,
    (response_json->'market_data'->'market_cap'->>'usd')::numeric AS market_cap_usd
    FROM filtered_data
),
-- Flag drop days --
drop_flags AS (
    SELECT
        *,
        CASE WHEN price_change_1day_usd < 0 THEN 1 ELSE 0 END AS is_drop
    FROM price_usd_changes
),
-- Identify streaks --
streak_groups AS (
    SELECT
        *,
        -- Every time is_drop=0 (meaning no drop that day), streak_group in incremented by 1.
        -- This allows to group streaks of consecutive drop days together.
        SUM(CASE WHEN is_drop = 0 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY coin_id ORDER BY date) AS streak_group
    FROM drop_flags
),
-- Filter streaks with more than 3 days --
streak_lengths AS (
    SELECT
        coin_id,
        streak_group,
        COUNT(*) FILTER (WHERE is_drop = 1) AS drop_days_count,
        -- The minimum date consider the first day in which the price dropped --
        MIN(date) FILTER (WHERE is_drop = 1) AS streak_start,
        -- The maximum date consider the last day in which the price dropped --
        MAX(date) FILTER (WHERE is_drop = 1) AS last_drop_date
    FROM streak_groups
    GROUP BY coin_id, streak_group
    HAVING COUNT(*) FILTER (WHERE is_drop = 1) > 3
),
-- All drop days from the qualifying streak group --
drops AS (
    SELECT sg.coin_id, sg.date, sg.price_usd, sg.price_change_1day_usd
    FROM streak_groups AS sg
    JOIN streak_lengths AS sl
        ON sg.coin_id = sl.coin_id
        AND sg.streak_group = sl.streak_group
    WHERE sg.is_drop = 1
),
-- First recovery day right after the last drop (if it exists) --
recoveries AS (
    SELECT sl.coin_id, nxt.date, nxt.price_usd, nxt.price_change_1day_usd
    FROM streak_lengths AS sl
    JOIN LATERAL (
        SELECT sg2.*
        FROM streak_groups AS sg2
        WHERE 
            sg2.coin_id = sl.coin_id
            AND sg2.date > sl.last_drop_date
        ORDER BY sg2.date
        LIMIT 1
        ) nxt ON TRUE
),
-- Information about dates for each coin
dates_info AS (
    SELECT 
        coin_id, 
        MAX(date) AS last_date_available,
        MIN(date) AS first_date_available,
        (MAX(date) - MIN(date)) AS time_span_days
    FROM filtered_data
    GROUP BY coin_id
),
-- Latest market cap in usd, per coin --
latest_caps AS (
    SELECT
        p.coin_id,
        p.market_cap_usd AS latest_market_cap_usd
    FROM price_usd_changes p
    JOIN dates_info d
        ON d.coin_id = p.coin_id
        AND d.last_date_available = p.date
)
-- Average immediate recovery jump after >3days-streaks per coin --
SELECT
    rec.coin_id,
    AVG(rec.price_change_1day_usd) AS avg_recov_usd,
    COUNT(*) AS num_streaks,
    di.last_date_available AS last_date,
    di.first_date_available AS first_date,
    di.time_span_days,
    lc.latest_market_cap_usd
FROM recoveries AS rec
JOIN dates_info AS di
    ON rec.coin_id = di.coin_id
LEFT JOIN latest_caps AS lc
    ON rec.coin_id = lc.coin_id
GROUP BY 
    rec.coin_id, 
    first_date, 
    last_date, 
    di.time_span_days, 
    lc.latest_market_cap_usd
ORDER BY rec.coin_id;
