WITH monthly_prices AS (
    SELECT 
        symbol,
        exchange,
        DATE_TRUNC('month', TO_TIMESTAMP(timestamp)) AS month,
        FIRST_VALUE(close) OVER (
            PARTITION BY symbol, exchange, DATE_TRUNC('month', TO_TIMESTAMP(timestamp))
            ORDER BY timestamp
        ) AS month_start_price,
        LAST_VALUE(close) OVER (
            PARTITION BY symbol, exchange, DATE_TRUNC('month', TO_TIMESTAMP(timestamp))
            ORDER BY timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_end_price
    FROM prices_1d
)
SELECT DISTINCT
    month,
    symbol,
    exchange,
    month_start_price,
    month_end_price,
    ROUND(
        ((month_end_price - month_start_price) / month_start_price * 100)::numeric,
        2
    ) AS price_change_percentage
FROM monthly_prices
ORDER BY month DESC, symbol, exchange;

WITH latest_date AS (
    SELECT MAX(timestamp) as max_ts FROM prices_1d
),
thirty_days_ago AS (
    SELECT max_ts - (30 * 24 * 60 * 60) as thirty_days_ago_ts 
    FROM latest_date
),
price_changes AS (
    SELECT 
        p_current.symbol,
        p_current.exchange,
        p_current.close as current_price,
        p_old.close as price_30_days_ago,
        TO_TIMESTAMP(p_current.timestamp) as current_date,
        TO_TIMESTAMP(p_old.timestamp) as old_date
    FROM prices_1d p_current
    JOIN latest_date ld ON p_current.timestamp = ld.max_ts
    LEFT JOIN prices_1d p_old 
        ON p_current.symbol = p_old.symbol 
        AND p_current.exchange = p_old.exchange
        AND p_old.timestamp >= (SELECT thirty_days_ago_ts FROM thirty_days_ago)
        AND p_old.timestamp < (SELECT thirty_days_ago_ts FROM thirty_days_ago) + (24 * 60 * 60)
)
SELECT 
    symbol,
    exchange,
    current_date,
    old_date,
    current_price,
    price_30_days_ago,
    ROUND(
        ((current_price - price_30_days_ago) / price_30_days_ago * 100)::numeric,
        2
    ) as price_change_percentage
FROM price_changes
WHERE price_30_days_ago IS NOT NULL
ORDER BY price_change_percentage DESC;
