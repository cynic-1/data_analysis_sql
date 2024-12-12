SELECT symbol, cross_time, cross_type, close
        FROM btc_ema_cross_view 
        ORDER BY cross_time desc limit 100;
        
SELECT pg_get_viewdef('btc_ema_cross_view', true);


WITH golden_crosses_suc_price as (
WITH golden_crosses AS (
    -- 从 btc_ema_cross_view 获取指定时间范围内的金叉
    SELECT 
        symbol,
        EXTRACT(EPOCH FROM TO_TIMESTAMP(cross_time, 'YYYY-MM-DD HH24:MI:SS'))::bigint as cross_timestamp,
        close as cross_price
    FROM btc_ema_cross_view
    WHERE cross_time BETWEEN '2024-11-02 08:00:00' AND '2024-11-30 08:00:00'
    AND cross_type = 'golden'
),
future_prices AS (
    -- 获取金叉后不同时间点的价格
    SELECT 
        gc.symbol,
        gc.cross_timestamp,
        gc.cross_price,
        MAX(CASE WHEN p."timestamp" = gc.cross_timestamp + 86400 THEN p.close END) as price_1d,
        MAX(CASE WHEN p."timestamp" = gc.cross_timestamp + 259200 THEN p.close END) as price_3d,
        MAX(CASE WHEN p."timestamp" = gc.cross_timestamp + 604800 THEN p.close END) as price_7d,
        MAX(CASE WHEN p."timestamp" = gc.cross_timestamp + 1209600 THEN p.close END) as price_14d,
        MAX(CASE WHEN p."timestamp" = gc.cross_timestamp + 604800+1209600 THEN p.close END) as price_21d
    FROM golden_crosses gc
    LEFT JOIN prices_1d p ON gc.symbol = p.symbol 
        AND p."timestamp" IN (
            gc.cross_timestamp + 86400,   -- 1天后
            gc.cross_timestamp + 259200,  -- 3天后
            gc.cross_timestamp + 604800,  -- 7天后
            gc.cross_timestamp + 1209600,  -- 14天后
            gc.cross_timestamp + 604800+1209600  -- 14天后
        )
    GROUP BY gc.symbol, gc.cross_timestamp, gc.cross_price
)
SELECT 
    symbol,
    to_char(to_timestamp(cross_timestamp), 'YYYY-MM-DD HH24:MI:SS') as cross_time,
    round(cross_price, 2) as cross_price,
    round(COALESCE((price_1d - cross_price) * 100.0 / cross_price, 0), 2) as change_1d_pct,
    round(COALESCE((price_3d - cross_price) * 100.0 / cross_price, 0), 2) as change_3d_pct,
    round(COALESCE((price_7d - cross_price) * 100.0 / cross_price, 0), 2) as change_7d_pct,
    round(COALESCE((price_14d - cross_price) * 100.0 / cross_price, 0), 2) as change_14d_pct,
    round(COALESCE((price_21d - cross_price) * 100.0 / cross_price, 0), 2) as change_21d_pct,
    price_1d as price_1d,
    price_3d as price_3d,
    price_7d as price_7d,
    price_14d as price_14d,
    price_21d as price_21d
FROM future_prices
ORDER BY cross_timestamp
)
select * from golden_crosses_suc_price;
SELECT 
    -- 汇总统计
    COUNT(*) as total_signals,
    round(AVG(change_1d_pct), 2) as avg_1d_change,
    round(AVG(change_3d_pct), 2) as avg_3d_change,
    round(AVG(change_7d_pct), 2) as avg_7d_change,
    round(AVG(change_14d_pct), 2) as avg_14d_change,
    round(AVG(change_21d_pct), 2) as avg_21d_change,
    -- 成功率（上涨比例）
    round(SUM(CASE WHEN change_1d_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_1d,
    round(SUM(CASE WHEN change_3d_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_3d,
    round(SUM(CASE WHEN change_7d_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_7d,
    round(SUM(CASE WHEN change_14d_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_14d,
    round(SUM(CASE WHEN change_21d_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_21d,
    -- 最大涨幅
    round(MAX(change_1d_pct), 2) as max_gain_1d,
    round(MAX(change_3d_pct), 2) as max_gain_3d,
    round(MAX(change_7d_pct), 2) as max_gain_7d,
    round(MAX(change_14d_pct), 2) as max_gain_14d,
    round(MAX(change_21d_pct), 2) as max_gain_21d,
    -- 最大跌幅
    round(MIN(change_1d_pct), 2) as max_loss_1d,
    round(MIN(change_3d_pct), 2) as max_loss_3d,
    round(MIN(change_7d_pct), 2) as max_loss_7d,
    round(MIN(change_14d_pct), 2) as max_loss_14d,
    round(MIN(change_21d_pct), 2) as max_loss_21d
FROM golden_crosses_suc_price t;
