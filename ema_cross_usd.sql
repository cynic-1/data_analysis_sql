CREATE OR REPLACE VIEW ema_cross_view AS
 WITH ema_cross AS (
         SELECT prices_1d.symbol,
            prices_1d.exchange,
            prices_1d."timestamp",
            prices_1d.open,
            prices_1d.high,
            prices_1d.low,
            prices_1d.close,
            prices_1d.vbtc,
            prices_1d.usd_ema_20,
            prices_1d.usd_ema_200,
            prices_1d.btc_ema_20,
            prices_1d.btc_ema_200,
            prices_1d.count,
            lag(prices_1d.usd_ema_20) OVER (PARTITION BY prices_1d.symbol ORDER BY prices_1d."timestamp") AS prev_ema_20,
            lag(prices_1d.usd_ema_200) OVER (PARTITION BY prices_1d.symbol ORDER BY prices_1d."timestamp") AS prev_ema_200
           FROM prices_1d
        ), all_crosses AS (
         SELECT ema_cross.symbol,
            ema_cross.exchange,
            ema_cross."timestamp",
            ema_cross.open,
            ema_cross.high,
            ema_cross.low,
            ema_cross.close,
            ema_cross.vbtc,
            ema_cross.usd_ema_20,
            ema_cross.usd_ema_200,
            ema_cross.btc_ema_20,
            ema_cross.btc_ema_200,
            ema_cross.count,
            ema_cross.prev_ema_20,
            ema_cross.prev_ema_200,
                CASE
                    WHEN ema_cross.prev_ema_20 < ema_cross.prev_ema_200 AND ema_cross.usd_ema_20 > ema_cross.usd_ema_200 THEN 'golden'::text
                    WHEN ema_cross.prev_ema_20 > ema_cross.prev_ema_200 AND ema_cross.usd_ema_20 < ema_cross.usd_ema_200 THEN 'death'::text
                    ELSE NULL::text
                END AS cross_type,
            row_number() OVER (PARTITION BY ema_cross.symbol, (
                CASE
                    WHEN ema_cross.prev_ema_20 < ema_cross.prev_ema_200 AND ema_cross.usd_ema_20 > ema_cross.usd_ema_200 THEN 'golden'::text
                    WHEN ema_cross.prev_ema_20 > ema_cross.prev_ema_200 AND ema_cross.usd_ema_20 < ema_cross.usd_ema_200 THEN 'death'::text
                    ELSE NULL::text
                END) ORDER BY ema_cross."timestamp" DESC) AS cross_num
           FROM ema_cross
          WHERE ema_cross.prev_ema_20 IS NOT NULL
        ), latest_status AS (
         SELECT p1.symbol,
                CASE
                    WHEN p1.usd_ema_20 > p1.usd_ema_200 THEN 'golden'::text
                    ELSE 'death'::text
                END AS current_status
           FROM prices_1d p1
          WHERE p1."timestamp" = (( SELECT max(p2."timestamp") AS max
                   FROM prices_1d p2
                  WHERE p1.symbol = p2.symbol))
        )
 SELECT ac.symbol,
    ac.exchange,
    to_char(to_timestamp(ac."timestamp"::double precision), 'YYYY-MM-DD HH24:MI:SS'::text) AS cross_time,
    ac.cross_type,
    ls.current_status,
    ac.open AS open,
    ac.high AS high,
    ac.low AS low,
    ac.close AS close,
    ac.vbtc AS vbtc,
    ac.usd_ema_20 AS usd_ema_20,
    ac.usd_ema_200 AS usd_ema_200,
    ac.btc_ema_20 AS btc_ema_20,
    ac.btc_ema_200 AS btc_ema_200
   FROM all_crosses ac
     JOIN latest_status ls ON ac.symbol = ls.symbol
  WHERE ac.cross_num = 1 AND ac.cross_type IS NOT NULL AND ac.cross_type = ls.current_status
  ORDER BY ac.symbol, ac."timestamp";

SELECT symbol, cross_time, cross_type, close
        FROM ema_cross_view 
        ORDER BY cross_time;
        
SELECT pg_get_viewdef('ema_cross_view', true);

select * from prices_1d where symbol = 'HOT' order by "timestamp" desc;

-- 在 prices_1d 表上创建关键列的索引
CREATE INDEX idx_prices_1d_symbol_timestamp ON prices_1d(symbol, "timestamp");
CREATE INDEX idx_prices_1d_timestamp ON prices_1d("timestamp");

WITH golden_crosses_suc_price as (
WITH golden_crosses AS (
    -- 从 ema_cross_view 获取指定时间范围内的金叉
    SELECT 
        symbol,
        EXTRACT(EPOCH FROM TO_TIMESTAMP(cross_time, 'YYYY-MM-DD HH24:MI:SS'))::bigint as cross_timestamp,
        close as cross_price
    FROM ema_cross_view
    WHERE cross_time BETWEEN '2024-11-02 08:00:00' AND '2024-11-22 08:00:00'
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
