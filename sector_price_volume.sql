WITH results AS (
    SELECT *
    FROM crosstab(
        $$
        WITH token_metrics AS (
            SELECT 
                t.tag_name,
                p.timestamp,
                TO_TIMESTAMP(p.timestamp)::date as date,
                ((p.close - p.open) / NULLIF(p.open, 0) * 100)::numeric(10,2) as daily_return,
                p.volume as market_volume,
                COALESCE(c.market_cap, c.fully_diluted_market_cap) as market_cap,
                ROW_NUMBER() OVER (PARTITION BY p.symbol, TO_TIMESTAMP(p.timestamp)::date ORDER BY p.timestamp DESC) as rn
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            JOIN token_tags tt ON tk.token_id = tt.token_id
            JOIN tags t ON tt.tag_id = t.tag_id
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE p.timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
        ),
        sector_metrics AS (
            SELECT 
                tag_name,
                date,
                SUM(daily_return * COALESCE(market_cap, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) as weighted_return,
                SUM(market_volume) as total_volume
            FROM token_metrics
            WHERE rn = 1
            GROUP BY tag_name, date
        )
        SELECT 
            tag_name,
            date,
            CASE 
                WHEN weighted_return >= 0 THEN 
                    '+' || ROUND(weighted_return, 2)::text || '%'
                ELSE 
                    ROUND(weighted_return, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN total_volume >= 1000000000 THEN ROUND(total_volume/1000000000, 2)::text || 'B'
                WHEN total_volume >= 1000000 THEN ROUND(total_volume/1000000, 2)::text || 'M'
                ELSE ROUND(total_volume/1000, 2)::text || 'K'
            END as metrics
        FROM sector_metrics
        ORDER BY tag_name, date DESC
        $$,
        $$
        WITH dates AS (
            SELECT DISTINCT TO_TIMESTAMP(timestamp)::date as date
            FROM prices_1d
            WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
        )
        SELECT date FROM dates ORDER BY date DESC
        $$
    ) AS ct (
        "板块" text,
        "D1" text,
        "D2" text,
        "D3" text,
        "D4" text,
        "D5" text,
        "D6" text,
        "D7" text
    )
)
(
    SELECT *
    FROM results
    ORDER BY substring(split_part("D1", ' | ', 2) from '[\d.]+')::numeric * (
        CASE 
            WHEN "D1" LIKE '%B' THEN 1000000000
            WHEN "D1" LIKE '%M' THEN 1000000
            WHEN "D1" LIKE '%K' THEN 1000
            ELSE 1
        END
    ) DESC NULLS LAST
)
UNION ALL
SELECT 
    '总计',
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date)
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date)
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D1",
 (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '1 day'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '1 day'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D2",
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '2 days'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '2 days'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D3",
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '3 days'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '3 days'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D4",
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '4 days'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '4 days'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D5",
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '5 days'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '5 days'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D6",
    (
        SELECT 
            CASE 
                WHEN AVG_RETURN >= 0 THEN 
                    '+' || ROUND(AVG_RETURN, 2)::text || '%'
                ELSE 
                    ROUND(AVG_RETURN, 2)::text || '%'
            END || ' | ' || 
            CASE 
                WHEN TOTAL_VOL >= 1000000000 THEN ROUND(TOTAL_VOL/1000000000, 2)::text || 'B'
                WHEN TOTAL_VOL >= 1000000 THEN ROUND(TOTAL_VOL/1000000, 2)::text || 'M'
                ELSE ROUND(TOTAL_VOL/1000, 2)::text || 'K'
            END
        FROM (
            SELECT 
                SUM(p.volume) as TOTAL_VOL,
                SUM(((p.close - p.open) / NULLIF(p.open, 0) * 100) * COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)) 
                / NULLIF(SUM(COALESCE(c.market_cap, c.fully_diluted_market_cap, 0)), 0) as AVG_RETURN
            FROM prices_1d p
            JOIN tokens tk ON p.symbol = tk.symbol
            LEFT JOIN unique_cmc c ON p.symbol = c.symbol
            WHERE TO_TIMESTAMP(p.timestamp)::date = (
                SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '6 days'
                FROM prices_1d
                WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
            )
            AND (p.symbol, p.timestamp) IN (
                SELECT symbol, MAX(timestamp)
                FROM prices_1d
                WHERE TO_TIMESTAMP(timestamp)::date = (
                    SELECT MAX(TO_TIMESTAMP(timestamp)::date) - INTERVAL '6 days'
                    FROM prices_1d
                    WHERE timestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '7 days'))
                )
                GROUP BY symbol
            )
        ) t
    ) as "D7";
