-- Composition: side-by-side comparison of multiple metrics
WITH {{ metric_a_alias }} AS (
    {{ metric_a_query }}
),
{{ metric_b_alias }} AS (
    {{ metric_b_query }}
)
SELECT
    COALESCE(a.period, b.period) AS period
    , COALESCE(a.market, b.market) AS market
    , a.{{ metric_a_value }} AS {{ metric_a_alias }}
    , b.{{ metric_b_value }} AS {{ metric_b_alias }}
FROM {{ metric_a_alias }} a
FULL OUTER JOIN {{ metric_b_alias }} b ON a.period = b.period AND a.market = b.market
ORDER BY 1 DESC
