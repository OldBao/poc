-- Aggregation: monthly average rollup
-- Wraps a Layer 1 fragment that outputs (grass_region, grass_date, value_columns)
SELECT
    substr(cast(grass_date as varchar), 1, 7) AS period
    , grass_region AS market
    , avg({{ value_expr }}) AS {{ value_alias }}
FROM (
    {{ inner_query }}
) _inner
GROUP BY 1, 2
ORDER BY 1 DESC
