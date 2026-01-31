-- Composition: month-over-month comparison
WITH current_period AS (
    {{ current_query }}
),
previous_period AS (
    {{ previous_query }}
)
SELECT
    c.period AS current_period
    , p.period AS previous_period
    , c.market
    , c.{{ value_col }} AS current_value
    , p.{{ value_col }} AS previous_value
    , (c.{{ value_col }} - p.{{ value_col }}) / NULLIF(p.{{ value_col }}, 0) AS change_rate
FROM current_period c
LEFT JOIN previous_period p ON c.market = p.market
