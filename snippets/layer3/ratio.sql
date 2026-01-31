-- Composition: ratio of two metrics
WITH {{ numerator_alias }} AS (
    {{ numerator_query }}
),
{{ denominator_alias }} AS (
    {{ denominator_query }}
)
SELECT
    a.period
    , a.market
    , a.{{ numerator_value }} AS {{ numerator_alias }}
    , b.{{ denominator_value }} AS {{ denominator_alias }}
    , a.{{ numerator_value }} / NULLIF(b.{{ denominator_value }}, 0) AS {{ ratio_alias }}
FROM {{ numerator_alias }} a
JOIN {{ denominator_alias }} b ON a.period = b.period AND a.market = b.market
