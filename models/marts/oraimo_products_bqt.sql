WITH recent AS(
    SELECT
      MAX(web_scrap_ts) AS max_ts,
      product_model
    FROM {{ ref('stg_oraimo_products') }}
    GROUP BY product_model
)

SELECT 
    p.*,
    CASE 
      WHEN r.max_ts IS NOT NULL THEN 1
      ELSE 0
    END AS is_current,
    COALESCE(r.max_ts, '1970-01-01 00:00:00') AS latest_scrap_ts
FROM {{ref('stg_oraimo_products')}} p
LEFT JOIN recent r 
    ON p.product_model = r.product_model
    AND p. web_scrap_ts = r.max_ts
