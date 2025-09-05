SELECT
    p.updated_at,
    e.listing_id,
    -- p.product_url,
    CASE 
        WHEN p.listing_id IS NOT NULL THEN p.product_url
        ELSE CONCAT('https://www.kilimall.co.ke/listing/', e.listing_id)
    END AS product_url,
    e.product_title,
    e.market_price,
    e.selling_price,
    e.status,
    e.shop_name,
    CASE
        WHEN p.listing_id IS NULL THEN 0
        ELSE 1
    END AS was_scraped
FROM {{ref('stg_kilimall_export')}} e
LEFT JOIN {{ref('stg_kilimall_products')}} p
    ON e.listing_id = p.listing_id
    AND e.shop_name = p.store_name