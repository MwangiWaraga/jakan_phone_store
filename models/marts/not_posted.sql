with oraimo as (
  select 
    product_id,
    category,
    lower(left(product_title, 18)) as oraimo_shortned_product_title,
    product_title as oraimo_title,
    product_url as oraimo_url,
    current_price,
    in_stock
  from {{ ref('oraimo_products') }}
  where in_stock = 1
),

kilimall as (
  select
    lower(left(product_title, 18)) as kil_shortened_title,
    listing_id,
    product_title as kil_product_title,
    product_url as kil_url,
    is_active
  from {{ ref('kilimall_products') }}
  where brand = 'Oraimo'
    -- and is_active = 1
),

final as (
    select
      o.product_id,
      o.oraimo_title,
      o.category,
      o.oraimo_url,
      o.current_price,
    --   k.listing_id,
      case
        when k.listing_id is null then 'Never Posted'
        else 'Posted but Inactive'
      end as post_history
    from oraimo o
    left join kilimall k
      on o.oraimo_shortned_product_title = k.kil_shortened_title
    where 
      (k.listing_id is null
        or k.is_active = 0
    )
      and o.in_stock = 1
)

select * from final