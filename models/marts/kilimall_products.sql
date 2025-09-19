with raw as (
    select 
      listing_id,
      sku_id,
      initcap(trim(split(title, ' ')[SAFE_OFFSET(0)])) as brand,
      initcap(trim(substr(title, length(split(title, ' ')[safe_offset(0)]) + 2))) as product_title,
      market_reference_price as market_price,
      selling_price,
      non_fbk_inventory as stock,
      case   
        when status = "ACTIVE" then 1
        else 0
      end as is_active,
      updated_at_ts
    from {{source('jakan_phone_store', 'kilimall_products_raw')}}
)

select * from raw