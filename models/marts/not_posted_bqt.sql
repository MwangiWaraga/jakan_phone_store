
with latest_scrap as (
    select max(oraimo_latest_scrap_ts) as latest from {{ ref('oraimo_kilimall_bqt') }}
)

, final as (
select
        oraimo_product_title,
        oraimo_product_url,
        oraimo_mrkt_price,
        oraimo_current_price,
        oraimo_latest_scrap_ts
    from {{ ref('oraimo_kilimall_bqt') }}
    cross join latest_scrap
    where 1 = 1
        and oraimo_in_stock  = 1
        and match_status = 'no_candidate'
        and oraimo_latest_scrap_ts = latest
 qualify row_number() over(partition by oraimo_product_title order by oraimo_product_title) = 1
)

select
    *
from final