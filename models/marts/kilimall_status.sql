with oraimo as (
  select
    product_id,
    product_title,
    product_model,
    product_url,
    current_price,
    in_stock,
    length(product_title) as o_len
  from {{ ref('oraimo_products') }}
),

len_freq as (
  select
    o_len,
    count(*) as n
  from oraimo
  group by o_len
),

mode_len_cte as (
  select o_len as mode_len
  from len_freq
  order by n desc, o_len
  limit 1
),

len_stats as (
  select avg(o_len) as mean_len
  from oraimo
),

chosen_len as (
  select
    greatest(
      20,
      least(
        120,
        coalesce(
          (select mode_len from mode_len_cte),
          cast(round((select mean_len from len_stats)) as int64)
        )
      )
    ) as trim_len
),

kilimall as (
  select
    product_url,
    listing_id,
    sku_id,
    product_title,
    selling_price
  from {{ ref('kilimall_products') }}
  where brand = 'Oraimo'
    and is_active = 1
),

oraimo_norm as (
  select
    o.*,
    lower(regexp_replace(product_title, r'[^0-9a-z]+', ' ')) as o_title_norm,
    substr(regexp_replace(lower(product_title), r'[^0-9a-z]+', ''), 1, 10) as o_block
  from oraimo o
),

kilimall_norm as (
  select
    k.*,
    lower(regexp_replace(product_title, r'[^0-9a-z]+', ' ')) as k_title_norm,
    substr(regexp_replace(lower(product_title), r'[^0-9a-z]+', ''), 1, 10) as k_block
  from kilimall k
),

candidates as (
  select
    o.product_id,
    o.product_title as oraimo_title,
    o.product_model,
    o.product_url as oraimo_url,
    o.current_price as oraimo_price,
    o.in_stock as oraimo_in_stock,
    k.listing_id,
    k.sku_id,
    k.product_title as kilimall_title,
    k.product_url as kilimall_url,
    k.selling_price as kilimall_price,
    o.o_title_norm,
    k.k_title_norm
  from oraimo_norm o
  left join kilimall_norm k
    on (
      o.o_block = k.k_block
      or substr(o.o_block, 1, 8) = substr(k.k_block, 1, 8)
    )
),

-- trim + precompute token arrays (distinct) for both sides
trimmed as (
  select
    c.*,
    (select trim_len from chosen_len) as trim_len,
    substr(c.o_title_norm, 1, (select trim_len from chosen_len)) as o_trim_norm,
    substr(c.k_title_norm, 1, (select trim_len from chosen_len)) as k_trim_norm,
    array(
      select distinct tok
      from unnest(split(regexp_replace(substr(c.o_title_norm, 1, (select trim_len from chosen_len)), r'[^0-9a-z]+', ' '), ' ')) tok
      where tok <> ''
    ) as o_tokens,
    array(
      select distinct tok
      from unnest(split(regexp_replace(substr(c.k_title_norm, 1, (select trim_len from chosen_len)), r'[^0-9a-z]+', ' '), ' ')) tok
      where tok <> ''
    ) as k_tokens
  from candidates c
),

-- jaccard similarity = |A ∩ B| / |A ∪ B| computed from arrays; no macros
scored as (
  select
    t.*,
    safe_divide(
      array_length( array(
        select tok from unnest(t.o_tokens) tok
        where tok in unnest(t.k_tokens)
        group by tok
      )),
      nullif(
        array_length( array(
          select tok from (
            select tok from unnest(t.o_tokens) tok
            union distinct
            select tok from unnest(t.k_tokens) tok
          )
        )),
        0
      )
    ) as sim_trim
  from trimmed t
),

ranked as (
  select
    s.*,
    row_number() over (partition by product_id order by sim_trim desc nulls last) as rn
  from scored s
),

params as (
  select
    cast(0.90 as float64) as strong_threshold,
    cast(0.75 as float64) as weak_threshold
),

final_match as (
  select
    r.product_id,
    r.oraimo_title,
    r.product_model,
    r.oraimo_url,
    r.oraimo_price,
    r.oraimo_in_stock,
    r.listing_id,
    r.sku_id,
    r.kilimall_title,
    r.kilimall_url,
    r.kilimall_price,
    r.trim_len,
    r.sim_trim,
    case
      when r.listing_id is null then 'no_match'
      when r.sim_trim >= (select strong_threshold from params) then 'strong_fuzzy'
      when r.sim_trim >= (select weak_threshold from params) then 'weak_fuzzy'
      else 'no_confident_match'
    end as match_quality
  from ranked r
  where r.rn = 1
)

select * from final_match
