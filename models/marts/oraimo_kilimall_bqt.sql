{{ config(materialized='table') }}

{% set ed_threshold     = var('ed_threshold', 0.45) %}
{% set jacc_threshold   = var('jaccard_threshold', 0.30) %}
{% set min_final_score  = var('final_min_score', 0.55) %}
{% set prefix_bonus     = var('prefix_bonus', 0.05) %}

-- Upstream refs
with oraimo_src as (
  select * from {{ ref('oraimo_products_bqt') }}
),
kilimall_src as (
  select * from {{ ref('kilimall_products_bqt') }}
),

-- Normalize Oraimo
oraimo_norm as (
  select
    product_url         as oraimo_product_url,
    product_title       as oraimo_product_title,
    short_description   as oraimo_description,
    market_price        as oraimo_mrkt_price,
    current_price       as oraimo_current_price,
    product_model       as oraimo_product_model,
    latest_scrap_ts     as oraimo_latest_scrap_ts,
    in_stock            as oraimo_in_stock,
    -- normalized
    regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ') as n1_raw,
    regexp_replace(
      regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
      r'^(?:\s*oraimo\s+)', ''
    ) as n1_nb
  from oraimo_src
  where ifnull(is_current, 0) = 1
),
kilimall_norm as (
  select
    listing_id              as kilimall_listing_id,
    product_url             as kilimall_product_url,
    product_title           as kilimall_product_title,
    selling_price           as kilimall_selling_price,
    was_scraped,
    status                  as kilimall_active_status,
    shop_name,
    brand,
    regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ') as n2_raw,
    regexp_replace(
      regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
      r'^(?:\s*oraimo\s+)', ''
    ) as n2_nb
  from kilimall_src
),

-- First tokens for simple blocking
oraimo_head as (
  select
    *,
    split(n1_nb, ' ')[offset(0)]       as oraimo_t1,
    split(n1_nb, ' ')[safe_offset(1)]  as oraimo_t2
  from oraimo_norm
),
kilimall_head as (
  select
    *,
    split(n2_nb, ' ')[offset(0)]       as kilimall_t1,
    split(n2_nb, ' ')[safe_offset(1)]  as kilimall_t2
  from kilimall_norm
),

-- Candidate pairs: block by first token
cand as (
  select
    o.*,
    k.*
  from oraimo_head o
  join kilimall_head k
    on o.oraimo_t1 = k.kilimall_t1
),

-- Prefix-trim + measures
measures as (
  select
    c.*,
    substr(c.n2_nb, 1, length(c.n1_nb)) as kilimall_prefix,

    ARRAY(
      select distinct t from unnest(split(c.n1_nb, ' ')) as t
      where length(t) >= 3
        and t not in (
          'oraimo','as','picture','with','and','for','black','white','case',
          'plus','pro','max','mini','byte','lit','inch','wireless',
          'earbuds','earphones','charger','charging','power','bank','mah','w','amp'
        )
    ) as toks_oraimo,

    ARRAY(
      select distinct t from unnest(split(substr(c.n2_nb, 1, length(c.n1_nb)), ' ')) as t
      where length(t) >= 3
        and t not in (
          'oraimo','as','picture','with','and','for','black','white','case',
          'plus','pro','max','mini','byte','lit','inch','wireless',
          'earbuds','earphones','charger','charging','power','bank','mah','w','amp'
        )
    ) as toks_kilimall_prefix
  from cand c
),

scored as (
  select
    m.*,

    (
      select count(*) from (
        select x from unnest(m.toks_oraimo) as x
        intersect distinct
        select y from unnest(m.toks_kilimall_prefix) as y
      )
    ) as intersect_size,

    (
      select count(*) from (
        select x from unnest(m.toks_oraimo) as x
        union distinct
        select y from unnest(m.toks_kilimall_prefix) as y
      )
    ) as union_size,

    safe_divide(
      (
        select count(*) from (
          select x from unnest(m.toks_oraimo) as x
          intersect distinct
          select y from unnest(m.toks_kilimall_prefix) as y
        )
      ),
      (
        select count(*) from (
          select x from unnest(m.toks_oraimo) as x
          union distinct
          select y from unnest(m.toks_kilimall_prefix) as y
        )
      )
    ) as jaccard_prefix,

    1 - safe_divide(edit_distance(m.n1_nb, m.kilimall_prefix), greatest(length(m.n1_nb), length(m.kilimall_prefix))) as ed_sim_prefix,

    case
      when m.oraimo_t2 is null
        then starts_with(m.n2_nb, concat(m.oraimo_t1, ' ')) or m.n2_nb = m.oraimo_t1
      else starts_with(m.n2_nb, concat(m.oraimo_t1, ' ', m.oraimo_t2, ' '))
    end as startswith2
  from measures m
),

ranked as (
  select
    *,
    (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) as score,
    row_number() over (
      partition by oraimo_product_url
      order by (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) desc
    ) as rn
  from scored
  where
        ed_sim_prefix  >= {{ ed_threshold }}
     or jaccard_prefix >= {{ jacc_threshold }}
     or startswith2
)

-- Final: keep all Oraimo, attach best Kilimall (if any)
select
  o.oraimo_product_url,
  o.oraimo_product_title,
  o.oraimo_description,
  o.oraimo_mrkt_price,
  o.oraimo_current_price,
  o.oraimo_product_model,
  o.oraimo_latest_scrap_ts,
  o.oraimo_in_stock,
  r.kilimall_listing_id,
  r.kilimall_product_url,
  r.kilimall_product_title,
  r.kilimall_selling_price,
  r.was_scraped,
  r.kilimall_active_status,
  r.shop_name,
  r.brand,
  r.kilimall_prefix,
  r.intersect_size,
  r.union_size,
  r.jaccard_prefix,
  r.ed_sim_prefix,
  r.startswith2,
  r.score,
  case
    when r.kilimall_listing_id is null then 'no_candidate'
    when r.score >= {{ min_final_score }} then 'matched'
    else 'low_confidence'
  end as match_status
from oraimo_norm o
left join ranked r
  on r.oraimo_product_url = o.oraimo_product_url
 and r.rn = 1



-- {{ config(materialized='table') }}

-- {# -------- Tunables (override in dbt_project.yml) -------- #}
-- {% set ed_threshold     = var('ed_threshold', 0.45) %}
-- {% set jacc_threshold   = var('jaccard_threshold', 0.30) %}
-- {% set min_final_score  = var('final_min_score', 0.55) %}
-- {% set prefix_bonus     = var('prefix_bonus', 0.05) %}

-- {# -------- Upstream refs -------- #}
-- with o_src as (
--   select * from {{ ref('oraimo_products_bqt') }}
-- ),
-- k_src as (
--   select * from {{ ref('kilimall_products_bqt') }}
-- ),

-- {# -------- Normalize; strip leading "oraimo " only if it starts the string -------- #}
-- o as (
--   select
--     product_url   as oraimo_id,
--     product_title as oraimo_title,
--     -- normalized
--     regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ')                                  as n1_raw,
--     -- brandless (leading only)
--     regexp_replace(
--       regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
--       r'^(?:\s*oraimo\s+)', ''
--     ) as n1_nb
--   from o_src
--   where ifnull(is_current, 1) = 1
-- ),
-- k as (
--   select
--     listing_id,
--     product_url   as kilimall_url,
--     product_title as kilimall_title,
--     regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ')                                  as n2_raw,
--     regexp_replace(
--       regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
--       r'^(?:\s*oraimo\s+)', ''
--     ) as n2_nb
--   from k_src
-- ),

-- {# -------- First tokens for simple blocking -------- #}
-- o_head as (
--   select
--     *,
--     split(n1_nb, ' ')[offset(0)]       as o_t1,
--     split(n1_nb, ' ')[safe_offset(1)]  as o_t2
--   from o
-- ),
-- k_head as (
--   select
--     *,
--     split(n2_nb, ' ')[offset(0)]       as k_t1,
--     split(n2_nb, ' ')[safe_offset(1)]  as k_t2
--   from k
-- ),

-- {# -------- Candidate pairs: block by first token (fast + matches your observation) -------- #}
-- cand as (
--   select
--     o.oraimo_id, o.oraimo_title, o.n1_nb,
--     o.o_t1, o.o_t2,
--     k.listing_id, k.kilimall_title, k.n2_nb
--   from o_head o
--   join k_head k
--     on o.o_t1 = k.k_t1
-- ),

-- {# -------- Prefix-trim + measures (no 'tok' aliasing anywhere) -------- #}
-- measures as (
--   select
--     c.*,
--     -- trim Kilimall to Oraimo's length (brandless)
--     substr(c.n2_nb, 1, length(c.n1_nb)) as k_prefix,

--     -- token sets (distinct) with a compact stoplist; keep 'kit'
--     ARRAY(
--       select distinct t from unnest(split(c.n1_nb, ' ')) as t
--       where length(t) >= 3
--         and t not in (
--           'oraimo','as','picture','with','and','for','black','white','case',
--           'plus','pro','max','mini','byte','lit','inch','wireless',
--           'earbuds','earphones','charger','charging','power','bank','mah','w','amp'
--         )
--     ) as toks_o,

--     ARRAY(
--       select distinct t from unnest(split(substr(c.n2_nb, 1, length(c.n1_nb)), ' ')) as t
--       where length(t) >= 3
--         and t not in (
--           'oraimo','as','picture','with','and','for','black','white','case',
--           'plus','pro','max','mini','byte','lit','inch','wireless',
--           'earbuds','earphones','charger','charging','power','bank','mah','w','amp'
--         )
--     ) as toks_kpref
--   from cand c
-- ),

-- scored as (
--   select
--     m.*,

--     -- intersection size via INTERSECT DISTINCT (no inner alias leakage)
--     (
--       select count(*) from (
--         select x from unnest(m.toks_o)       as x
--         intersect distinct
--         select y from unnest(m.toks_kpref)   as y
--       )
--     ) as intersect_size,

--     -- union size
--     (
--       select count(*) from (
--         select x from unnest(m.toks_o)       as x
--         union distinct
--         select y from unnest(m.toks_kpref)   as y
--       )
--     ) as union_size,

--     -- Jaccard on prefix tokens
--     safe_divide(
--       (
--         select count(*) from (
--           select x from unnest(m.toks_o)       as x
--           intersect distinct
--           select y from unnest(m.toks_kpref)   as y
--         )
--       ),
--       (
--         select count(*) from (
--           select x from unnest(m.toks_o)       as x
--           union distinct
--           select y from unnest(m.toks_kpref)   as y
--         )
--       )
--     ) as jaccard_prefix,

--     -- edit-distance similarity on equal-length strings (brandless)
--     1 - safe_divide(edit_distance(m.n1_nb, m.k_prefix), greatest(length(m.n1_nb), length(m.k_prefix))) as ed_sim_prefix,

--     -- small bonus if Kilimall also starts with Oraimo's first two tokens (when present)
--     case
--       when m.o_t2 is null
--         then starts_with(m.n2_nb, concat(m.o_t1, ' ')) or m.n2_nb = m.o_t1
--       else starts_with(m.n2_nb, concat(m.o_t1, ' ', m.o_t2, ' '))
--     end as startswith2
--   from measures m
-- ),

-- ranked as (
--   select
--     *,
--     (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) as score,
--     row_number() over (
--       partition by oraimo_id
--       order by (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) desc
--     ) as rn
--   from scored
--   where
--         ed_sim_prefix  >= {{ ed_threshold }}
--      or jaccard_prefix >= {{ jacc_threshold }}
--      or startswith2
-- )

-- {# -------- Final: keep all Oraimo, attach best Kilimall (if any) -------- #}
-- select
--   o.oraimo_id,
--   o.oraimo_title,
--   r.listing_id                                   as kilimall_listing_id,
--   r.kilimall_title,
--   -- diagnostics
--   substr(r.n2_nb, 1, length(r.n1_nb))            as k_prefix,
--   r.intersect_size,
--   r.union_size,
--   r.jaccard_prefix,
--   r.ed_sim_prefix,
--   r.startswith2,
--   r.score,
--   case
--     when r.listing_id is null then 'no_candidate'
--     when r.score >= {{ min_final_score }} then 'matched'
--     else 'low_confidence'
--   end as match_status
-- from o o
-- left join ranked r
--   on r.oraimo_id = o.oraimo_id
--  and r.rn = 1