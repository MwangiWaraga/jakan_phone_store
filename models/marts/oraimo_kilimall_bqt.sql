{{ config(materialized='table') }}

-- Tunables (override in dbt_project.yml)
{% set block_min_overlap = var('block_min_overlap', 1) %}
{% set ed_threshold     = var('ed_threshold', 0.45) %}
{% set jacc_threshold   = var('jaccard_threshold', 0.30) %}
{% set min_final_score  = var('final_min_score', 0.45) %}

-- Upstream refs
with o_src as (
  select * from {{ ref('oraimo_products_bqt') }}
),
k_src as (
  select * from {{ ref('kilimall_products_bqt') }}
),

-- Normalize titles (lower + strip punctuation) and pick only current Oraimo rows
o as (
  select
    product_url                                     as oraimo_id,         -- use your key
    product_title                                   as oraimo_title,
    regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' ') as n1
  from o_src
  where ifnull(is_current, 1) = 1
),
k as (
  select
    listing_id,
    product_url                                     as kilimall_url,
    product_title                                   as kilimall_title,
    regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' ') as n2
  from k_src
),

-- Tokenize (remove tiny/common words)
o_tok as (
  select
    oraimo_id, oraimo_title, n1,
    array_agg(distinct tok) as toks1
  from (
    select oraimo_id, oraimo_title, n1, tok
    from o, unnest(split(n1,' ')) as tok
    where length(tok) >= 3
      and tok not in unnest([
        'oraimo','as','picture','with','and','for','black','white','kit','case',
        'plus','pro','max','mini','byte','lit','inch','watch','wireless','earbuds',
        'earphones','charger','charging','power','bank','mah','w','amp'
      ])
  )
  group by 1,2,3
),
k_tok as (
  select
    listing_id, kilimall_url, kilimall_title, n2,
    array_agg(distinct tok) as toks2
  from (
    select listing_id, kilimall_url, kilimall_title, n2, tok
    from k, unnest(split(n2,' ')) as tok
    where length(tok) >= 3
      and tok not in unnest([
        'oraimo','as','picture','with','and','for','black','white','kit','case',
        'plus','pro','max','mini','byte','lit','inch','watch','wireless','earbuds',
        'earphones','charger','charging','power','bank','mah','w','amp'
      ])
  )
  group by 1,2,3,4
),

-- Block candidates: cross join, then filter by overlap count
cand as (
  select
    o.oraimo_id, o.oraimo_title, o.n1, o.toks1,
    k.listing_id, k.kilimall_title, k.n2, k.toks2,
    array(
      select distinct tok from unnest(o.toks1) as tok
      where tok in unnest(k.toks2)
    ) as intersect_tokens,
    array_length(
      array(
        select distinct tok from unnest(o.toks1) as tok
        where tok in unnest(k.toks2)
      )
    ) as intersect_count
  from o_tok o
  join k_tok k
    on 1=1
),

filtered_cand as (
  select *
  from cand
  where intersect_count >= {{ block_min_overlap }}
),

-- Score candidates
scored as (
  select
    *,
    -- token Jaccard
    safe_divide(
      array_length(intersect_tokens),
      array_length(array(
        select distinct tok
        from (
          select tok from unnest(toks1) as tok
          union distinct
          select tok from unnest(toks2) as tok
        )
      ))
    ) as jaccard,
    -- edit-distance similarity
    1 - safe_divide(edit_distance(n1, n2), greatest(length(n1), length(n2))) as ed_sim
  from filtered_cand
),

ranked as (
  select
    *,
    0.6*ed_sim + 0.4*jaccard as score,
    row_number() over (partition by oraimo_id order by 0.6*ed_sim + 0.4*jaccard desc) as rn
  from scored
  where ed_sim >= {{ ed_threshold }} or jaccard >= {{ jacc_threshold }}
)

-- Keep ALL Oraimo rows; attach the best Kilimall match (if any)
select
  o.oraimo_id,
  o.oraimo_title,
  r.listing_id                                  as kilimall_listing_id,
  r.kilimall_title,
  r.intersect_tokens,
  r.jaccard,
  r.ed_sim,
  r.score,
  case
    when r.listing_id is null then 'no_candidate'
    when r.score >= {{ min_final_score }} then 'matched'
    else 'low_confidence'
  end as match_status
from o o
left join ranked r
  on r.oraimo_id = o.oraimo_id
 and r.rn = 1