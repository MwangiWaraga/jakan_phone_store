{{ config(materialized='table') }}

{# ---------------- Tunables (override in dbt_project.yml) ---------------- #}
{% set ed_threshold     = var('ed_threshold', 0.45) %}
{% set jacc_threshold   = var('jaccard_threshold', 0.30) %}
{% set min_final_score  = var('final_min_score', 0.55) %}
{% set prefix_bonus     = var('prefix_bonus', 0.05) %}

{# ---------------- Upstream refs ---------------- #}
with o_src as (
  select * from {{ ref('oraimo_products_bqt') }}
),
k_src as (
  select * from {{ ref('kilimall_products_bqt') }}
),

{# ---------------- Normalize; drop leading "oraimo " only at the start ---------------- #}
o as (
  select
    product_url   as oraimo_id,
    product_title as oraimo_title,
    regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ')                                  as n1_raw,
    regexp_replace(
      regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
      r'^(?:\s*oraimo\s+)', ''
    ) as n1_nb
  from o_src
  where ifnull(is_current, 1) = 1
),
k as (
  select
    listing_id,
    product_url   as kilimall_url,
    product_title as kilimall_title,
    regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' ')                                  as n2_raw,
    regexp_replace(
      regexp_replace(regexp_replace(lower(product_title), r'[^a-z0-9 ]', ' '), r'\s+', ' '),
      r'^(?:\s*oraimo\s+)', ''
    ) as n2_nb
  from k_src
),

{# ---------------- Pull first tokens (for blocking) ---------------- #}
o_head as (
  select
    *,
    split(n1_nb, ' ')[offset(0)]         as o_t1,
    split(n1_nb, ' ')[safe_offset(1)]    as o_t2
  from o
),
k_head as (
  select
    *,
    split(n2_nb, ' ')[offset(0)]         as k_t1,
    split(n2_nb, ' ')[safe_offset(1)]    as k_t2
  from k
),

{# ---------------- Candidate pairs: block by first token match ---------------- #}
cand as (
  select
    o.oraimo_id, o.oraimo_title, o.n1_nb,
    k.listing_id, k.kilimall_title, k.n2_nb
  from o_head o
  join k_head k
    on o.o_t1 = k.k_t1
),

{# ---------------- Prefix-trim + scoring ---------------- #}
scored as (
  select
    *,
    -- trim Kilimall to Oraimo length (brandless)
    substr(n2_nb, 1, length(n1_nb)) as k_prefix,

    -- tokenization with a tight stoplist
    ARRAY(
      select distinct tok from unnest(split(n1_nb, ' ')) tok
      where length(tok) >= 3
        and tok not in unnest([
          'oraimo','as','picture','with','and','for','black','white','kit','case',
          'plus','pro','max','mini','byte','lit','inch','watch','wireless','earbuds',
          'earphones','charger','charging','power','bank','mah','w','amp'
        ])
    ) as toks_o,
    ARRAY(
      select distinct tok from unnest(split(substr(n2_nb, 1, length(n1_nb)), ' ')) tok
      where length(tok) >= 3
        and tok not in unnest([
          'oraimo','as','picture','with','and','for','black','white','kit','case',
          'plus','pro','max','mini','byte','lit','inch','watch','wireless','earbuds',
          'earphones','charger','charging','power','bank','mah','w','amp'
        ])
    ) as toks_kpref
  from cand
),

sim as (
  select
    *,
    -- intersection / union on prefix tokens
    ARRAY(
      select tok from unnest(toks_o) tok
      where tok in unnest(toks_kpref)
    ) as intersect_tokens,

    SAFE_DIVIDE(
      ARRAY_LENGTH(ARRAY(
        select distinct tok from (
          select tok from unnest(toks_o)
          union distinct
          select tok from unnest(toks_kpref)
        )
      )),
      1
    ) as union_size
  from scored
),

measures as (
  select
    *,
    SAFE_DIVIDE(ARRAY_LENGTH(intersect_tokens), union_size) as jaccard_prefix,

    -- edit distance on equal-length strings (oraimo vs kilimall prefix)
    1 - SAFE_DIVIDE(EDIT_DISTANCE(n1_nb, k_prefix), GREATEST(length(n1_nb), length(k_prefix))) as ed_sim_prefix,

    -- bonus if Kilimall starts with the same first two tokens as Oraimo (when present)
    case
      when split(n1_nb, ' ')[safe_offset(1)] is null then true
      else starts_with(n2_nb, concat(split(n1_nb, ' ')[offset(0)], ' ', split(n1_nb, ' ')[safe_offset(1)], ' '))
    end as startswith2
  from sim
),

ranked as (
  select
    *,
    (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) as score,
    row_number() over (
      partition by oraimo_id
      order by (0.60*ed_sim_prefix + 0.40*jaccard_prefix) + if(startswith2, {{ prefix_bonus }}, 0) desc
    ) as rn
  from measures
  where
      ed_sim_prefix  >= {{ ed_threshold }}
   or jaccard_prefix >= {{ jacc_threshold }}
   or startswith2
)

select
  o.oraimo_id,
  o.oraimo_title,
  r.listing_id                                  as kilimall_listing_id,
  r.kilimall_title,
  r.k_prefix,
  r.intersect_tokens,
  r.jaccard_prefix,
  r.ed_sim_prefix,
  r.startswith2,
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