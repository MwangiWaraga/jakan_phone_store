{% macro string_similarity(left_text, right_text) -%}
  {{ adapter.dispatch('string_similarity') (left_text, right_text) }}
{%- endmacro %}

{% macro bigquery__string_similarity(left_text, right_text) -%}
-- Token Jaccard similarity (0..1), BigQuery-safe, no outer alias refs.
(
  select
    case
      when {{ left_text }} is null or {{ right_text }} is null then null
      else safe_divide(inter_n, nullif(union_n, 0))
    end
  from (
    with
      a as (
        select array(
          select distinct tok
          from unnest(
            split(regexp_replace(lower({{ left_text }}), r'[^0-9a-z]+', ' '), ' ')
          ) tok
          where tok <> ''
        ) as tokens
      ),
      b as (
        select array(
          select distinct tok
          from unnest(
            split(regexp_replace(lower({{ right_text }}), r'[^0-9a-z]+', ' '), ' ')
          ) tok
          where tok <> ''
        ) as tokens
      ),
      inter as (
        -- |A ∩ B| = distinct tokens that appear in both arrays
        select count(*) as inter_n
        from (
          select tok
          from unnest((select tokens from a)) tok
          where tok in unnest((select tokens from b))
          group by tok
        )
      ),
      uni as (
        -- |A ∪ B|
        select count(*) as union_n
        from (
          select tok from unnest((select tokens from a)) tok
          union distinct
          select tok from unnest((select tokens from b)) tok
        )
      )
    select (select inter_n from inter) as inter_n,
           (select union_n from uni)  as union_n
  )
)
{%- endmacro %}
