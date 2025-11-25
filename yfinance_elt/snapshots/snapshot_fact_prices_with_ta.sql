{% snapshot snapshot_fact_prices_with_ta %}
{{
  config(
    target_schema='SNAPSHOT',
    unique_key='symbol || \'-\' || TO_VARCHAR(dt)',
    strategy='timestamp',
    updated_at='dt',
    invalidate_hard_deletes=True
  )
}}
SELECT * FROM {{ ref('fact_prices_with_ta') }}
{% endsnapshot %}