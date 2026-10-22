with converted as (
    select
        order_id,
        store_id,
        status,
        cast(created_at as timestamp) + interval '3 hours' as riyadh_created_at
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    store_id,
    cast(riyadh_created_at as date) as order_date,
    cast(strftime(riyadh_created_at, '%V') as integer) as iso_week
from converted
where status = 'delivered'
  and cast(riyadh_created_at as date) >= date '2026-08-31'
