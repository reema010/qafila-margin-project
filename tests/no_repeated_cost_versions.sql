with ordered as (
    select
        sku,
        valid_from,
        unit_cost_sar,
        lag(unit_cost_sar) over (
            partition by sku
            order by valid_from
        ) as previous_cost
    from {{ ref('product_cost_history') }}
)

select *
from ordered
where previous_cost = unit_cost_sar
