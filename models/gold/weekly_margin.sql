with eligible_lines as (
    select
        o.iso_week,
        o.order_date,
        st.city,
        p.category,
        trim(p.sku) as sku,
        oi.quantity,
        round(oi.quantity * oi.unit_price - oi.discount, 2) as line_revenue
    from {{ ref('stg_orders') }} o
    inner join {{ source('raw', 'order_items') }} oi
        on oi.order_id = o.order_id
    inner join {{ source('raw', 'products') }} p
        on p.product_id = oi.product_id
    inner join {{ source('raw', 'stores') }} st
        on st.store_id = o.store_id
    where oi.quantity > 0
),

priced_lines as (
    select
        l.*,
        h.unit_cost_sar,
        case
            when h.unit_cost_sar is not null then l.quantity * h.unit_cost_sar
            else null
        end as line_cost
    from eligible_lines l
    left join {{ ref('product_cost_history') }} h
        on h.sku = l.sku
       and h.valid_from <= l.order_date
       and (h.valid_to is null or l.order_date < h.valid_to)
)

select
    iso_week,
    city,
    category,
    cast(count(*) as integer) as lines,
    round(sum(line_revenue), 2) as revenue_sar,
    round(sum(case when unit_cost_sar is not null then line_revenue else 0 end), 2)
        as costed_revenue_sar,
    round(sum(coalesce(line_cost, 0)), 2) as cost_sar,
    round(
        sum(case when unit_cost_sar is not null then line_revenue else 0 end)
        - sum(coalesce(line_cost, 0)),
        2
    ) as margin_sar,
    cast(sum(case when unit_cost_sar is null then 1 else 0 end) as integer)
        as uncosted_lines
from priced_lines
group by iso_week, city, category
