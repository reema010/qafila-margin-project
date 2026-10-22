with reviewed as (
    select
        sp.source_file,
        sp.supplier_id,
        sp.sku,
        sp.valid_from,
        sp.raw_cost,
        case
            when p.product_id is null then 'unknown_sku'
            when sp.unit_cost_sar is null or sp.unit_cost_sar <= 0 then 'invalid_cost'
            else null
        end as reason
    from {{ ref('stg_supplier_prices') }} sp
    left join {{ source('raw', 'products') }} p
        on sp.sku = trim(p.sku)
)

select
    source_file,
    supplier_id,
    sku,
    valid_from,
    raw_cost,
    reason
from reviewed
where reason is not null
