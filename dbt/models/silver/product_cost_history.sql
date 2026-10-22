with accepted_quotes as (
    select
        sp.source_file,
        sp.sku,
        sp.valid_from,
        sp.unit_cost_sar
    from {{ ref('stg_supplier_prices') }} sp
    inner join {{ source('raw', 'products') }} p
        on sp.sku = trim(p.sku)
    where sp.unit_cost_sar > 0
),

winner_per_date as (
    select
        source_file,
        sku,
        valid_from,
        unit_cost_sar
    from accepted_quotes
    qualify row_number() over (
        partition by sku, valid_from
        order by source_file desc
    ) = 1
),

with_previous as (
    select
        sku,
        valid_from,
        unit_cost_sar,
        lag(unit_cost_sar) over (
            partition by sku
            order by valid_from
        ) as previous_cost
    from winner_per_date
),

changed_quotes as (
    select
        sku,
        valid_from,
        unit_cost_sar
    from with_previous
    where previous_cost is null
       or unit_cost_sar <> previous_cost
)

select
    sku,
    cast(unit_cost_sar as double) as unit_cost_sar,
    valid_from,
    lead(valid_from) over (
        partition by sku
        order by valid_from
    ) as valid_to,
    lead(valid_from) over (
        partition by sku
        order by valid_from
    ) is null as is_current
from changed_quotes
