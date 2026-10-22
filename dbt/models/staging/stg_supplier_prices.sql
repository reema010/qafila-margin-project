with raw_prices as (
    select
        supplier_id,
        trim(sku) as sku,
        cast(valid_from as date) as valid_from,
        parse_filename(filename) as source_file,
        coalesce(cast(cost_sar as varchar), cast(unit_cost as varchar)) as raw_cost
    from {{ source('raw', 'supplier_prices') }}
),

normalized as (
    select
        *,
        translate(
            replace(
                replace(trim(raw_cost), ',', '.'),
                '٫',
                '.'
            ),
            '٠١٢٣٤٥٦٧٨٩',
            '0123456789'
        ) as cost_text
    from raw_prices
)

select
    supplier_id,
    sku,
    valid_from,
    source_file,
    raw_cost,
    round(try_cast(cost_text as double), 2) as unit_cost_sar
from normalized
