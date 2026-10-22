select
    sku,
    sum(case when is_current then 1 else 0 end) as current_versions
from {{ ref('product_cost_history') }}
group by sku
having sum(case when is_current then 1 else 0 end) <> 1
