select *
from {{ ref('product_cost_history') }}
where unit_cost_sar <= 0
   or (is_current and valid_to is not null)
   or (not is_current and valid_to is null)
   or (valid_to is not null and valid_to <= valid_from)
