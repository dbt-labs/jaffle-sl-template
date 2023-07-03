-- metricflow_time_spine.sql
with days as (
    --for BQ adapters use "DATE('01/01/2000','mm/dd/yyyy')"
    {{dbt_utils.date_spine('day'
    , "to_date('01/01/2000','mm/dd/yyyy')"
    , "to_date('01/01/2027','mm/dd/yyyy')"
    )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select *
from final