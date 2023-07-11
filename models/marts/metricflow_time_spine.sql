{{
    config(
        materialized='table'
    )
}}

-- metricflow_time_spine.sql
with days as (
    --for BQ adapters use "DATE('01/01/2000','mm/dd/yyyy')"
{{ dbt_date.get_base_dates(start_date="2000-01-01", end_date="2027-12-31") }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select *
from final
