
-- metricflow_time_spine.sql
with 

hours as (
    
    --for BQ adapters use "DATE('01/01/2000','mm/dd/yyyy')"
    {{ dbt_date.get_base_dates(n_dateparts=10000*10, datepart="hour") }}

)


select * 
from hours
where date_hour > dateadd(year, -4, current_timestamp()) 
and date_hour < dateadd(day, 30, current_timestamp())