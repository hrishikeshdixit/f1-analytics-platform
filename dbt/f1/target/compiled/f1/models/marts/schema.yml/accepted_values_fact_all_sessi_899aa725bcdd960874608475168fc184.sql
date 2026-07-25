
    
    

with all_values as (

    select
        session_category as value_field,
        count(*) as n_records

    from `f1-analytics-491120`.`transformed`.`fact_all_sessions`
    group by session_category

)

select *
from all_values
where value_field not in (
    'Race','Qualifying','Practice','Sprint'
)


