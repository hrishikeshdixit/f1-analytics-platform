
    
    

with all_values as (

    select
        session_type as value_field,
        count(*) as n_records

    from `f1-analytics-491120`.`transformed`.`stg_laps_practice`
    group by session_type

)

select *
from all_values
where value_field not in (
    'Practice 1','Practice 2','Practice 3'
)


