
    
    

with all_values as (

    select
        session_type as value_field,
        count(*) as n_records

    from `f1-analytics-491120`.`transformed`.`stg_laps`
    group by session_type

)

select *
from all_values
where value_field not in (
    'Race'
)


