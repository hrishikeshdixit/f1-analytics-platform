
    
    

with all_values as (

    select
        tyre_compound as value_field,
        count(*) as n_records

    from `f1-analytics-491120`.`transformed`.`fact_laps`
    group by tyre_compound

)

select *
from all_values
where value_field not in (
    'SOFT','MEDIUM','HARD','INTERMEDIATE','WET'
)


