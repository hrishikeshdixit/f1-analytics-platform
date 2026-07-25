
    
    

with dbt_test__target as (

  select driver_code as unique_field
  from `f1-analytics-491120`.`transformed`.`dim_drivers`
  where driver_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


