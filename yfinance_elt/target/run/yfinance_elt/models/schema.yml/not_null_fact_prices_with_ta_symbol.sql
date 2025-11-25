select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from PRATHMESH_1.analytics.fact_prices_with_ta
where symbol is null



      
    ) dbt_internal_test