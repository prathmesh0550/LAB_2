
      
  
    

        create or replace transient table PRATHMESH_1.SNAPSHOT.snapshot_fact_prices_with_ta
         as
        (
    

    select *,
        md5(coalesce(cast(symbol || '-' || TO_VARCHAR(dt) as varchar ), '')
         || '|' || coalesce(cast(dt as varchar ), '')
        ) as dbt_scd_id,
        dt as dbt_updated_at,
        dt as dbt_valid_from,
        
  
  coalesce(nullif(dt, dt), null)
  as dbt_valid_to

    from (
        

SELECT * FROM PRATHMESH_1.analytics.fact_prices_with_ta
    ) sbq



        );
      
  
  