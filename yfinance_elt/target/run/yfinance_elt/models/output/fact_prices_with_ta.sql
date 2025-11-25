
  
    

        create or replace transient table PRATHMESH_1.analytics.fact_prices_with_ta
         as
        (WITH  __dbt__cte__stg_prices as (
SELECT
  UPPER(symbol)               AS symbol,
  CAST(date AS DATE)          AS dt,
  CAST(open  AS NUMBER(14,6)) AS open,
  CAST(high  AS NUMBER(14,6)) AS high,
  CAST(low   AS NUMBER(14,6)) AS low,
  CAST(close AS NUMBER(14,6)) AS close,
  CAST(volume AS NUMBER(20,0)) AS volume
FROM PRATHMESH_1.RAW.YFINANCE_DAILY_PRICES
WHERE symbol IS NOT NULL
  AND date  IS NOT NULL
), base AS (
  SELECT * FROM __dbt__cte__stg_prices
),
returns AS (
  SELECT
    symbol,
    dt,
    open,
    high,
    low,
    close,
    volume,
    (close / LAG(close) OVER (PARTITION BY symbol ORDER BY dt)) - 1 AS daily_return
  FROM base
),
avg_gains_losses AS (
  SELECT
    symbol,
    dt,
    open,
    high,
    low,
    close,
    volume,
    daily_return,
    CASE WHEN daily_return > 0 THEN daily_return ELSE 0 END AS gain,
    CASE WHEN daily_return < 0 THEN ABS(daily_return) ELSE 0 END AS loss,
    AVG(CASE WHEN daily_return > 0 THEN daily_return ELSE 0 END)
      OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(CASE WHEN daily_return < 0 THEN ABS(daily_return) ELSE 0 END)
      OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
  FROM returns
)
SELECT
  symbol,
  dt,
  open,
  high,
  low,
  close,
  volume,
  AVG(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
  AVG(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
  CASE
    WHEN avg_loss_14 = 0 THEN 100
    WHEN avg_gain_14 IS NULL OR avg_loss_14 IS NULL THEN NULL
    ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
  END AS rsi_14,
  daily_return
FROM avg_gains_losses
        );
      
  