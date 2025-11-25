WITH base AS (
  SELECT * FROM {{ ref('stg_prices') }}
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
