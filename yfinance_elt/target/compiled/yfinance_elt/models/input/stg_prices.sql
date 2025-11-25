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