DECLARE split_date DATE;
SET split_date = (
  SELECT DATE(MAX(week))
  FROM (
    SELECT week,
           ROW_NUMBER() OVER(ORDER BY week) AS rownum,
           COUNT(*) OVER() AS total_rows
    FROM `astral-casing-407105.BQML_TUTORIAL.newtable2`
  )
  WHERE rownum = CAST(0.8 * total_rows AS INT64)
);
 
-- Create training table
CREATE OR REPLACE TABLE `astral-casing-407105.BQML_TUTORIAL.train_table` AS
SELECT
  state,
  term,
  week,
  score
FROM
  `astral-casing-407105.BQML_TUTORIAL.newtable2`
WHERE
  week <= split_date;
 
-- Create testing table
CREATE OR REPLACE TABLE `astral-casing-407105.BQML_TUTORIAL.test_table` AS
SELECT
  state,
  term,
  week,
  score
FROM
  `astral-casing-407105.BQML_TUTORIAL.newtable2`
WHERE
  week > split_date;
 
 
CREATE OR REPLACE MODEL `BQML_TUTORIAL.arima_model2`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week',
  time_series_data_col='score',
  time_series_id_col=['state', 'term'],
  data_frequency='WEEKLY',
  decompose_time_series=TRUE
) AS
SELECT
  state,
  term,
  week,
  score
FROM
  `astral-casing-407105.BQML_TUTORIAL.train_table`;
 
 
CREATE OR REPLACE TABLE `astral-casing-407105.BQML_TUTORIAL.forecasted_table` AS
SELECT
  state,
  term,
  forecast_timestamp,
  GREATEST(LEAST(forecast_value, 100), 1) AS predicted_score
FROM ML.FORECAST(MODEL `astral-casing-407105.BQML_TUTORIAL.arima_model2`, STRUCT(24 AS horizon));
 
 
-- Join your forecasts with the actual test data
WITH forecasted AS (
  SELECT
    state,
    term,
    forecast_timestamp,
    GREATEST(LEAST(forecast_value, 100), 1) AS predicted_score
  FROM ML.FORECAST(MODEL `astral-casing-407105.BQML_TUTORIAL.arima_model2`, STRUCT(24 AS horizon))
),
actual AS (
  SELECT
    state,
    term,
    week,
    score AS actual_score
  FROM `astral-casing-407105.BQML_TUTORIAL.test_table`
)
 
-- Calculate error metrics
SELECT
  AVG(ABS(f.predicted_score - a.actual_score)) AS MAE,
  SQRT(AVG(POWER(f.predicted_score - a.actual_score, 2))) AS RMSE,
FROM forecasted f
JOIN actual a ON f.state = a.state AND f.term = a.term AND DATE(f.forecast_timestamp) = a.week;


CREATE OR REPLACE TABLE `astral-casing-407105.BQML.predicted_vals` AS
WITH MonthlyTopPredictions AS (
  SELECT
    state,
    term,
    forecast_timestamp,
    predicted_score,
    ROW_NUMBER() OVER (
      PARTITION BY state, EXTRACT(YEAR FROM forecast_timestamp), EXTRACT(MONTH FROM forecast_timestamp)
      ORDER BY predicted_score DESC
    ) AS rn
  FROM `astral-casing-407105.BQML_TUTORIAL.forecasted_table`
)
 
SELECT
  state,
  term,
  forecast_timestamp,
  predicted_score
FROM MonthlyTopPredictions
WHERE rn <= 3
ORDER BY state, EXTRACT(YEAR FROM forecast_timestamp), EXTRACT(MONTH FROM forecast_timestamp), predicted_score DESC