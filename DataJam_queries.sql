

--Ranking all the terms for a given state from top to bottom 
WITH WeeksPerState AS (
  SELECT
    RIGHT(`dma_name`, 2) AS `state`,
    `week`,
    DENSE_RANK() OVER(PARTITION BY RIGHT(`dma_name`, 2) ORDER BY `week` DESC) AS `week_rank`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms`
),
FilteredWeeks AS (
  SELECT
    `state`,
    `week`
  FROM
    WeeksPerState
  WHERE
    `week_rank` <= 4
),
FilteredTerms AS (
  SELECT
    RIGHT(t.`dma_name`, 2) AS `state`,
    t.`term`,
    t.`score`,
    t.`week`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms` t
  INNER JOIN
    FilteredWeeks fw ON t.`week` = fw.`week` AND RIGHT(t.`dma_name`, 2) = fw.`state`
  WHERE
    t.`score` IS NOT NULL -- Exclude records where score is null
),
AggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    SUM(`score`) AS `total_score` -- Aggregate only non-null scores
  FROM
    FilteredTerms
  GROUP BY
    `state`, `term`
),
RankedAggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    `total_score`,
    RANK() OVER(PARTITION BY `state` ORDER BY `total_score` DESC) AS `agg_rank`
  FROM
    AggregatedTerms
)
SELECT
  `state`,
  `term`,
  `total_score`,
  `agg_rank` AS `new_rank` -- Renaming agg_rank to new_rank for clarity
FROM
  RankedAggregatedTerms
-- WHERE
--   `agg_rank` <= 3 -- Select only the bottom 3 ranks
ORDER BY
  `state`, `new_rank`
  
  
  
--------------------------------------------------------------------------------------------------------------------------------
----------------------------------------Calculate bottom 3 terms per state 

WITH WeeksPerState AS (
  SELECT
    RIGHT(`dma_name`, 2) AS `state`,
    `week`,
    DENSE_RANK() OVER(PARTITION BY RIGHT(`dma_name`, 2) ORDER BY `week` DESC) AS `week_rank`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms`
),
FilteredWeeks AS (
  SELECT
    `state`,
    `week`
  FROM
    WeeksPerState
  WHERE
    `week_rank` <= 4
),
FilteredTerms AS (
  SELECT
    RIGHT(t.`dma_name`, 2) AS `state`,
    t.`term`,
    IFNULL(t.`score`, 0) AS `score`,  -- Replace null scores with 0, or use WHERE to exclude them completely
    t.`week`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms` t
  INNER JOIN
    FilteredWeeks fw ON t.`week` = fw.`week` AND RIGHT(t.`dma_name`, 2) = fw.`state`
  WHERE
    t.`score` IS NOT NULL  -- Exclude terms with null scores
),
AggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    SUM(`score`) AS `total_score`
  FROM
    FilteredTerms
  GROUP BY
    `state`, `term`
),
RankedAggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    `total_score`,
    RANK() OVER(PARTITION BY `state` ORDER BY `total_score` ASC) AS `agg_rank` -- Order by total_score ascending for bottom ranks
  FROM
    AggregatedTerms
)
SELECT
  `state`,
  `term`,
  `total_score`,
  `agg_rank` AS `new_rank` -- Renaming agg_rank to new_rank for clarity
FROM
  RankedAggregatedTerms
WHERE
  `agg_rank` <= 3
ORDER BY
  `state`, `new_rank`
  
------------------------------------------------------------------------------------------------------------------------------------------------------  
-----------------------------Top 3 terms in the US for each state
WITH WeeksPerState AS (
  SELECT
    RIGHT(`dma_name`, 2) AS `state`,
    `week`,
    DENSE_RANK() OVER(PARTITION BY RIGHT(`dma_name`, 2) ORDER BY `week` DESC) AS `week_rank`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms`
),
FilteredWeeks AS (
  SELECT
    `state`,
    `week`
  FROM
    WeeksPerState
  WHERE
    `week_rank` <= 4
),
FilteredTerms AS (
  SELECT
    RIGHT(t.`dma_name`, 2) AS `state`,
    t.`term`,
    t.`score`,
    t.`week`
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms` t
  INNER JOIN
    FilteredWeeks fw ON t.`week` = fw.`week` AND RIGHT(t.`dma_name`, 2) = fw.`state`
),
AggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    SUM(`score`) AS `total_score`
  FROM
    FilteredTerms
  GROUP BY
    `state`, `term`
),
RankedAggregatedTerms AS (
  SELECT
    `state`,
    `term`,
    `total_score`,
    RANK() OVER(PARTITION BY `state` ORDER BY `total_score` DESC) AS `agg_rank`
  FROM
    AggregatedTerms
)
SELECT
  `state`,
  `term`,
  `total_score`,
  `agg_rank` AS `new_rank` -- Renaming agg_rank to new_rank for clarity
FROM
  RankedAggregatedTerms
WHERE
  `agg_rank` <= 3
ORDER BY
  `state`, `new_rank`
  ----------------------------------------------------------------------------------------------------------------------------
  -------------------------------------------------------Percent gain top terms
 
WITH LatestWeeks AS (
  SELECT week
  FROM (
    SELECT week,
           RANK() OVER(ORDER BY week DESC) as week_rank
    FROM `astral-casing-407105.IU_Hackathon_Dataset.top_rising_terms`
    GROUP BY week
  )
  WHERE week_rank <= 4
),
StateWeekTerms AS (
  SELECT
    RIGHT(dma_name, 2) AS state,
    term,
    week,
    score,
    percent_gain
  FROM `astral-casing-407105.IU_Hackathon_Dataset.top_rising_terms`
  WHERE week IN (SELECT week FROM LatestWeeks)
),
ScoredTerms AS (
  SELECT
    state,
    term,
    SUM(score) AS total_score
  FROM StateWeekTerms
  GROUP BY state, term
),
RankedScoredTerms AS (
  SELECT state, term, total_score,
         ROW_NUMBER() OVER(PARTITION BY state ORDER BY total_score DESC) as rank
  FROM ScoredTerms
),
TopTerms AS (
  SELECT
    state,
    term,
    total_score
  FROM RankedScoredTerms
  WHERE rank <= 3
)
SELECT
  tt.state,
  tt.term,
  tt.total_score,
  ROW_NUMBER() OVER(PARTITION BY tt.state ORDER BY tt.total_score DESC) as term_rank,
  SUM(swt.percent_gain) as compound_percent_gain
FROM TopTerms tt
JOIN StateWeekTerms swt ON tt.state = swt.state AND tt.term = swt.term
GROUP BY tt.state, tt.term, tt.total_score
ORDER BY tt.state, tt.total_score DESC;
 
 
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------bottom percent gains
 
WITH LatestWeeks AS (
  SELECT week
  FROM (
    SELECT week,
           RANK() OVER(ORDER BY week DESC) as week_rank
    FROM `astral-casing-407105.IU_Hackathon_Dataset.top_rising_terms`
    GROUP BY week
  )
  WHERE week_rank <= 4
),
StateWeekTerms AS (
  SELECT
    RIGHT(dma_name, 2) AS state,
    term,
    week,
    percent_gain
  FROM `astral-casing-407105.IU_Hackathon_Dataset.top_rising_terms`
  WHERE week IN (SELECT week FROM LatestWeeks)
),
BottomTerms AS (
  SELECT
    state,
    term,
    SUM(percent_gain) as cumulative_percent_gain
  FROM StateWeekTerms
  WHERE percent_gain IS NOT NULL
  GROUP BY state, term
),
RankedBottomTerms AS (
  SELECT state, term, cumulative_percent_gain,
         ROW_NUMBER() OVER(PARTITION BY state ORDER BY cumulative_percent_gain ASC) as rank
  FROM BottomTerms
)
SELECT state, term, cumulative_percent_gain, rank
FROM RankedBottomTerms
WHERE rank <= 3
ORDER BY state, cumulative_percent_gain ASC;
 
----------------------------------------------------------------------------------------------------------------------------------
------------------------------------Top and bottom term in the US
WITH LatestWeeks AS (
  SELECT DISTINCT
    week
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms`
  ORDER BY
    week DESC
  LIMIT 4
),
AggregatedTerms AS (
  SELECT
    t.term,
    SUM(t.score) AS total_score
  FROM
    `astral-casing-407105.IU_Hackathon_Dataset.top_terms` t
  JOIN
    LatestWeeks lw ON t.week = lw.week
  WHERE
    t.score IS NOT NULL
  GROUP BY
    t.term
),
RankedAggregatedTerms AS (
  SELECT
    term,
    total_score,
    RANK() OVER(ORDER BY total_score DESC) AS new_rank,
    'Top' AS label
  FROM
    AggregatedTerms
)
SELECT
  term,
  total_score,
  4 - new_rank AS rank,
  label
FROM
  RankedAggregatedTerms
WHERE
  new_rank <= 3
ORDER BY
  rank DESC
  
  
