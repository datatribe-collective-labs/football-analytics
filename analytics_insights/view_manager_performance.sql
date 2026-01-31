-- View: db_ws.gold_analytics.manager_performance
-- Table: db_ws.gold.dim_manager
-- Table: db_ws.gold.dim_football_clubs
-- Table: db_ws.gold_analytics.fact_league_standing
-- Purpose: Club performance under each manager
-- Grain: One row per manager per club per season


CREATE OR REPLACE VIEW db_ws.gold_analytics.view_manager_performance AS
WITH club_stats AS (
  SELECT
    club_id,
    won,
    total_games_played,
    lost,
    draw,
    ROW_NUMBER() OVER(PARTITION BY club_id ORDER BY ingestion_ts DESC) rn
  FROM
    db_ws.gold.fact_league_standing
),
manager_info AS (
  SELECT
    manager_id,
    club_id,
    manager_name,
    manager_nationality,
    contract_start_date,
    contract_end_date,
    is_current,
    ROW_NUMBER() OVER(PARTITION BY manager_id ORDER BY contract_start_date) AS rn,
    DATEDIFF(COALESCE(contract_end_date,CURRENT_DATE), contract_start_date) AS tenure_days
  FROM
    db_ws.gold.dim_managers
  WHERE is_current = true
)
SELECT
  m.manager_id,
  m.manager_name,
  f.football_club,
  c.total_games_played,
  c.won AS total_wins,
  c.lost AS total_losses,
  c.draw AS total_draws,
  ROUND((c.won * 100) / NULLIF(c.total_games_played,0),2) AS win_rate_percent
FROM
  club_stats AS c
JOIN
  db_ws.gold.dim_football_clubs AS f
ON
  c.club_id = f.club_id
JOIN
  manager_info AS m
ON
  c.club_id = m.club_id
WHERE
  c.rn = 1
AND 
  m.rn = 1
ORDER BY
  win_rate_percent DESC
