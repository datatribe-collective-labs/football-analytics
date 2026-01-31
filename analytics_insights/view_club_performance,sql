-- View: db_ws.gold_analytics.league_standing
-- Table: db_ws.gold.dim_season
-- Table: db_ws.gold.dim_football_clubs
-- Table: db_ws.gold_analytics.league_standing
-- Purpose: Club level KPI for season analysis
-- Grain: One row per club per season

CREATE VIEW IF NOT EXISTS db_ws.gold_analytics.view_club_performance AS
WITH club_metrics AS (
  SELECT
    club_id,
    season_id,
    total_games_played,
    won,
    total_goals_scored,
    total_goals_conceded,
    goal_difference,
    points
  FROM
    db_ws.gold_analytics.league_standing
),
calculated_metrics AS (
  SELECT
    club_id,
    season_id,
    ROUND(((won * 100)/ total_games_played),2) AS win_rate_percent,
    total_goals_scored,
    total_goals_conceded,
    points
  FROM
    club_metrics
)
SELECT
  c.club_id,
  c.season_id,
  s.season_name,
  f.football_club,
  c.win_rate_percent,
  c.total_goals_scored,
  c.total_goals_conceded,
  c.points
FROM
  calculated_metrics AS c
JOIN 
  db_ws.gold.dim_football_clubs f
ON 
  c.club_id = f.club_id
JOIN
  db_ws.gold.dim_season AS s
ON
  c.season_id = s.season_id;
