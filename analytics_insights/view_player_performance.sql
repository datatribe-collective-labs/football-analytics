-- View: db_ws.gold_analytics.player_season_performance
-- Table: db_ws.gold.dim_football_clubs
-- Table: db_ws.gold_analytics.fact_top_scorers
-- Purpose: Player level KPI for season analysis
-- Grain: One row per player per club

CREATE VIEW IF NOT EXISTS db_ws.gold_analytics.view_player_performance AS
WITH player_metrics AS (
  SELECT
    player_sk,
    player_id,
    goals,
    assists,
    total_matches_played,
    ROW_NUMBER() over(
      PARTITION BY
        player_sk
      ORDER BY
        updated_at
    ) AS rn
  FROM
    db_ws.gold.fact_top_scorers
  WHERE
    total_matches_played > 0
),
derived_metrics AS (
  SELECT
    player_sk,
    player_id,
    goals AS total_goals,
    assists AS total_assists,
    CASE
      WHEN goals > 0 THEN ROUND((goals / total_matches_played),2)
      ELSE 0
    END AS goals_per_match,
    CASE
      WHEN assists > 0 THEN ROUND((assists / total_matches_played),2)
      ELSE 0
    END AS assists_per_match
  FROM
    player_metrics
  WHERE rn = 1
)
SELECT
  d.player_id,
  p.player_name,
  c.football_club,
  d.total_goals,
  d.total_assists,
  d.goals_per_match,
  d.assists_per_match
FROM
  derived_metrics d
JOIN db_ws.gold.dim_players p
  ON
    d.player_sk = p.player_sk
  AND
    p.is_current = true
JOIN db_ws.gold.dim_football_clubs c
  ON
    p.club_id = c.club_id
ORDER BY
  d.goals_per_match DESC
