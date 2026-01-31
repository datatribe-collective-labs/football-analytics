-- CREATE SCHEMA IF NOT EXISTS db_ws.gold_analytics;

-- View: leagure_standing
-- Purpose: Official league table by club and season
-- Grain: One row per club per season

-- Table: fact_league_standing
-- Table: dim_football_clubs

-- Create a view for the league table
CREATE OR REPLACE VIEW db_ws.gold_analytics.view_league_standing AS
-- Deduplicate the fact_league_standing based on ingestion_ts
WITH dedup_fact_league_standing AS (
  SELECT
    club_id,
    season_id,
    season_year,
    total_games_played,
    won,
    draw,
    lost,
    total_goals_scored,
    total_goals_conceded,
    goal_difference,
    points,
    ROW_NUMBER() OVER(
      PARTITION BY
        club_id,season_id
      ORDER BY ingestion_ts
    ) AS rn
  FROM
    db_ws.gold.fact_league_standing
)
SELECT
  f.club_id,
  f.season_id,
  f.season_year,
  c.football_club,
  c.club_abbreviation,
  f.total_games_played,
  f.won,
  f.draw,
  f.lost,
  f.total_goals_scored,
  f.total_goals_conceded,
  f.goal_difference,
  f.points
FROM
  dedup_fact_league_standing AS f
JOIN
  db_ws.gold.dim_football_clubs AS c
ON
  f.club_id = c.club_id
WHERE
  rn = 1;
