-- View: db_ws.gold_analytics.season_performance
-- Table: db_ws.gold.dim_players
-- Table: db_ws.gold.dim_football_clubs
-- Table: db_ws.gold_analytics.league_standing
-- Table: db_ws.gold.fact_top_scorers
-- Purpose: Star player dependency per club
-- Grain: One row per club

CREATE OR REPLACE VIEW db_ws.gold_analytics.view_player_dependency AS
WITH player_stats AS (
  SELECT
    player_id,
    club_id,
    SUM(goals) AS star_player_total_goals
  FROM db_ws.gold.fact_top_scorers
  GROUP BY player_id,club_id
),
ranked_players AS (
  SELECT
    player_id,
    club_id,
    star_player_total_goals,
    ROW_NUMBER() OVER(
       PARTITION BY
        club_id
      ORDER BY
        star_player_total_goals DESC
    ) AS rn
  FROM
    player_stats
),
star_players AS (
  SELECT
    player_id,
    club_id,
    star_player_total_goals
  FROM
    ranked_players
  WHERE
    rn = 1
),
club_stats AS (
  SELECT
    club_id,
    total_goals_scored,
    ROW_NUMBER() OVER(
      PARTITION BY
        club_id
      ORDER BY
        updated_at
    )AS rn
  FROM
    db_ws.gold.fact_league_standing
)
SELECT
  fc.football_club,
  fc.club_abbreviation,
  dp.player_name,
  c.total_goals_scored club_total_goals,
  ROUND((sp.star_player_total_goals * 100) /NULLIF(c.total_goals_scored,0),2) AS goals_dependency_percent
FROM
  star_players sp
JOIN
  db_ws.gold.dim_players dp
ON
  sp.player_id = dp.player_id
JOIN
  club_stats c
ON
  dp.club_id = c.club_id
JOIN
  db_ws.gold.dim_football_clubs fc
ON
  dp.club_id = fc.club_id
WHERE c.rn = 1
ORDER BY goals_dependency_percent DESC;
