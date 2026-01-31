-- View: db_ws.gold_analytics.manager_club_tenure
-- Table: db_ws.gold.dim_managers
-- Table: db_ws.gold.dim_football_clubs
-- Purpose: Manager tenure per club
-- Grain: One row per manager per club

CREATE OR REPLACE VIEW db_ws.gold_analytics.view_manager_tenure AS
WITH manager_info AS (
  SELECT
    manager_id,
    club_id,
    manager_name,
    manager_nationality,
    contract_start_date,
    contract_end_date,
    is_current
  FROM
    db_ws.gold.dim_managers
),
derived_manager_info AS (
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
    manager_info
  WHERE is_current = true
),
retrieved_manager_info AS (
  SELECT
    manager_id,
    club_id,
    manager_name,
    manager_nationality,
    contract_start_date,
    contract_end_date,
    tenure_days,
    is_current
  FROM
    derived_manager_info
  WHERE
    rn = 1
)
SELECT
  m.manager_id,
  m.manager_name,
  fc.football_club,
  m.manager_nationality,
  m.contract_start_date,
  m.contract_end_date,
  m.is_current,
  m.tenure_days
FROM
  retrieved_manager_info AS m
JOIN
  db_ws.gold.dim_football_clubs AS fc
ON
  m.club_id = fc.club_id
WHERE
  contract_start_date IS NOT NULL
ORDER BY
  tenure_days DESC
