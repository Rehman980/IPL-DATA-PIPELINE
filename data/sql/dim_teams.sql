CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset}}.dim_teams` (
  team_id STRING,
  team_name STRING,
  matches_played INT64,
  matches_won INT64,
  win_percentage FLOAT64,
  last_updated TIMESTAMP
);