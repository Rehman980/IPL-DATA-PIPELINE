CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset}}.dim_players` (
  player_id STRING NOT NULL,
  player_name STRING NOT NULL,
  batting_hand STRING,
  bowling_skill STRING,
  country STRING,
  date_of_birth DATE,
  first_match_date DATE,
  last_match_date DATE,
  total_matches INT64,
  total_runs INT64,
  total_wickets INT64,
  batting_average FLOAT64,
  bowling_average FLOAT64,
  is_active BOOLEAN,
  last_updated TIMESTAMP
)
CLUSTER BY player_name;