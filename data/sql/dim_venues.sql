CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset}}.dim_venues` (
  venue_id STRING NOT NULL,
  venue_name STRING NOT NULL,
  city STRING,
  country STRING,
  capacity INT64,
  matches_hosted INT64,
  home_team STRING,
  pitch_type STRING,
  avg_first_innings_score FLOAT64,
  avg_second_innings_score FLOAT64,
  is_current BOOLEAN,
  last_updated TIMESTAMP
)
CLUSTER BY venue_name, city;