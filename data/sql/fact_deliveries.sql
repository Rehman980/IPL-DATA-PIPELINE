CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset}}.fact_deliveries` (
  delivery_id STRING NOT NULL,
  match_id INT64 NOT NULL,
  inning INT64 NOT NULL,
  over_number INT64 NOT NULL,
  ball_number INT64 NOT NULL,
  batsman_id STRING NOT NULL,
  bowler_id STRING NOT NULL,
  non_striker_id STRING,
  batting_team STRING NOT NULL,
  bowling_team STRING NOT NULL,
  runs_batsman INT64,
  runs_extras INT64,
  runs_total INT64,
  is_wicket BOOLEAN,
  dismissal_kind STRING,
  dismissal_player STRING,
  fielder_id STRING,
  extras_type STRING,
  batting_phase STRING,
  ball_speed_kmph FLOAT64,
  pitch_zone STRING,
  shot_type STRING,
  load_timestamp TIMESTAMP
)
PARTITION BY RANGE_BUCKET(match_id, GENERATE_ARRAY(1, 10000, 100))
CLUSTER BY match_id, inning, over_number, batsman_id, bowler_id;