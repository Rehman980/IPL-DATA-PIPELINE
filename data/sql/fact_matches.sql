CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset}}.fact_matches` (
  match_id INT64,
  date DATE,
  season STRING,
  team1 STRING,
  team2 STRING,
  toss_winner STRING,
  toss_decision STRING,
  result STRING,
  winner STRING,
  venue STRING,
  umpire1 STRING,
  umpire2 STRING,
  player_of_match STRING
)
PARTITION BY DATE_TRUNC(date, YEAR);