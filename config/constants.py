class Config:
    # GCS Configuration
    GCS_BUCKET = 'ipl-data-rehman'  # Will be overridden by .env
    RAW_MATCHES_PATH = "raw/matches/"
    RAW_DELIVERIES_PATH = "raw/deliveries/"
    RAW_PLAYERS_PATH = "raw/players/"
    PROCESSED_PATH = "processed/"
    
    # BigQuery Configuration
    BIGQUERY_DATASET = "ipl_spark"
    FACT_MATCHES_TABLE = "fact_matches"
    FACT_DELIVERIES_TABLE = "fact_deliveries"
    DIM_TEAMS_TABLE = "dim_teams"
    DIM_PLAYERS_TABLE = "dim_players"
    DIM_VENUES_TABLE = "dim_venues"
    
    # Data Quality Thresholds
    NULL_THRESHOLD = 0.05
    MATCH_ID_MIN = 1
    MATCH_ID_MAX = 9999