from pyspark.sql.types import *
from google.cloud.bigquery import SchemaField

matches_schema = StructType([
    StructField("id", IntegerType()),
    StructField("Season", StringType()),
    StructField("city", StringType()),
    StructField("date", DateType()),
    StructField("team1", StringType()),
    StructField("team2", StringType()),
    StructField("toss_winner", StringType()),
    StructField("toss_decision", StringType()),
    StructField("result", StringType()),
    StructField("dl_applied", IntegerType()),
    StructField("winner", StringType()),
    StructField("win_by_runs", IntegerType()),
    StructField("win_by_wickets", IntegerType()),
    StructField("player_of_match", StringType()),
    StructField("venue", StringType()),
    StructField("umpire1", StringType()),
    StructField("umpire2", StringType()),
    StructField("umpire3", StringType()),
    StructField("inserted_timestamp", TimestampType())
])


matches_schema_bq = [
    SchemaField("id", "INTEGER"),
    SchemaField("Season", "STRING"),
    SchemaField("city", "STRING"),
    SchemaField("date", "DATE"),
    SchemaField("team1", "STRING"),
    SchemaField("team2", "STRING"),
    SchemaField("toss_winner", "STRING"),
    SchemaField("toss_decision", "STRING"),
    SchemaField("result", "STRING"),
    SchemaField("dl_applied", "INTEGER"),
    SchemaField("winner", "STRING"),
    SchemaField("win_by_runs", "INTEGER"),
    SchemaField("win_by_wickets", "INTEGER"),
    SchemaField("player_of_match", "STRING"),
    SchemaField("venue", "STRING"),
    SchemaField("umpire1", "STRING"),
    SchemaField("umpire2", "STRING"),
    SchemaField("umpire3", "STRING"),
    SchemaField("inserted_timestamp", "TIMESTAMP")
]

