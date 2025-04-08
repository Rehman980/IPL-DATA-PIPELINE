from pyspark.sql.types import *
from google.cloud.bigquery import SchemaField

deliveries_schema = StructType([
    StructField("match_id", IntegerType()),
    StructField("inning", IntegerType()),
    StructField("batting_team", StringType()),
    StructField("bowling_team", StringType()),
    StructField("over", IntegerType()),
    StructField("ball", IntegerType()),
    StructField("batsman", StringType()),
    StructField("non_striker", StringType()),
    StructField("bowler", StringType()),
    StructField("is_super_over", IntegerType()),
    StructField("wide_runs", IntegerType()),
    StructField("bye_runs", IntegerType()),
    StructField("legbye_runs", IntegerType()),
    StructField("noball_runs", IntegerType()),
    StructField("penalty_runs", IntegerType()),
    StructField("batsman_runs", IntegerType()),
    StructField("extra_runs", IntegerType()),
    StructField("total_runs", IntegerType()),
    StructField("player_dismissed", StringType()),
    StructField("dismissal_kind", StringType()),
    StructField("fielder", StringType()),
    StructField("inserted_timestamp", TimestampType())
])

deliveries_schema_bq = [
    SchemaField("match_id", "INTEGER"),
    SchemaField("inning", "INTEGER"),
    SchemaField("batting_team", "STRING"),
    SchemaField("bowling_team", "STRING"),
    SchemaField("over", "INTEGER"),
    SchemaField("ball", "INTEGER"),
    SchemaField("batsman", "STRING"),
    SchemaField("non_striker", "STRING"),
    SchemaField("bowler", "STRING"),
    SchemaField("is_super_over", "INTEGER"),
    SchemaField("wide_runs", "INTEGER"),
    SchemaField("bye_runs", "INTEGER"),
    SchemaField("legbye_runs", "INTEGER"),
    SchemaField("noball_runs", "INTEGER"),
    SchemaField("penalty_runs", "INTEGER"),
    SchemaField("batsman_runs", "INTEGER"),
    SchemaField("extra_runs", "INTEGER"),
    SchemaField("total_runs", "INTEGER"),
    SchemaField("player_dismissed", "STRING"),
    SchemaField("dismissal_kind", "STRING"),
    SchemaField("fielder", "STRING"),
    SchemaField("inserted_timestamp", "TIMESTAMP")
]