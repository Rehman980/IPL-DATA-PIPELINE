from pyspark.sql.types import *

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
    StructField("fielder", StringType())
])