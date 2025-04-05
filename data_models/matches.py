from pyspark.sql.types import *

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
    StructField("umpire3", StringType())
])