from pyspark.sql import functions as F
from pyspark.sql.window import Window

class IPLTransformer:
    @staticmethod
    def process_matches(df):
        return df.withColumn("season", F.year(F.to_date("date")))
    
    @staticmethod
    def calculate_team_stats(df):
        return df.groupBy("team1").agg(
            F.count("match_id").alias("matches_played"),
            F.sum(F.when(F.col("winner") == F.col("team1"), 1).otherwise(0)).alias("wins")
        )