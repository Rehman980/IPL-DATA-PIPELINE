from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

class DataTransformer:
    @staticmethod
    def transform_matches(df: DataFrame) -> DataFrame:
        """Transform raw matches data"""
        return (df.withColumn("season", F.year(F.to_date("date"))))
    
    @staticmethod
    def transform_deliveries(df: DataFrame) -> DataFrame:
        """Transform ball-by-ball data"""
        return df.withColumn("is_wicket", F.when(F.col("player_dismissed").isNotNull(), 1).otherwise(0))
    
    @staticmethod
    def create_team_stats(matches_df: DataFrame) -> DataFrame:
        """Create team performance aggregates"""
        return (matches_df
                .groupBy("team1")
                .agg(F.count("match_id").alias("matches_played"),
                     F.sum(F.when(F.col("winner") == F.col("team1"), 1).otherwise(0)).alias("matches_won")))