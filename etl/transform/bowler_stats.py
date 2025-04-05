from pyspark.sql import functions as F

class BowlerStats:
    @staticmethod
    def analyze(deliveries_df):
        return deliveries_df.groupBy("bowler").agg(
            F.countDistinct("match_id").alias("matches_played"),
            F.count("ball").alias("balls_bowled"),
            F.sum("total_runs").alias("runs_conceded"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
               .otherwise(0)).alias("wickets_taken"),
            F.sum(F.when(F.col("wide_runs") > 0, 1).otherwise(0)).alias("wides"),
            F.sum(F.when(F.col("noball_runs") > 0, 1).otherwise(0)).alias("no_balls")
        ).withColumn(
            "economy",
            F.round(F.col("runs_conceded") / (F.col("balls_bowled") / 6), 2)
        ).withColumn(
            "bowling_avg",
            F.round(F.col("runs_conceded") / F.when(F.col("wickets_taken") == 0, 1)
               .otherwise(F.col("wickets_taken")), 2)
        )