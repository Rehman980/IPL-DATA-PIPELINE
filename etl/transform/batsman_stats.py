from pyspark.sql import functions as F

class BatsmanStats:
    @staticmethod
    def analyze(deliveries_df):
        return deliveries_df.groupBy("batsman").agg(
            F.countDistinct("match_id").alias("matches_played"),
            F.sum("batsman_runs").alias("total_runs"),
            F.count("ball").alias("balls_faced"),
            F.sum(F.when(F.col("player_dismissed") == F.col("batsman"), 1)
               .otherwise(0)).alias("times_out"),
            F.sum(F.when(F.col("batsman_runs") == 4, 1).otherwise(0)).alias("fours"),
            F.sum(F.when(F.col("batsman_runs") == 6, 1).otherwise(0)).alias("sixes")
        ).withColumn(
            "strike_rate",
            F.round(F.col("total_runs") / F.col("balls_faced") * 100, 2)
        ).withColumn(
            "average",
            F.round(F.col("total_runs") / F.when(F.col("times_out") == 0, 1)
               .otherwise(F.col("times_out")), 2)
        )