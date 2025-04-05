from pyspark.sql import functions as F

class DeathOvers:
    @staticmethod
    def analyze(deliveries_df):
        return deliveries_df.filter(F.col("over") >= 16).groupBy("batting_team").agg(
            F.sum("total_runs").alias("death_over_runs"),
            F.avg("total_runs").alias("avg_death_over_runs"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
               .otherwise(0)).alias("wickets_lost")
        )