from pyspark.sql import functions as F

class PhaseComparison:
    @staticmethod
    def analyze(deliveries_df):
        # Define match phases
        powerplay = deliveries_df.filter(F.col("over") <= 6)
        middle = deliveries_df.filter((F.col("over") > 6) & (F.col("over") <= 15))
        death = deliveries_df.filter(F.col("over") >= 16)
        
        # Calculate stats for each phase
        powerplay_stats = powerplay.groupBy("batting_team").agg(
            F.sum("total_runs").alias("powerplay_runs"),
            F.avg("total_runs").alias("powerplay_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
               .otherwise(0)).alias("powerplay_wickets")
        )
        
        middle_stats = middle.groupBy("batting_team").agg(
            F.sum("total_runs").alias("middle_runs"),
            F.avg("total_runs").alias("middle_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
               .otherwise(0)).alias("middle_wickets")
        )
        
        death_stats = death.groupBy("batting_team").agg(
            F.sum("total_runs").alias("death_runs"),
            F.avg("total_runs").alias("death_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
               .otherwise(0)).alias("death_wickets")
        )
        
        # Combine all phases
        return powerplay_stats.join(
            middle_stats, "batting_team"
        ).join(
            death_stats, "batting_team"
        )