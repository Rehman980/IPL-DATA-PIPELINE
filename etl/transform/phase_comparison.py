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
            F.round(F.sum("total_runs")/F.countDistinct("over"), 2).alias("powerplay_run_rate"),
            F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("powerplay_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
                .otherwise(0)).alias("powerplay_wickets")
        )

        middle_stats = middle.groupBy("batting_team").agg(
            F.sum("total_runs").alias("middle_overs_runs"),
            F.round(F.sum("total_runs")/F.countDistinct("over"), 2).alias("middle_overs_run_rate"),
            F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("middle_overs_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
                .otherwise(0)).alias("middle_overs_wickets")
        )

        death_stats = death.groupBy("batting_team").agg(
            F.sum("total_runs").alias("death_overs_runs"),
            F.round(F.sum("total_runs")/F.countDistinct("over"), 2).alias("death_overs_run_rate"),
            F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("death_overs_avg"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1)
                .otherwise(0)).alias("death_overs_wickets")
        )

        # Combine all phases
        return powerplay_stats.join(
            middle_stats, "batting_team"
        ).join(
            death_stats, "batting_team"
        )