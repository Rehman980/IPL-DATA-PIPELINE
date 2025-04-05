from pyspark.sql import functions as F

class TeamPerformance:
    @staticmethod
    def analyze(matches_df, deliveries_df):
        # Team batting stats
        batting_stats = deliveries_df.groupBy("batting_team").agg(
            F.countDistinct("match_id").alias("matches_played"),
            F.sum("total_runs").alias("total_runs"),
            F.avg("total_runs").alias("avg_runs_per_match")
        )
        
        # Team bowling stats
        bowling_stats = deliveries_df.groupBy("bowling_team").agg(
            F.sum("total_runs").alias("runs_conceded"),
            F.avg("total_runs").alias("avg_runs_conceded"),
            F.sum(F.when(F.col("player_dismissed").isNotNull(), 1).otherwise(0)).alias("wickets_taken")
        )
        
        # Match results
        match_results = matches_df.groupBy("winner").agg(
            F.count("*").alias("matches_won")
        )
        
        # Combine all stats
        return batting_stats.join(
            bowling_stats,
            batting_stats.batting_team == bowling_stats.bowling_team
        ).join(
            match_results,
            batting_stats.batting_team == match_results.winner,
            "left"
        ).select(
            batting_stats.batting_team.alias("team"),
            "matches_played",
            "matches_won",
            "total_runs",
            "avg_runs_per_match",
            "runs_conceded",
            "avg_runs_conceded",
            "wickets_taken"
        )