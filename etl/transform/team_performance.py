from pyspark.sql import functions as F

class TeamPerformance:
    @staticmethod
    def analyze(matches_df, deliveries_df):
        # Team batting stats
        batting_stats = deliveries_df.join(
                matches_df,
                deliveries_df.match_id == matches_df.id,
                "inner"
            ).withColumnRenamed("season","batting_season"
            ).groupBy("batting_season","batting_team").agg(
            F.countDistinct("match_id").alias("matches_played"),
            F.sum("total_runs").alias("total_runs"),
            F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("avg_runs_per_match")
        )

            # Team bowling stats
        bowling_stats = deliveries_df.join(
                matches_df,
                deliveries_df.match_id == matches_df.id,
                "inner"
            ).withColumnRenamed("season","bowling_season"
            ).groupBy("bowling_season","bowling_team").agg(
                F.sum("total_runs").alias("runs_conceded"),
                F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("avg_runs_conceded"),
                F.sum(F.when(F.col("player_dismissed").isNotNull(), 1).otherwise(0)).alias("wickets_taken"),
                F.round(F.sum(F.when(F.col("player_dismissed").isNotNull(), 1).otherwise(0))/F.countDistinct("match_id"), 2).alias("avg_wickets_taken_per_match")
            )

            # Match results
        match_results = matches_df.groupBy("season","winner").agg(
                F.count("*").alias("matches_won")
            )
            
            # Combine all stats
        return batting_stats.join(
                bowling_stats,
                (batting_stats.batting_team == bowling_stats.bowling_team) &
                (batting_stats.batting_season == bowling_stats.bowling_season),
                "inner"
            ).join(
                match_results,
                (batting_stats.batting_team == match_results.winner) &
                (batting_stats.batting_season == match_results.season),
                "left"
            ).select(
                batting_stats.batting_season.alias("season"),
                batting_stats.batting_team.alias("team"),
                "matches_played",
                F.coalesce("matches_won",F.lit(0)).alias("matches_won"),
                F.round(F.coalesce("matches_won",F.lit(0))/F.col("matches_played"), 2).alias("win_percentage"),
                "total_runs",
                "avg_runs_per_match",
                "runs_conceded",
                "avg_runs_conceded",
                "wickets_taken",
                "avg_wickets_taken_per_match"
            ).orderBy(
                "season",
                F.desc("win_percentage"),
                "matches_played"
            )