from pyspark.sql import functions as F

class VenueAnalysis:
    @staticmethod
    def analyze(matches_df, deliveries_df):
        # Match results by venue
        venue_results = matches_df.groupBy("venue").agg(
            F.count("*").alias("total_matches"),
            F.avg("win_by_runs").alias("avg_win_by_runs"),
            F.avg("win_by_wickets").alias("avg_win_by_wickets")
        )
        
        # Batting stats by venue
        batting_stats = deliveries_df.groupBy("venue", "batting_team").agg(
            F.avg("total_runs").alias("avg_runs_per_match"),
            F.sum("total_runs").alias("total_runs")
        )
        
        return venue_results.join(
            batting_stats, "venue"
        )