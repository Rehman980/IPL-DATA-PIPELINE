from pyspark.sql import functions as F
from pyspark.sql.window import Window

class VenueAnalysis:
    @staticmethod
    def analyze(matches_df, deliveries_df):
        return deliveries_df.join(
            matches_df,
            deliveries_df.match_id == matches_df.id,
            "inner"
        ).withColumn("player_runs",
            F.sum("batsman_runs").over(Window.partitionBy("venue", "batsman"))
        ).withColumn("most_runs_player",
            F.row_number().over(Window.partitionBy("venue").orderBy(F.desc("player_runs")))
        ).withColumn("player_wickets",
            F.sum(F.when((F.col("player_dismissed").isNotNull()) & (F.col("dismissal_kind") != "run out"), 1)
               .otherwise(0)).over(Window.partitionBy("venue", "bowler"))
        ).withColumn("most_wickets_player",
            F.row_number().over(Window.partitionBy("venue").orderBy(F.desc("player_wickets")))
        ).groupBy(
            "venue"
        ).agg(
            F.round(F.sum("total_runs")/F.countDistinct("match_id"), 2).alias("avg_runs_per_match"),
            F.round(F.sum(F.when(F.col("inning") ==  1, F.col("total_runs")).otherwise(0))/F.countDistinct("match_id"), 2).alias("avg_runs_first_inning"),
            F.round(F.sum(F.when(F.col("inning") ==  2, F.col("total_runs")).otherwise(0))/F.countDistinct("match_id"), 2).alias("avg_runs_second_inning"),
            F.countDistinct(F.when((F.col("inning") ==  1) & (F.col("batting_team") == F.col("winner")), F.col("match_id"))).alias("Batting_1st_wins"),
            F.countDistinct(F.when((F.col("inning") ==  2) & (F.col("batting_team") == F.col("winner")), F.col("match_id"))).alias("Batting_2nd_wins"),
            F.max(F.when(F.col("most_runs_player") == 1, F.concat(F.col("batsman"),F.lit(": "),F.col("player_runs")))).alias("Most_runs_player"),
            F.max(F.when(F.col("most_wickets_player") == 1, F.concat(F.col("bowler"),F.lit(": "),F.col("player_wickets")))).alias("Most_wickets_player")
        )