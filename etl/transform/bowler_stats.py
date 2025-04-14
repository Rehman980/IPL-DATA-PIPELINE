from pyspark.sql import functions as F
from pyspark.sql.window import Window

class BowlerStats:
    @staticmethod
    def analyze(deliveries_df):
        return deliveries_df.withColumn("over_run",
                    F.sum("total_runs").over(Window.partitionBy("match_id","inning","over"))
                    ).withColumn("maiden_over", 
                        F.when(F.col("over_run") == 0,F.col("over"))   
                    ).withColumn("wicket_taken",
                        F.when((F.col("player_dismissed").isNotNull()) & (~F.col("dismissal_kind").isin("run out","retired hurt","obstructing the field")), 1)
                        .otherwise(0)
                    ).withColumn("bowler_match_total_wicket", 
                            F.sum("wicket_taken").over(Window.partitionBy("bowler","match_id")) 
                    ).withColumn("runs_conceded",
                            F.col("total_runs") - F.col("bye_runs") - F.col("legbye_runs")
                    ).withColumn("bowler_match_total_runs",
                            F.sum("runs_conceded").over(Window.partitionBy("bowler","match_id"))
                    ).withColumn("bowler_best_wicket_match",
                            F.row_number().over(Window.partitionBy("bowler").orderBy(F.desc("bowler_match_total_wicket"),F.asc("bowler_match_total_runs")))
                    ).groupBy("bowler").agg(
                        F.countDistinct("match_id").alias("matches_played"),
                        F.count("ball").alias("balls_bowled"),
                        F.count(F.when((F.col("noball_runs") == 0) & (F.col("wide_runs") == 0), F.col("ball"))).alias("legal_balls"),
                        F.sum("runs_conceded").alias("runs_conceded"),
                        F.sum("wicket_taken").alias("wickets_taken"),
                        F.sum(F.when(F.col("wide_runs") > 0, 1).otherwise(0)).alias("wides"),
                        F.sum(F.when(F.col("noball_runs") > 0, 1).otherwise(0)).alias("no_balls"),
                        F.countDistinct("maiden_over").alias("maiden_overs"),
                        F.max(F.when(F.col("bowler_best_wicket_match") == 1, F.concat(F.col("bowler_match_total_wicket"),F.lit("/"),F.col("bowler_match_total_runs")))
                        .otherwise(0)).alias("best_bowling_figure"),
                        F.countDistinct(F.when(F.col("bowler_match_total_wicket") >= 3, F.col("match_id"))).alias("three_wickets_haul"),
                        F.countDistinct(F.when(F.col("bowler_match_total_wicket") >= 5, F.col("match_id"))).alias("five_wickets_haul"),
                    ).withColumn("overs_bowled",
                        F.concat(
                        F.floor(F.col("legal_balls")/6),
                        F.lit("."),
                        F.floor(F.col("legal_balls")%6))
                    ).withColumn(
                        "economy",
                        F.round(F.col("runs_conceded") / (F.col("legal_balls") / 6), 2)
                    ).withColumn(
                        "bowling_avg",
                        F.round(F.col("runs_conceded") / F.when(F.col("wickets_taken") == 0, 1)
                        .otherwise(F.col("wickets_taken")), 2)
                    ).withColumn(
                        "strike_rate",
                        F.round(F.col("legal_balls") / F.when(F.col("wickets_taken") == 0, 1)
                        .otherwise(F.col("wickets_taken")), 2)
                    ).select("bowler","matches_played","overs_bowled","maiden_overs","runs_conceded",
                        "wickets_taken","economy","bowling_avg","strike_rate","best_bowling_figure",
                        "three_wickets_haul","five_wickets_haul","wides","no_balls"
                    ).orderBy(
                        F.desc("wickets_taken"),
                        "bowling_avg"
                    )