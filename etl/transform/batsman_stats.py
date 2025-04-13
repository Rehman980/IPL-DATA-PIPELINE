from pyspark.sql import functions as F
from pyspark.sql.window import Window

class BatsmanStats:
    @staticmethod
    def analyze(deliveries_df):
        return deliveries_df.withColumn("batsman_match_runs", 
                        F.sum("batsman_runs").over(Window.partitionBy("match_id", "batsman"))
                    ).withColumn("batsman_dismissed", 
                        F.when(F.col("player_dismissed") == F.col("batsman"), 1)
                        .otherwise(0)
                    ).withColumn("batsman_match_dismissed",
                        F.sum("batsman_dismissed").over(Window.partitionBy("match_id", "batsman"))     
                    ).groupBy("batsman").agg(
                        F.countDistinct("match_id").alias("matches_played"),
                        F.sum("batsman_runs").alias("total_runs"),
                        F.count(F.when(F.col("wide_runs") == 0,"ball")).alias("balls_faced"),
                        F.sum("batsman_dismissed").alias("times_out"),
                        F.sum(F.when(F.col("batsman_runs") == 4, 1).otherwise(0)).alias("fours"),
                        F.sum(F.when(F.col("batsman_runs") == 6, 1).otherwise(0)).alias("sixes"),
                        F.max("batsman_match_runs").alias("highest_score"),
                        F.countDistinct(F.when((F.col("batsman_match_runs") == 0) & (F.col("batsman_match_dismissed") == 1), "match_id")).alias("ducks"),
                        F.countDistinct(F.when(F.col("batsman_match_runs").between(50,99), "match_id")).alias("half_centuries"),
                        F.countDistinct(F.when(F.col("batsman_match_runs") >= 100, "match_id")).alias("centuries"),
                    ).withColumn(
                        "strike_rate",
                        F.round(F.col("total_runs") / F.col("balls_faced") * 100, 2)
                    ).withColumn(
                        "average",
                        F.round(F.col("total_runs") / (F.when(F.col("times_out") == 0, 1)
                        .otherwise(F.col("times_out"))), 2)
                    ).withColumn("not_outs", 
                        F.col("matches_played") - F.col("times_out")
                    ).drop(
                        "times_out","zero_runs"
                    ).orderBy(
                        F.desc("total_runs"),
                        F.desc("average")
                    )