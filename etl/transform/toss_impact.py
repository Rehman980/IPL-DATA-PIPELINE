from pyspark.sql import functions as F

class TossImpact:
    @staticmethod
    def analyze(matches_df):
        return matches_df.groupBy("toss_winner", "toss_decision").agg(
            F.count("*").alias("total_matches"),
            F.sum(F.when(F.col("toss_winner") == F.col("winner"), 1)
               .otherwise(0)).alias("matches_won")
        ).withColumn(
            "win_percentage",
            F.round(F.col("matches_won") / F.col("total_matches") * 100, 2)
        ).withColumnRenamed("toss_winner","team"
        ).orderBy(
            "toss_winner", 
            "toss_decision"
        )