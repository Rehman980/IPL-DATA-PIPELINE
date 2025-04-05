from pyspark.sql import functions as F

class PlayerOfMatch:
    @staticmethod
    def analyze(matches_df):
        return matches_df.groupBy("player_of_match").agg(
            F.count("*").alias("awards"),
            F.collect_set("Season").alias("seasons")
        ).orderBy(F.desc("awards"))