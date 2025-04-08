from pyspark.sql import SparkSession
from config.constants import SPARK_CONFIGS

class SparkConnector:
    
    def spark_start(self):
        builder = SparkSession.builder.appName("MyApp")
        for key, value in SPARK_CONFIGS.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        return spark
    
    def spark_end(self, spark):
        spark.stop()