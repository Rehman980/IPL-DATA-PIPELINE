from pyspark.sql import SparkSession
from config.constants import Config
from config.settings import Settings

class BigQueryUtils:
    @staticmethod
    def configure_spark_for_bigquery(spark: SparkSession):
        """Configure Spark session for BigQuery"""
        spark.conf.set("viewsEnabled", "true")
        spark.conf.set("materializationDataset", Config.BIGQUERY_DATASET)
        spark.conf.set("project", Settings.GCP_PROJECT)
        
    @staticmethod
    def get_bigquery_table_path(table_name: str) -> str:
        """Get full BigQuery table path"""
        return f"{Settings.GCP_PROJECT}.{Config.BIGQUERY_DATASET}.{table_name}"