from pyspark.sql import SparkSession, DataFrame
from config.constants import Config
from config.settings import Settings
from src.utils.bigquery_utils import BigQueryUtils

class DataWriter:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        BigQueryUtils.configure_spark_for_bigquery(spark)
        
    def write_to_bigquery(self, df: DataFrame, table_name: str, write_mode: str = "overwrite"):
        """Write DataFrame to BigQuery"""
        (df.write
           .format("bigquery")
           .option("table", BigQueryUtils.get_bigquery_table_path(table_name))
           .option("temporaryGcsBucket", Settings.GCS_BUCKET)
           .mode(write_mode)
           .save())
    
    def write_to_gcs(self, df: DataFrame, path: str, format: str = "parquet"):
        """Write DataFrame to GCS"""
        full_path = f"gs://{Settings.GCS_BUCKET}/{path}"
        (df.write
           .format(format)
           .mode("overwrite")
           .save(full_path))