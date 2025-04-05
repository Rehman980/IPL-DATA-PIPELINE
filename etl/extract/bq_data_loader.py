from typing import List
from connectors.bigquery_connector import BigQueryConnector
from utils.logging_config import logger
from data_models.matches import matches_schema
from data_models.deliveries import deliveries_schema
from utils.validation import validate_spark_df
from pyspark.sql import SparkSession, DataFrame

class BQDataLoader:
    def __init__(self):
        self.bq = BigQueryConnector()
        self.spark = SparkSession.builder.getOrCreate()

    def load_to_staging(self, file_paths: List[str], file_type: str) -> bool:
        """
        Load files to BigQuery staging tables with validation
        Args:
            file_paths: List of local file paths
            file_type: 'matches' or 'deliveries'
        Returns:
            bool: True if successful
        """
        try:
            if not file_paths:
                logger.info(f"No {file_type} files to load")
                return False

            # Read and validate data
            schema = matches_schema if file_type == 'matches' else deliveries_schema
            df = self.spark.read.parquet(*file_paths)
            validate_spark_df(df, schema)

            # Convert to pandas for BigQuery load
            pandas_df = df.toPandas()
            
            logger.info(f"Loading {len(pandas_df)} {file_type} records to staging")
            self.bq.load_to_staging(pandas_df, file_type)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {file_type} data: {str(e)}")
            raise