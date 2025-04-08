from google.cloud import bigquery
from config.constants import PROJECT_ID, STAGING_DATASET, DATASET
from config.tables import STAGING_TABLES, ANALYTICS_TABLES
import logging
import pandas as pd
from utils.logging_config import logger
from data_models.matches import matches_schema_bq
from data_models.deliveries import deliveries_schema_bq

class BigQueryConnector:
    def __init__(self):
        self.client = bigquery.Client()
    
    def insert_df_into_db_table(self, df, table_id):
        """Load DataFrame to staging table with upload timestamp"""

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema = matches_schema_bq if 'matches' in table_id else deliveries_schema_bq,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()

    def load_to_staging(self, file_paths, table_type):
        """Load parquet files to staging tables"""
        
        table_id = f"{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLES[table_type]}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        try:
            for file_path in file_paths:
                with open(file_path, "rb") as source_file:
                    df = pd.read_parquet(source_file)
                    # Add upload timestamp column
                    df['inserted_timestamp'] = pd.Timestamp.now(tz='UTC')
                    job = self.insert_df_into_db_table(
                        df, table_id)
                    logging.info(f"Loaded {file_path} to {table_id}")
            return True
        except Exception as e:
            logger.info(f"Failed to load {file_paths} to {table_id}: {str(e)}")
            return False
    
    def get_staging_data(self, table_type):
        """Query staging table and return pandas DataFrame"""
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLES[table_type]}`
        """
        return self.client.query(query).to_dataframe()
    
    def write_results(self, results):
        """Write transformed results to analytics tables"""
        for result_type, df in results.items():
            table_id = f"{PROJECT_ID}.{DATASET}.{ANALYTICS_TABLES[result_type]}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config)
            job.result()
            logging.info(f"Loaded {result_type} data to {table_id}")