from google.cloud import bigquery
from config.constants import PROJECT_ID, STAGING_DATASET, DATASET
from config.tables import STAGING_TABLES, ANALYTICS_TABLES
import pandas as pd
import logging

class BigQueryConnector:
    def __init__(self):
        self.client = bigquery.Client()
    
    def load_to_staging(self, file_paths, table_type):
        """Load parquet files to staging tables"""
        table_id = f"{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLES[table_type]}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        for file_path in file_paths:
            with open(file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_id, job_config=job_config)
                job.result()
                logging.info(f"Loaded {file_path} to {table_id}")
    
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