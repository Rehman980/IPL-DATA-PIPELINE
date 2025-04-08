from google.cloud import bigquery
from config.constants import PROJECT_ID, STAGING_DATASET
from pandas import Timestamp

class Watermark:
    _TABLE = f"{PROJECT_ID}.{STAGING_DATASET}.watermarks"
    
    @classmethod
    def get_last_processed(cls, file_type):
        client = bigquery.Client()
        query = f"""
        SELECT MAX(updated_timestamp) as last_run
        FROM `{cls._TABLE}`
        WHERE file_type = '{file_type}'
        """
        result = client.query(query).result()
        row = list(result)
        return row[0].last_run if row and row[0].last_run else Timestamp('2000-01-01 00:00:00.000000 UTC')
    
    @classmethod
    def update(cls, file_type, timestamp):
        client = bigquery.Client()
        query = f"""
        INSERT INTO `{cls._TABLE}`
        (file_type, updated_timestamp)
        VALUES ('{file_type}', '{timestamp}')
        """
        client.query(query).result()