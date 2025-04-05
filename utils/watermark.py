from google.cloud import bigquery
from config.constants import PROJECT_ID, DATASET
from datetime import datetime

class Watermark:
    _TABLE = f"{PROJECT_ID}.{DATASET}.watermarks"
    
    @classmethod
    def get_last_processed(cls, file_type):
        client = bigquery.Client()
        query = f"""
        SELECT MAX(timestamp) as last_run
        FROM `{cls._TABLE}`
        WHERE file_type = '{file_type}'
        """
        result = client.query(query).result()
        row = list(result)
        return row[0].last_run if row and row[0].last_run else datetime.min
    
    @classmethod
    def update(cls, file_type, timestamp):
        client = bigquery.Client()
        query = f"""
        INSERT INTO `{cls._TABLE}`
        (file_type, timestamp)
        VALUES ('{file_type}', '{timestamp.isoformat()}')
        """
        client.query(query).result()