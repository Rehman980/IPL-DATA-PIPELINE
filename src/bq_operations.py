from google.cloud import bigquery
from config.settings import Settings
import pandas as pd

class BigQueryOperator:
    def __init__(self):
        self.client = bigquery.Client()
    
    def query_to_dataframe(self, query):
        return self.client.query(query).to_dataframe()
    
    def dataframe_to_table(self, df, table_id):
        full_table_id = f"{Settings.GCP_PROJECT}.{table_id}"
        df.to_gbq(
            destination_table=full_table_id,
            project_id=Settings.GCP_PROJECT,
            if_exists="replace"
        )