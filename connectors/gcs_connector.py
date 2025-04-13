from google.cloud import storage
from config.constants import BUCKET_NAME, TEMP_FOLDER
import os
from datetime import datetime
import shutil

class GCSConnector:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(BUCKET_NAME)
    
    def download_new_files(self, prefix, last_processed):
        """Download new files since last run"""
        blobs = self.bucket.list_blobs(prefix=prefix)
        new_files = []
        
        for blob in blobs:
            if blob.name.endswith(".parquet") and blob.updated > last_processed:
                local_path = f"{TEMP_FOLDER}/{blob.name}"
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                blob.download_to_filename(local_path)
                new_files.append(local_path)
        
        return new_files
    
    def upload_csv(self, results):
        """Upload DataFrame as CSV to GCS"""

        for result_type, df in results.items():
            destination_path = f"analytics/{result_type}/{datetime.now().strftime('%Y%m%d%H%M')}.csv"
            temp_path = f"temp_{destination_path.replace('/', '_')}.csv"
            df.to_csv(temp_path, index=False)
            blob = self.bucket.blob(destination_path)
            blob.upload_from_filename(temp_path)
            os.remove(temp_path)

    def delete_temp_folder(self):
        shutil.rmtree(f"{TEMP_FOLDER}/")