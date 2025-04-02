from google.cloud import storage
from config.settings import Settings
import tempfile
import os

class GCSOperator:
    def __init__(self):
        self.client = storage.Client()
    
    def download(self, blob_path, local_dir="data"):
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, os.path.basename(blob_path))
        
        bucket = self.client.bucket(Settings.GCS_BUCKET)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_path)
        return local_path
    
    def upload(self, local_path, gcs_path):
        bucket = self.client.bucket(Settings.GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)