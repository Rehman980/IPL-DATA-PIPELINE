from google.cloud import storage
from config.settings import Settings
import os

class GCSUtils:
    @staticmethod
    def upload_file(local_path: str, gcs_path: str):
        """Upload file to GCS"""
        client = storage.Client()
        bucket = client.bucket(Settings.GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        
    @staticmethod
    def download_file(gcs_path: str, local_path: str):
        """Download file from GCS"""
        client = storage.Client()
        bucket = client.bucket(Settings.GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.download_to_filename(local_path)