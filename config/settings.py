import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    GCP_PROJECT = os.getenv("GCP_PROJECT")
    GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    
    @classmethod
    def validate(cls):
        if not cls.GCP_PROJECT:
            raise ValueError("GCP_PROJECT environment variable not set")
        if not cls.GCP_CREDENTIALS:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
        print('GCP connection validated')