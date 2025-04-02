import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Required
    GCP_PROJECT = os.getenv("GCP_PROJECT")
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    
    # Optional with defaults
    LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls):
        if not cls.GCP_PROJECT:
            raise ValueError("GCP_PROJECT must be set in .env")
        if not cls.GCS_BUCKET:
            raise ValueError("GCS_BUCKET must be set in .env")