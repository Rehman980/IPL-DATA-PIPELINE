import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT")
BUCKET_NAME = os.getenv("GCS_BUCKET")
DATASET = os.getenv("BQ_DATASET")
STAGING_DATASET = os.getenv("STAGING_DATASET")