import os
from dotenv import load_dotenv

load_dotenv()

# Constants for the pipeline
PROJECT_ID = os.getenv("GCP_PROJECT")
BUCKET_NAME = os.getenv("GCS_BUCKET")
DATASET = os.getenv("BQ_DATASET")
STAGING_DATASET = os.getenv("STAGING_DATASET")


# Constants for Sparksession
SPARK_CONFIGS ={
                "spark.app.name": "IPL Analytics",
                "spark.master": "local[*]",
                "spark.executor.memory": "2g",
                "spark.driver.memory": "1g"
                }