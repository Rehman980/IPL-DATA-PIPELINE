from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import json
from typing import Dict
from config.constants import Config
from config.settings import Settings
from src.utils.gcs_utils import GCSUtils

class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def load_from_gcs(self, path: str, schema_file: str = None) -> DataFrame:
        """Load data from GCS with optional schema validation"""
        full_path = f"gs://{Settings.GCS_BUCKET}/{path}"
        df = self.spark.read.parquet(full_path)
        
        if schema_file:
            self.validate_schema(df, schema_file)
            
        return df
    
    def validate_schema(self, df: DataFrame, schema_file: str):
        """Validate DataFrame against JSON schema"""
        schema_path = f"data/schemas/{schema_file}"
        local_schema_path = "/tmp/schema.json"
        
        # Download schema from GCS if running in cloud
        if schema_path.startswith("gs://"):
            GCSUtils.download_file(schema_path, local_schema_path)
            schema_path = local_schema_path
        
        with open(schema_path) as f:
            schema = json.load(f)
        
        # Basic validation - can be expanded
        required_fields = schema.get("required", [])
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")