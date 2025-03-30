import logging
from pyspark.sql import SparkSession
from config.settings import Settings
from config.constants import Config
from src.data_loader import DataLoader
from src.data_transformer import DataTransformer
from src.data_writer import DataWriter
from src.data_quality import DataQualityChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Create and configure Spark session"""
    return ((SparkSession.builder
         .appName("IPL Data Processing")
         .config("spark.jars", "C:\\spark_jars\\spark-bigquery-with-dependencies_2.12-0.28.0.jar")
         .getOrCreate()))

def process_matches(spark: SparkSession):
    """Process matches data"""
    logger.info("Processing matches data")
    
    loader = DataLoader(spark)
    transformer = DataTransformer()
    writer = DataWriter(spark)
    quality_checker = DataQualityChecker()
    
    # Load data
    matches_df = loader.load_from_gcs(Config.RAW_MATCHES_PATH, "matches.json")
    
    # Validate data
    if not quality_checker.run_checks(matches_df, "matches"):
        raise ValueError("Data quality checks failed for matches data")
    
    # Transform data
    matches_transformed = transformer.transform_matches(matches_df)
    team_stats = transformer.create_team_stats(matches_transformed)
    
    # Write results
    writer.write_to_bigquery(matches_transformed, Config.FACT_MATCHES_TABLE)
    writer.write_to_bigquery(team_stats, Config.DIM_TEAMS_TABLE)
    writer.write_to_gcs(matches_transformed, f"{Config.PROCESSED_PATH}matches/")

def main():
    Settings.validate()
    spark = create_spark_session()
    
    try:
        process_matches(spark)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()