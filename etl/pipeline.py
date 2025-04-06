from utils.watermark import Watermark
from connectors.gcs_connector import GCSConnector
from connectors.bigquery_connector import BigQueryConnector
from connectors.spark_connector import SparkConnector
from utils.logging_config import logger
from datetime import datetime
from data_models.matches import matches_schema
from data_models.deliveries import deliveries_schema
from etl.transform import (team_performance, 
                           batsman_stats, 
                           bowler_stats, 
                           toss_impact, 
                           player_of_match, 
                           death_overs, 
                           phase_comparison, 
                           venue_analysis)

class Pipeline:
    def __init__(self):
        self.gcs = GCSConnector()
        self.bq = BigQueryConnector()
        self.spark_conn = SparkConnector()
    
    def run(self):
        logger.info("Starting IPL Analytics Pipeline")
        self.spark = self.spark_conn.spark_start()
        logger.info("Spark session started")
        
        try:
            # 1. Extract and Load to Staging
            self._extract_and_load()
            
            # 2. Transform
            transformed_data = self._transform()
            
            # 3. Load Results
            self._load_results(transformed_data)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark_conn.spark_end(self.spark)
            logger.info("Spark session ended")
    
    def _extract_and_load(self):
        """Download new files and load to staging"""
        last_matches = Watermark.get_last_processed("matches")
        last_deliveries = Watermark.get_last_processed("deliveries")
        
        new_matches = self.gcs.download_new_files("raw/matches/", last_matches)
        new_deliveries = self.gcs.download_new_files("raw/deliveries/", last_deliveries)
        
        if new_matches:
            self.bq.load_to_staging(new_matches, "matches")
            Watermark.update("matches", datetime.now())
        
        if new_deliveries:
            self.bq.load_to_staging(new_deliveries, "deliveries")
            Watermark.update("deliveries", datetime.now())
    
    def _transform(self):
        """Execute all transformations"""
        # Get data from staging
        matches_df = self.spark.createDataFrame(
            self.bq.get_staging_data("matches"),
            schema=matches_schema
        )
        deliveries_df = self.spark.createDataFrame(
            self.bq.get_staging_data("deliveries"),
            schema=deliveries_schema
        )
        
        # Execute transformations
        return {
            "team_performance": team_performance.analyze(matches_df, deliveries_df).toPandas(),
            "batsman_stats": batsman_stats.analyze(deliveries_df).toPandas(),
            "bowler_stats": bowler_stats.analyze(deliveries_df).toPandas(),
            "toss_impact": toss_impact.analyze(matches_df).toPandas(),
            "player_of_match": player_of_match.analyze(matches_df).toPandas(),
            "death_overs": death_overs.analyze(deliveries_df).toPandas(),
            "phase_comparison": phase_comparison.analyze(deliveries_df).toPandas(),
            "venue_analysis": venue_analysis.analyze(matches_df, deliveries_df).toPandas()
        }
    
    def _load_results(self, results):
        """Load results to BigQuery and GCS"""

        self.bq.write_results(results)
        self.gcs.upload_csv(results)