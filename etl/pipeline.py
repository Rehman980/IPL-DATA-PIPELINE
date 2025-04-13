from utils.watermark import Watermark
from connectors.gcs_connector import GCSConnector
from connectors.bigquery_connector import BigQueryConnector
from connectors.spark_connector import SparkConnector
from utils.logging_config import logger
from pandas import Timestamp
from data_models.matches import matches_schema
from data_models.deliveries import deliveries_schema
from etl.transform import (team_performance, 
                           batsman_stats, 
                           bowler_stats, 
                           toss_impact, 
                           player_of_match,
                           phase_comparison, 
                           venue_analysis)

class Pipeline:
    def __init__(self):
        self.gcs = GCSConnector()
        self.bq = BigQueryConnector()
        self.spark_conn = SparkConnector()
        self.spark_conn = SparkConnector()
    
    def run(self):
        logger.info('Starting IPL Analytics Pipeline')
        self.spark = self.spark_conn.spark_start()
        logger.info('Spark session started')
        logger.info("Starting IPL Analytics Pipeline")
        self.spark = self.spark_conn.spark_start()
        logger.info("Spark session started")
        
        try:
            # 1. Extract and Load to Staging
            load_status = self._extract_and_load()
            if load_status:
                # 2. Transform
                transformed_data = self._transform()
                # 3. Load Results
                self._load_results(transformed_data)
            else:
                logger.info('No new data to process')
                
            logger.info('Pipeline completed successfully')
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark_conn.spark_end(self.spark)
            logger.info("Spark session ended")
            self.spark_conn.spark_end(self.spark)
            logger.info("Spark session ended")
            self._delete_temp_folder()
    
    def _extract_and_load(self):
        """Download new files and load to staging"""
        logger.info('Downloading new files from GCS')
        
        # Get last processed timestamps
        last_matches = Watermark.get_last_processed("matches")
        last_deliveries = Watermark.get_last_processed("deliveries")

        new_matches = self.gcs.download_new_files("raw/matches/", last_matches)
        new_deliveries = self.gcs.download_new_files("raw/deliveries/", last_deliveries)
        
        if not new_matches and not new_deliveries:
            return False

        logger.info(f"New matches files: {new_matches}")
        logger.info(f"New deliveries files: {new_deliveries}")

        # Load new files to staging
        logger.info('Loading new files to staging')
        if new_matches:
            if self.bq.load_to_staging(new_matches, "matches"):
                Watermark.update("matches", Timestamp.now(tz='UTC'))
                logger.info('Matches data loaded to staging')
        
        if new_deliveries:
            if self.bq.load_to_staging(new_deliveries, "deliveries"):
                Watermark.update("deliveries", Timestamp.now(tz='UTC'))
                logger.info('Deliveries data loaded to staging')
        return True
    
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
            "team_performance": team_performance.TeamPerformance.analyze(matches_df, deliveries_df).toPandas(),
            "batsman_stats": batsman_stats.BatsmanStats.analyze(deliveries_df).toPandas(),
            "bowler_stats": bowler_stats.BowlerStats.analyze(deliveries_df).toPandas(),
            "toss_impact": toss_impact.TossImpact.analyze(matches_df).toPandas(),
            "player_of_match": player_of_match.PlayerOfMatch.analyze(matches_df).toPandas(),
            "phase_comparison": phase_comparison.PhaseComparison.analyze(deliveries_df).toPandas(),
            "venue_analysis": venue_analysis.VenueAnalysis.analyze(matches_df, deliveries_df).toPandas()
        }
    
    def _load_results(self, results):
        """Load results to BigQuery and GCS"""
        logger.info('Loading results to BigQuery')

        self.bq.write_results(results)
        self.gcs.upload_csv(results)
        logger.info('Results loaded to BigQuery')
        logger.info('Uploading results to GCS')
        self.gcs.upload_csv(results)
        logger.info('Results uploaded to GCS')

    def _delete_temp_folder(self):
        """Delete temporary folder"""
        try:
            self.gcs.delete_temp_folder()
            logger.info('Temporary folder deleted')
        except Exception as e:
            logger.error(f"Failed to delete temporary folder: {str(e)}")