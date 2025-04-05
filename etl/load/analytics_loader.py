from typing import Dict
import pandas as pd
from connectors.bigquery_connector import BigQueryConnector
from utils.logging_config import logger
from utils.data_utils import validate_data

class AnalyticsLoader:
    def __init__(self):
        self.bq = BigQueryConnector()

    def write_results(self, results: Dict[str, pd.DataFrame]) -> bool:
        """
        Write transformed results to BigQuery analytics tables
        Args:
            results: Dictionary of {result_type: DataFrame}
        Returns:
            bool: True if all writes succeeded
        """
        try:
            for result_type, df in results.items():
                logger.info(f"Loading {result_type} data ({len(df)} rows)")
                
                # Validate before writing
                validate_data(df)
                
                # Write to BigQuery
                self.bq.write_results(result_type, df)
                
            logger.info("All analytics data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load analytics data: {str(e)}")
            raise