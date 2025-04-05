import os
from datetime import datetime
from typing import Dict
import pandas as pd
from connectors.gcs_connector import GCSConnector
from utils.logging_config import logger

class GCSExporter:
    def __init__(self):
        self.gcs = GCSConnector()

    def export_results(self, results: Dict[str, pd.DataFrame]) -> bool:
        """
        Export results to GCS as CSV files
        Args:
            results: Dictionary of {result_type: DataFrame}
        Returns:
            bool: True if all exports succeeded
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            for result_type, df in results.items():
                gcs_path = f"analytics/{result_type}/{timestamp}.csv"
                logger.info(f"Exporting {result_type} to {gcs_path}")
                
                # Write to temporary local file
                temp_path = f"temp_{result_type}.csv"
                df.to_csv(temp_path, index=False)
                
                # Upload to GCS
                self.gcs.upload_csv(temp_path, gcs_path)
                
                # Cleanup
                os.remove(temp_path)
                
            logger.info("All results exported to GCS")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export results: {str(e)}")
            raise