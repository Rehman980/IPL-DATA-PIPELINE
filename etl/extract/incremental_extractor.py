import os
from datetime import datetime
from typing import List
from connectors.gcs_connector import GCSConnector
from utils.watermark import Watermark
from utils.logging_config import logger

class IncrementalExtractor:
    def __init__(self):
        self.gcs = GCSConnector()
        self.watermark = Watermark()

    def get_new_files(self, file_type: str) -> List[str]:
        """
        Get new files since last run for a specific file type
        Args:
            file_type: 'matches' or 'deliveries'
        Returns:
            List of local file paths
        """
        try:
            last_run = self.watermark.get_last_processed(file_type)
            prefix = f"raw/{file_type}/"
            
            logger.info(f"Checking for new {file_type} files since {last_run}")
            new_files = self.gcs.download_new_files(prefix, last_run)
            
            if not new_files:
                logger.info(f"No new {file_type} files found")
                return []
                
            logger.info(f"Found {len(new_files)} new {file_type} files")
            return new_files
            
        except Exception as e:
            logger.error(f"Failed to extract {file_type} files: {str(e)}")
            raise

    def cleanup_temp_files(self, file_paths: List[str]):
        """Cleanup downloaded temporary files"""
        for file_path in file_paths:
            try:
                os.remove(file_path)
                logger.debug(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to cleanup {file_path}: {str(e)}")