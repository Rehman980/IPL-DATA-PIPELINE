import logging
import os

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("ipl_analytics.log"),
            logging.StreamHandler()
        ]
    )