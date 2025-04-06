import logging
import os

def logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("ipl_analytics.log"),
            logging.StreamHandler()
        ]
    )