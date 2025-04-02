import os
import glob
from src.utils.logger import setup_logger

logger = setup_logger()

def clean_temp_files():
    patterns = [
        'data/raw/*.parquet',
        'data/processed/*.parquet',
        'temp_*.parquet'
    ]
    
    for pattern in patterns:
        for file in glob.glob(pattern):
            os.remove(file)
            logger.info(f"Removed temporary file: {file}")

if __name__ == "__main__":
    clean_temp_files()