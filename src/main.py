from pyspark.sql import SparkSession
from src.gcs_operations import GCSOperator
from src.bq_operations import BigQueryOperator
from src.transformations import IPLTransformer
import os

def main():
    # Initialize clients
    spark = SparkSession.builder.appName("IPL Processing").getOrCreate()
    gcs = GCSOperator()
    bq = BigQueryOperator()

    try:
        # 1. Download data
        local_matches = gcs.download(GCS.RAW_MATCHES_PATH)
        
        # 2. Process with PySpark
        matches_df = spark.read.parquet(local_matches)
        processed_df = IPLTransformer.process_matches(matches_df)
        team_stats = IPLTransformer.calculate_team_stats(processed_df)
        
        # 3. Save and upload
        temp_output = "temp_team_stats.parquet"
        team_stats.toPandas().to_parquet(temp_output)
        gcs.upload(temp_output, f"{GCS.PROCESSED_PATH}team_stats.parquet")
        
        # 4. Load to BigQuery
        bq.dataframe_to_table(team_stats.toPandas(), f"{BigQuery.DATASET}.team_stats")
        
    finally:
        spark.stop()
        if os.path.exists(temp_output):
            os.remove(temp_output)

if __name__ == "__main__":
    main()