from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from config.constants import Config

class DataQualityChecker:
    @staticmethod
    def run_checks(df: DataFrame, table_name: str) -> bool:
        """Run all data quality checks"""
        checks = {
            "null_check": DataQualityChecker.check_nulls(df, table_name),
            "id_range_check": DataQualityChecker.check_id_ranges(df, table_name),
            "value_validation": DataQualityChecker.validate_values(df, table_name)
        }
        
        return all(checks.values())
    
    @staticmethod
    def check_nulls(df: DataFrame, table_name: str) -> bool:
        """Check for excessive null values in critical columns"""
        if table_name == "matches":
            critical_columns = ["match_id", "date", "team1", "team2"]
        else:
            critical_columns = ["match_id", "inning", "over", "ball"]
            
        null_results = {}
        total_rows = df.count()
        
        for column in critical_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_rows
            null_results[column] = null_percentage <= Config.NULL_THRESHOLD
            
        return all(null_results.values())
    
    @staticmethod
    def check_id_ranges(df: DataFrame, table_name: str) -> bool:
        """Validate ID ranges"""
        if table_name == "matches":
            invalid_ids = df.filter(
                (col("match_id") < Config.MATCH_ID_MIN) | 
                (col("match_id") > Config.MATCH_ID_MAX)
            )
            return invalid_ids.count() == 0
        return True