from pyspark.sql import DataFrame

def validate_spark_df(df: DataFrame, schema):
    """Validate Spark DataFrame against schema"""
    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a Spark DataFrame")
    
    if set(df.columns) != set(field.name for field in schema.fields):
        raise ValueError("Schema mismatch")
    
    return True