import pytest
from pyspark.sql import SparkSession
from src.data_quality import DataQualityChecker
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_null_check(spark):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True)
    ])
    
    data = [(1, "Alice"), (2, None), (3, "Bob")]
    df = spark.createDataFrame(data, schema)
    
    # Should pass as null percentage (33%) is above default threshold (5%)
    assert not DataQualityChecker.check_nulls(df, "test")