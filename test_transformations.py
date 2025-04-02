import pytest
from pyspark.sql import SparkSession
from src.data_transformer import DataTransformer
from pyspark.sql.types import StructType, StructField, StringType, DateType

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_transform_matches(spark):
    schema = StructType([
        StructField("date", StringType(), False),
        StructField("team1", StringType(), False),
        StructField("team2", StringType(), False)
    ])
    
    data = [("2023-04-09", "MI", "CSK")]
    df = spark.createDataFrame(data, schema)
    
    transformed = DataTransformer.transform_matches(df)
    assert "season" in transformed.columns
    assert transformed.first()["season"] == 2023