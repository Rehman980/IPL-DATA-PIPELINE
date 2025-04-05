import pytest
from pyspark.sql import SparkSession
from data_models.matches import matches_schema
from data_models.deliveries import deliveries_schema
from etl.transform import team_performance

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_team_performance(spark):
    # Create test DataFrames
    matches_data = [(1, "2023", "Mumbai", "2023-04-09", "MI", "CSK", "MI", "bat", "normal", 0, "MI", 20, 0, "Player1", "Wankhede", "Ump1", "Ump2", None)]
    matches_df = spark.createDataFrame(matches_data, matches_schema)
    
    deliveries_data = [(1, 1, "MI", "CSK", 1, 1, "Player1", "Player2", "Bowler1", 0, 0, 0, 0, 0, 0, 4, 0, 4, None, None, None)]
    deliveries_df = spark.createDataFrame(deliveries_data, deliveries_schema)
    
    # Test transformation
    result = team_performance.TeamPerformance.analyze(matches_df, deliveries_df)
    assert result.count() > 0