import pytest
from etl.pipeline import Pipeline

class TestPipeline:
    def test_pipeline_init(self):
        pipeline = Pipeline()
        assert pipeline is not None