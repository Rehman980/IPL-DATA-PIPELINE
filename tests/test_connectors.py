import pytest
from connectors.gcs_connector import GCSConnector
from connectors.bigquery_connector import BigQueryConnector

class TestConnectors:
    def test_gcs_connector_init(self):
        connector = GCSConnector()
        assert connector is not None
    
    def test_bq_connector_init(self):
        connector = BigQueryConnector()
        assert connector is not None