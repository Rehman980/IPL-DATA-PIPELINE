import pytest
from unittest.mock import Mock, patch
from src.gcs_operations import GCSOperator

@pytest.fixture
def mock_gcs():
    with patch('google.cloud.storage.Client') as mock:
        yield mock

def test_download(mock_gcs):
    operator = GCSOperator()
    operator.download("test/path.parquet")
    mock_gcs.return_value.bucket.return_value.blob.return_value.download_to_filename.assert_called_once()