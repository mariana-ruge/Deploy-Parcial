"""
Unit tests for Lambda 1 (Dollar fetcher).
This lambda fetches the dollar values from BanRep and stores them in S3.
"""

import json
import pytest
from moto import mock_aws
import boto3
from lambda_functions.lambda1_fetch import lambda_handler


@mock_aws
def test_lambda1_saves_file_to_s3(monkeypatch):
    """Test that Lambda1 saves a file with timestamp in S3"""

    # Mock bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "dolar_raw_test"
    s3.create_bucket(Bucket=bucket_name)

    # Mock response from BanRep
    fake_data = {"dolar": [{"fecha": "2025-09-06", "valor": 4050.25}]}

    def mock_get(url, timeout=10):
        class MockResponse:
            def raise_for_status(self): pass
            def json(self): return fake_data
        return MockResponse()

    monkeypatch.setattr("lambda_functions.lambda1_fetch.requests.get", mock_get)

    # Run lambda
    event, context = {}, {}
    result = lambda_handler(event, context)

    # Check return status
    assert result["status"] == "success"

    # Check that file exists in S3
    response = s3.list_objects_v2(Bucket=bucket_name)
    assert response["KeyCount"] == 1
    key = response["Contents"][0]["Key"]
    assert key.startswith("dolar-") and key.endswith(".json")
