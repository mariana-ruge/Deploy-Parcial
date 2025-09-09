import json
import pytest
from moto import mock_aws
import boto3
from lambda_functions.lambda1_fetch import lambda_handler


@mock_aws
def test_lambda1_saves_file_to_s3(monkeypatch):
    """Test that Lambda1 saves a file with timestamp in S3"""

    # mock bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "dolar_raw_test"
    s3.create_bucket(Bucket=bucket_name)

    # 👉 Forzar variable de entorno al bucket de prueba
    monkeypatch.setenv("BUCKET_NAME", bucket_name)

    # fake response from BanRep con raise_for_status incluido
    class FakeResponse:
        def __init__(self, data):
            self._data = data
            self.status_code = 200
        def json(self):
            return self._data
        def raise_for_status(self):
            return None  # no error

    fake_data = {"dolar": [{"fecha": "2025-09-06", "valor": 4050.25}]}
    monkeypatch.setattr("lambda_functions.lambda1_fetch.requests.get",
                        lambda url, timeout=10: FakeResponse(fake_data))

    event, context = {}, {}
    result = lambda_handler(event, context)

    # check that lambda reported success
    assert result["status"] == "success"

    # check that file exists in S3
    response = s3.list_objects_v2(Bucket=bucket_name)
    assert response["KeyCou]()
