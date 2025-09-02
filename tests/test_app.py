import json
import app
import boto3
from moto import mock_aws
from pytest import raises

def test_fetch_and_store_happy_path(mocker, monkeypatch):
    # Configurar S3_BUCKET para la prueba
    monkeypatch.setenv("S3_BUCKET", "dolar_raw_test")

    # Mock de la respuesta HTTP
    fake_payload = b'[["1756818048000","4040"],["1756818069000","4041.3333"]]'
    mock_get = mocker.patch("app.requests.get")
    mock_resp = mocker.MagicMock()
    mock_resp.content = fake_payload
    mock_resp.raise_for_status = lambda: None
    mock_get.return_value = mock_resp

    with mock_aws():
        # Crear bucket simulado
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="dolar_raw_test")

        # Ejecutar
        result = app.fetch_and_store()

        # Afirmaciones
        assert result["bucket"] == "dolar_raw_test"
        key = result["key"]
        got = s3.get_object(Bucket="dolar_raw_test", Key=key)["Body"].read()
        assert got == fake_payload
        assert key.startswith("dolar-") and key.endswith(".json")

def test_missing_bucket_env(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with raises(RuntimeError):
        app.fetch_and_store()

