import json
import pytest
from unittest.mock import patch
from lambda_functions.lambda2_process import lambda_handler


@patch("lambda_functions.lambda2_process.insert_into_rds")
def test_lambda2_inserts_into_rds(mock_insert):
    """Test que Lambda2 lee archivo desde S3 y llama a RDS insert"""

    fake_event = {
        "Records": [
            {"s3": {"bucket": {"name": "dolar_raw_test"}, "object": {"key": "dolar-123.json"}}}
        ]
    }

    # 👉 Mockear S3
    with patch("lambda_functions.lambda2_process.get_file_from_s3") as mock_s3:
        mock_s3.return_value = json.dumps([
            {"fechahora": "2025-09-06T23:09:57", "valor": 4050.25}
        ])

        # Ejecutar lambda
        lambda_handler(fake_event, None)

    # Verificar que insert fue llamado una vez
    mock_insert.assert_called_once()
