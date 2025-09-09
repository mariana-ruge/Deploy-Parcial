"""
Unit tests for Lambda 2 (Dollar processor).
This lambda triggers on new S3 files, parses JSON, and inserts into RDS.
"""

import json
from unittest.mock import patch
from lambda_functions.lambda1_fetch import lambda_handler
from lambda_functions.lambda2_process import lambda_handler



@patch("lambda_functions.lambda2_process.insert_into_rds")
def test_lambda2_inserts_into_rds(mock_insert):
    """Test that Lambda2 reads file content and calls RDS insert"""

    fake_event = {
        "Records": [
            {"s3": {"bucket": {"name": "dolar_raw_test"}, "object": {"key": "dolar-123.json"}}}
        ]
    }

    # Mock S3 response
    with patch("lambda_functions.lambda2_process.get_file_from_s3") as mock_s3:
        mock_s3.return_value = json.dumps(
            [{"fechahora": "2025-09-06T23:09:57", "valor": 4050.25}]
        )

        lambda_handler(fake_event, None)

    # Validate RDS insert called once
    mock_insert.assert_called_once()
