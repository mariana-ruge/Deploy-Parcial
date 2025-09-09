"""
Unit tests for FastAPI app.

Endpoints:
- GET /health
- POST /consulta
"""

from fastapi.testclient import TestClient
from app import app   # ✅ corregido: antes era from app.main import app

client = TestClient(app)


def test_health_endpoint():
    """Test that /health returns status ok"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_consulta_endpoint(monkeypatch):
    """Test that /consulta returns results in expected format"""

    fake_data = [{"fechahora": "2025-09-06T23:09:57", "valor": 4050.25}]

    def fake_query(*args, **kwargs):
        return fake_data

    # ✅ corregido: referencia al query_dolar en app.py
    monkeypatch.setattr("app.query_dolar", fake_query)

    response = client.post("/consulta", json={
        "start": "2025-09-05T00:00:00-05:00",
        "end": "2025-09-08T18:00:00-05:00",
        "limit": 5
    })

    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "items" in data
    assert isinstance(data["items"], list)
