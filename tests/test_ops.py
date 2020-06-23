"""Test operations."""
from fastapi.testclient import TestClient
from simple_kp.server import app

client = TestClient(app)


def test_ops():
    """Test getting operations."""
    response = client.get('/FOTR/ops')
    print(response.json())
