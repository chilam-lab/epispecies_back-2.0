# tests/test_inegi_api.py
import pytest
import httpx
from fastapi.testclient import TestClient
from httpx import AsyncClient
from app.main import app
from app.routes.inegi import fetch_inegi_variables

pytestmark = pytest.mark.asyncio

client = TestClient(app)

@pytest.fixture
def mock_inegi_response(monkeypatch):
    """Mock the INEGI API response"""
    async def mock_get(*args, **kwargs):
        class MockResponse:
            async def json(self):
                return [{"id": 1, "name": "test_variable"}]
            
            def raise_for_status(self):
                pass
            
            status_code = 200
        
        return MockResponse()
    
    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

@pytest.fixture
def mock_inegi_error(monkeypatch):
    """Mock an INEGI API error"""
    async def mock_get(*args, **kwargs):
        class MockResponse:
            async def json(self):
                return {"error": "API error"}
            
            def raise_for_status(self):
                raise httpx.HTTPStatusError(
                    "API error",
                    request=None,
                    response=type('Response', (), {'status_code': 500, 'text': 'Server Error'})()
                )
        
        return MockResponse()
    
    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

# Test cases

async def test_get_inegi_variables_endpoint_success():
    """Test the API endpoint with successful response"""
    response = client.get("/database/inegi/variables")
    assert response.status_code == 200
    assert isinstance(response.json(), list)