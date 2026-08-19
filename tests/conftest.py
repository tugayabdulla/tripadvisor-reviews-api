import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.config import settings
from app.db import open_pool, fetch_one
from app.sources import Source, spec

@pytest.fixture(scope="session", autouse=True)
def _pool():
    open_pool()
    yield

@pytest.fixture(scope="session")
def client():
    return TestClient(app)

@pytest.fixture(scope="session")
def sample_ids():
    ids = {}
    for s in Source:
        sp = spec(s)
        row = fetch_one(
            f"SELECT location_id FROM {sp.source_table} WHERE review_count > 50 LIMIT 1")
        ids[s.value] = row["location_id"] if row else None
    return ids

@pytest.fixture
def auth():
    return {"X-RapidAPI-Proxy-Secret": settings.rapidapi_proxy_secret}
