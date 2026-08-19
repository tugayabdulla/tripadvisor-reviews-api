def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_health_db(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["db"] == "ok"

def test_health_db_down(client, monkeypatch):
    import app.routes.health as health_mod
    def boom(*a, **k):
        raise RuntimeError("db unavailable")
    monkeypatch.setattr(health_mod, "fetch_one", boom)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "db": "down"}
