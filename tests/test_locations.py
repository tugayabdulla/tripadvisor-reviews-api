from app.sources import Source, spec


def test_sources_cover_all():
    for s in Source:
        sp = spec(s)
        assert sp.source_table and sp.review_table


# Task 9: search

def test_location_search_by_name(client, auth):
    r = client.get("/location/search?query=Ritz&type=hotel&limit=5", headers=auth)
    assert r.status_code == 200
    rows = r.json()
    assert isinstance(rows, list) and len(rows) >= 1
    assert any("ritz" in (x["name"] or "").lower() for x in rows)


def test_location_search_no_latlong_for_product(client, auth):
    """attraction_products has no lat/long — endpoint must return nulls, not a SQL error."""
    r = client.get("/location/search?type=product&limit=5", headers=auth)
    assert r.status_code == 200
    rows = r.json()
    assert isinstance(rows, list)
    for row in rows:
        assert row["latitude"] is None
        assert row["longitude"] is None


def test_location_search_no_auth(client):
    r = client.get("/location/search?query=Ritz&type=hotel")
    assert r.status_code == 401


# Task 10: detail

def test_location_detail(client, sample_ids, auth):
    lid = sample_ids["hotel"]
    r = client.get(f"/location/{lid}?source=hotel", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["review_count"] is not None


def test_location_detail_restaurant(client, sample_ids, auth):
    lid = sample_ids["restaurant"]
    r = client.get(f"/location/{lid}?source=restaurant", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["url"] is None  # restaurants have no url


def test_location_detail_product_no_latlong(client, sample_ids, auth):
    lid = sample_ids["product"]
    if lid is None:
        return  # no products with review_count > 50 in test DB
    r = client.get(f"/location/{lid}?source=product", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["latitude"] is None
    assert body["longitude"] is None


def test_location_detail_not_found(client, auth):
    r = client.get("/location/999999999?source=hotel", headers=auth)
    assert r.status_code == 404


def test_location_detail_no_auth(client, sample_ids):
    lid = sample_ids["hotel"]
    r = client.get(f"/location/{lid}?source=hotel")
    assert r.status_code == 401
