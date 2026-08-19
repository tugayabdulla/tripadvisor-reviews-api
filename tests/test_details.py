import pytest
from app.models import (
    HotelDetail, RestaurantDetail, AttractionDetail, ProductDetail,
)


def test_detail_models_discriminator_defaults():
    assert HotelDetail(location_id=1).source == "hotel"
    assert RestaurantDetail(location_id=1).source == "restaurant"
    assert AttractionDetail(location_id=1).source == "attraction"
    assert ProductDetail(location_id=1).source == "product"


def test_hotel_model_has_source_specific_fields():
    m = HotelDetail(location_id=1, property_amenities=["Pool"], star_rating="5.0")
    assert m.property_amenities == ["Pool"]
    assert m.star_rating == "5.0"


from app.sources import Source, spec
from app.db import fetch_one


def test_specs_have_detail_model():
    for s in Source:
        assert spec(s).detail_model is not None


def test_detail_columns_all_exist_in_db():
    # derived columns must be real columns -> LIMIT 0 select must not raise
    for s in Source:
        sp = spec(s)
        cols = ", ".join(c for c in sp.detail_model.model_fields if c != "source")
        fetch_one(f"SELECT {cols} FROM {sp.source_table} LIMIT 0")


def test_details_hotel(client, sample_ids, auth):
    lid = sample_ids["hotel"]
    r = client.get(f"/location/{lid}/details?source=hotel", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["source"] == "hotel"
    assert "property_amenities" in body


def test_details_restaurant(client, sample_ids, auth):
    lid = sample_ids["restaurant"]
    r = client.get(f"/location/{lid}/details?source=restaurant", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "restaurant"
    assert "cuisines" in body
    assert "detail_page_url" in body


def test_details_attraction(client, sample_ids, auth):
    lid = sample_ids["attraction"]
    r = client.get(f"/location/{lid}/details?source=attraction", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["source"] == "attraction"
    assert "categories" in body


def test_details_product(client, sample_ids, auth):
    lid = sample_ids["product"]
    if lid is None:
        pytest.skip("no product sample id with review_count > 50")
    r = client.get(f"/location/{lid}/details?source=product", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["source"] == "product"
    assert "categories" in body


def test_details_not_found(client, auth):
    r = client.get("/location/999999999/details?source=hotel", headers=auth)
    assert r.status_code == 404


def test_details_no_auth(client, sample_ids):
    lid = sample_ids["hotel"]
    r = client.get(f"/location/{lid}/details?source=hotel")
    assert r.status_code == 401


def test_details_not_enriched_returns_200(client, auth):
    row = fetch_one("SELECT location_id FROM attractions WHERE enriched_at IS NULL LIMIT 1")
    if row is None:
        pytest.skip("no un-enriched attraction available")
    lid = row["location_id"]
    r = client.get(f"/location/{lid}/details?source=attraction", headers=auth)
    assert r.status_code == 200
    assert r.json()["enriched_at"] is None
