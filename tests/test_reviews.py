def test_reviews_basic(client, sample_ids, auth):
    lid = sample_ids["hotel"]
    r = client.get(f"/reviews/{lid}?source=hotel&limit=5", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["location_id"] == lid
    assert body["source"] == "hotel"
    assert len(body["reviews"]) == 5
    assert body["total_count"] >= 5
    assert body["reviews"][0]["review_id"]

def test_reviews_requires_auth(client, sample_ids):
    lid = sample_ids["hotel"]
    r = client.get(f"/reviews/{lid}?source=hotel")
    assert r.status_code == 401

def test_reviews_sort_rating(client, sample_ids, auth):
    lid = sample_ids["hotel"]
    r = client.get(f"/reviews/{lid}?source=hotel&sort=rating&limit=10", headers=auth)
    ratings = [x["rating"] for x in r.json()["reviews"]]
    assert ratings == sorted(ratings, reverse=True)

def test_reviews_min_rating(client, sample_ids, auth):
    lid = sample_ids["hotel"]
    r = client.get(f"/reviews/{lid}?source=hotel&min_rating=5&limit=10", headers=auth)
    assert all(x["rating"] == 5 for x in r.json()["reviews"])
