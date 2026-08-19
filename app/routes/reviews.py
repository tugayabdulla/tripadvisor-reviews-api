import time
from typing import Literal
from fastapi import APIRouter, Depends, Query, HTTPException, Response
from ..auth import require_rapidapi_secret
from ..sources import Source, spec
from ..db import fetch_all, fetch_one
from ..models import ReviewsResponse

router = APIRouter(dependencies=[Depends(require_rapidapi_secret)])

# PII (user_name, user_avatar, user_hometown, username, user_contributions, is_verified)
# is normalized into the `users` table and dropped from review tables.
# user_id is exposed only as a stable SHA-256 hash (lets clients group a user's
# reviews without revealing the real TripAdvisor id).
REVIEW_COLS = (
    "review_id::text AS review_id, location_id, rating, title, text, original_title, original_text, "
    "language, original_language, translation_type, published_date, "
    "encode(sha256(user_id::bytea), 'hex') AS user_id, "
    "trip_type, stay_date, additional_ratings, helpful_votes, "
    "has_mgmt_response, mgmt_response_text, mgmt_response_date, photo_urls, "
    "mgmt_response_author, mgmt_response_connection, "
    "publish_platform, review_tip"
)

REFRESH_COOLDOWN_S = 60


def _maybe_enqueue_refresh(source: str, location_id: int, last_refresh: int | None) -> None:
    """Live refresh: if this location hasn't been refreshed in the last 60s, queue it
    for the background worker. The response is still served from current DB data
    immediately; this enqueue is a ~1ms insert and never breaks the read path."""
    if last_refresh is not None and (time.time() - last_refresh) < REFRESH_COOLDOWN_S:
        return
    try:
        fetch_one(
            "INSERT INTO refresh_queue (source, location_id) VALUES (%s, %s) "
            "ON CONFLICT (source, location_id) DO NOTHING RETURNING location_id",
            (source, location_id))
    except Exception:
        pass


@router.get("/reviews/{location_id}", response_model=ReviewsResponse)
def get_reviews(
    location_id: int,
    response: Response,
    source: Source = Source.restaurant,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort: Literal["date", "rating"] = "date",
    language: str | None = None,
    min_rating: int | None = Query(None, ge=1, le=5),
    max_rating: int | None = Query(None, ge=1, le=5),
    trip_type: str | None = None,
):
    sp = spec(source)
    total = fetch_one(
        f"SELECT review_count, latest_review_date FROM {sp.source_table} WHERE location_id = %s",
        (location_id,))
    if total is None:
        raise HTTPException(status_code=404, detail="location not found",
                            headers={"X-RapidAPI-Billing": "Records=0"})
    _maybe_enqueue_refresh(source.value, location_id, total.get("latest_review_date"))

    where = ["location_id = %s"]
    params: list = [location_id]
    if language:
        where.append("language = %s"); params.append(language)
    if min_rating is not None:
        where.append("rating >= %s"); params.append(min_rating)
    if max_rating is not None:
        where.append("rating <= %s"); params.append(max_rating)
    if trip_type:
        where.append("trip_type = %s"); params.append(trip_type)
    if sort == "rating":
        order = "rating DESC, published_date DESC, review_id DESC"
    else:
        order = "published_date DESC, review_id DESC"
    params += [limit, offset]

    rows = fetch_all(
        f"SELECT {REVIEW_COLS} FROM {sp.review_table} "
        f"WHERE {' AND '.join(where)} ORDER BY {order} LIMIT %s OFFSET %s",
        tuple(params))
    # Usage-based billing: charge per record actually returned, not a flat 1/request,
    # so a 5-review response never costs the same as a 100-review one. RapidAPI reads
    # this header to increment the unified "Records" quota by exactly what we served.
    response.headers["X-RapidAPI-Billing"] = f"Records={len(rows)}"
    return {
        "location_id": location_id, "source": source.value,
        "total_count": total.get("review_count"),
        "limit": limit, "offset": offset, "reviews": rows,
    }
