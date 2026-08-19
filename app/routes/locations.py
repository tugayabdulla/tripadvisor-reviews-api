from fastapi import APIRouter, Depends, Query, HTTPException, Response
from ..auth import require_rapidapi_secret
from ..sources import Source, spec
from ..db import fetch_all, fetch_one
from ..models import LocationSummary, LocationDetail, LocationDetailUnion

router = APIRouter(dependencies=[Depends(require_rapidapi_secret)])


@router.get("/location/search", response_model=list[LocationSummary])
def search(
    response: Response,
    query: str | None = None,
    country: str | None = None,
    type: Source = Source.restaurant,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
):
    sp = spec(type)
    where, params = [], []
    if query:
        where.append("name ILIKE %s")
        params.append(f"%{query}%")
    if country and sp.has_country:
        where.append("country = %s")
        params.append(country)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    country_col = "country" if sp.has_country else "NULL::text AS country"
    latlong_cols = (
        "latitude, longitude"
        if sp.has_latlong
        else "NULL::double precision AS latitude, NULL::double precision AS longitude"
    )
    if query:
        order = "similarity(name, %s) DESC"
        order_params = [query]
    else:
        order = "review_count DESC NULLS LAST"
        order_params = []
    sql = (
        f"SELECT location_id, name, {country_col}, rating, review_count, {latlong_cols} "
        f"FROM {sp.source_table} {clause} ORDER BY {order} LIMIT %s OFFSET %s"
    )
    rows = fetch_all(sql, tuple(params + order_params + [limit, offset]))
    response.headers["X-RapidAPI-Billing"] = f"Records={len(rows)}"
    return rows


@router.get("/location/{location_id}", response_model=LocationDetail)
def location_detail(location_id: int, response: Response, source: Source = Source.restaurant):
    sp = spec(source)
    country_col = "country" if sp.has_country else "NULL::text AS country"
    url_col = "url" if sp.has_url else "NULL::text AS url"
    latlong_cols = (
        "latitude, longitude"
        if sp.has_latlong
        else "NULL::double precision AS latitude, NULL::double precision AS longitude"
    )
    row = fetch_one(
        f"SELECT location_id, name, {country_col}, rating, review_count, {latlong_cols}, "
        f"{url_col}, latest_review_date "
        f"FROM {sp.source_table} WHERE location_id = %s",
        (location_id,),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="location not found",
                            headers={"X-RapidAPI-Billing": "Records=0"})
    response.headers["X-RapidAPI-Billing"] = "Records=1"
    return row


@router.get("/location/{location_id}/details", response_model=LocationDetailUnion)
def location_details(location_id: int, response: Response, source: Source = Source.restaurant):
    sp = spec(source)
    # cols are developer-defined model field names (not user input); location_id is parameterized -> no injection
    cols = ", ".join(c for c in sp.detail_model.model_fields if c != "source")
    row = fetch_one(
        f"SELECT {cols} FROM {sp.source_table} WHERE location_id = %s",
        (location_id,),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="location not found",
                            headers={"X-RapidAPI-Billing": "Records=0"})
    response.headers["X-RapidAPI-Billing"] = "Records=1"
    return sp.detail_model(**row)
