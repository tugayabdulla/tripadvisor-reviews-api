# TripAdvisor Reviews API — RapidAPI listing copy

> Paste the **Overview** into the Hub Listing description. Paste each endpoint's
> blurb + example into that endpoint's Description / example-response field.

---

## OVERVIEW (Hub listing description)

**The largest queryable TripAdvisor reviews dataset on RapidAPI** — programmatic access to **500M+ reviews** across **4.6M+ locations** worldwide: restaurants, hotels, attractions, and tours/activities.

Search any location, pull full enriched details (ratings, sub-scores, amenities, price ranges, rankings, coordinates), and page through its reviews with filtering by rating, language, and trip type. Clean JSON, fast pagination, updated continuously.

**Why this API**
- 🌍 **Global coverage** — 4 source types (restaurant · hotel · attraction · product) in one consistent schema
- 📝 **Full review content** — text, ratings, trip type, stay date, management responses, helpful votes, photos
- 🏨 **Rich location details** — sub-ratings, amenities, price ranges, ranking position, lat/long, and more
- 🔄 **Kept fresh** — reviews and listings refreshed continuously
- 🔒 **Privacy-respecting** — reviewer identifiers are returned as stable anonymized hashes (group a reviewer's reviews without exposing personal data)

**Great for**: market & competitor research, travel apps, sentiment analysis, location intelligence, academic research, price monitoring.

---

## AUTHENTICATION
Standard RapidAPI auth — subscribe to a plan and send your `X-RapidAPI-Key`. No other setup. All responses are JSON.

`source` values everywhere: `restaurant` · `hotel` · `attraction` · `product`.

---

## ENDPOINTS

### GET `/location/search`
Find locations by name. Returns matching locations with quick summary stats.

| Param | Type | Notes |
|---|---|---|
| `query` | string | location name to search (required) |
| `country` | string | optional filter |
| `type` | string | optional source filter (restaurant/hotel/attraction/product) |
| `limit` | int | default 20, max 100 |
| `offset` | int | pagination |

**Example:** `/location/search?query=Amsterdam&limit=2`
```json
[
  {"location_id":1821986,"name":"Amsterdam","country":"Bulgaria","rating":3.8,"review_count":104,"latitude":42.148247,"longitude":24.748995}
]
```

### GET `/location/{location_id}`
Basic summary for one location.

| Param | Type | Notes |
|---|---|---|
| `location_id` | int | path |
| `source` | string | restaurant/hotel/attraction/product |

### GET `/location/{location_id}/details`
Full enriched detail for a location — all fields available for that source type (address, contact, ratings + sub-scores, price range, amenities, ranking, coordinates, etc.).

**Example:** `/location/20259744/details?source=restaurant`
```json
{
  "source":"restaurant","location_id":20259744,"name":"Eetcafe 't Juffertje",
  "country":"The Netherlands","latitude":52.89901,"longitude":4.732131,
  "address":"De Keizerskroon 1, 1787 BM Julianadorp The Netherlands",
  "telephone":"+31 223 864 625","website":"http://www.facebook.com/tJuffertje/"
}
```

### GET `/reviews/{location_id}`
Paged reviews for a location, newest first (or by rating).

| Param | Type | Notes |
|---|---|---|
| `location_id` | int | path |
| `source` | string | restaurant/hotel/attraction/product |
| `limit` | int | default 20, max 100 |
| `offset` | int | pagination |
| `sort` | string | `date` (default) or `rating` |
| `language` | string | filter by review language, e.g. `en` |
| `min_rating` / `max_rating` | int | 1–5 |
| `trip_type` | string | e.g. `COUPLES`, `FAMILY`, `BUSINESS`, `SOLO`, `FRIENDS` |

**Example:** `/reviews/20259744?source=restaurant&limit=1`
```json
{
  "location_id":20259744,"source":"restaurant","total_count":15,
  "limit":1,"offset":0,
  "reviews":[{
    "review_id":"1062114076","rating":5,"title":"Spitzenrestaurant",
    "text":"Top Essen, sehr nette Bedienung ...","language":"de",
    "published_date":1780005600,"trip_type":"COUPLES","stay_date":"2026-05-31",
    "helpful_votes":0,"has_mgmt_response":false,
    "user_id":"d3d531ec8b3e6c771740d65901f7846a..."
  }]
}
```
`published_date` is epoch seconds. `user_id` is an anonymized SHA-256 hash (consistent per reviewer).

### GET `/health`
Public uptime ping → `{"status":"ok","db":"ok"}`. No key required.

---

## NOTES
- **Pagination**: use `limit` + `offset`; `/reviews` returns `total_count` for the location.
- **Privacy**: no reviewer names/avatars/profiles are exposed — only a stable hashed `user_id`.
- **Rate limits** depend on your plan.
