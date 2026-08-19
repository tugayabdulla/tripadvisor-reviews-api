from typing import Any, Literal, Union, Annotated
from pydantic import BaseModel, Field

class Review(BaseModel):
    review_id: str
    location_id: int
    rating: int | None = None
    title: str | None = None
    text: str | None = None
    original_title: str | None = None
    original_text: str | None = None
    language: str | None = None
    original_language: str | None = None
    translation_type: str | None = None
    published_date: int | None = None  # epoch seconds
    user_id: str | None = None  # SHA-256 hash of the TripAdvisor user id (not the raw id)
    trip_type: str | None = None
    stay_date: str | None = None
    additional_ratings: Any | None = None  # jsonb
    helpful_votes: int | None = None
    has_mgmt_response: bool | None = None
    mgmt_response_text: str | None = None
    mgmt_response_date: str | None = None
    photo_urls: list[str] | None = None
    mgmt_response_author: str | None = None
    mgmt_response_connection: str | None = None
    publish_platform: str | None = None
    review_tip: str | None = None

class ReviewsResponse(BaseModel):
    location_id: int
    source: str
    total_count: int | None
    limit: int
    offset: int
    reviews: list[Review]

class LocationSummary(BaseModel):
    location_id: int
    name: str | None = None
    country: str | None = None
    rating: float | None = None
    review_count: int | None = None
    latitude: float | None = None
    longitude: float | None = None

class LocationDetail(LocationSummary):
    url: str | None = None
    latest_review_date: int | None = None


class HotelDetail(BaseModel):
    source: Literal["hotel"] = "hotel"
    location_id: int
    name: str | None = None
    url: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    address: str | None = None
    street1: str | None = None
    street2: str | None = None
    postal_code: str | None = None
    phone: str | None = None
    telephone: str | None = None
    official_url: str | None = None
    description: str | None = None
    accommodation_type: str | None = None
    star_rating: str | None = None
    num_rooms: int | None = None
    walk_score: int | None = None
    price_range_min: float | None = None
    price_range_max: float | None = None
    price_range_min_usd: float | None = None
    rating: float | None = None
    review_count: int | None = None
    rating_1_star: int | None = None
    rating_2_star: int | None = None
    rating_3_star: int | None = None
    rating_4_star: int | None = None
    rating_5_star: int | None = None
    sub_cleanliness: float | None = None
    sub_location: float | None = None
    sub_rooms: float | None = None
    sub_service: float | None = None
    sub_sleep_quality: float | None = None
    sub_value: float | None = None
    ranking_position: int | None = None
    ranking_total: int | None = None
    style_rankings: Any | None = None
    property_amenities: list[str] | None = None
    room_features: list[str] | None = None
    room_types: list[str] | None = None
    languages_spoken: list[str] | None = None
    travelers_choice: bool | None = None
    award_type: str | None = None
    award_year: int | None = None
    is_closed: bool | None = None
    nearby_attraction_count: int | None = None
    nearby_restaurant_count: int | None = None
    nearby_hotel_count: int | None = None
    tip_count: int | None = None
    topic_count: int | None = None
    photo_count: int | None = None
    ai_review_summary: str | None = None
    aliases: list[str] | None = None
    reviews_by_language: Any | None = None
    thumbnail: str | None = None
    latest_review_date: int | None = None
    enriched_at: int | None = None


class RestaurantDetail(BaseModel):
    source: Literal["restaurant"] = "restaurant"
    location_id: int
    name: str | None = None
    country: str | None = None
    detail_page_url: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    address: str | None = None
    street2: str | None = None
    postal_code: str | None = None
    telephone: str | None = None
    email: str | None = None
    website: str | None = None
    menu_url: str | None = None
    description: str | None = None
    cuisines: list[str] | None = None
    establishment_types: list[str] | None = None
    price_range: str | None = None
    price_min_usd: float | None = None
    price_max_usd: float | None = None
    meal_types: list[str] | None = None
    dining_options: list[str] | None = None
    diets: list[str] | None = None
    top_tags: list[str] | None = None
    top_reasons: list[str] | None = None
    neighborhoods: list[str] | None = None
    rating: float | None = None
    review_count: int | None = None
    rating_1_star: int | None = None
    rating_2_star: int | None = None
    rating_3_star: int | None = None
    rating_4_star: int | None = None
    rating_5_star: int | None = None
    sub_atmosphere: float | None = None
    sub_food: float | None = None
    sub_service: float | None = None
    sub_value: float | None = None
    ranking_position: int | None = None
    ranking_total: int | None = None
    opening_hours: Any | None = None
    open_status: str | None = None
    is_closed: bool | None = None
    is_permanently_closed: bool | None = None
    has_menu_provider: bool | None = None
    award_type: str | None = None
    award_year: int | None = None
    has_verified_owner: bool | None = None
    is_premium: bool | None = None
    ai_review_summary: str | None = None
    thumbnail: str | None = None
    reviews_by_language: Any | None = None
    latest_review_date: int | None = None
    enriched_at: int | None = None


class AttractionDetail(BaseModel):
    source: Literal["attraction"] = "attraction"
    location_id: int
    name: str | None = None
    url: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    address: str | None = None
    street1: str | None = None
    street2: str | None = None
    postal_code: str | None = None
    phone: str | None = None
    website: str | None = None
    email: str | None = None
    description: str | None = None
    categories: str | None = None
    hours: str | None = None
    open_status: str | None = None
    merchandising_text: str | None = None
    rating: float | None = None
    review_count: int | None = None
    rating_1_star: int | None = None
    rating_2_star: int | None = None
    rating_3_star: int | None = None
    rating_4_star: int | None = None
    rating_5_star: int | None = None
    ranking_position: int | None = None
    ranking_total: int | None = None
    photo_count: int | None = None
    thumbnail: str | None = None
    latest_review_date: int | None = None
    enriched_at: int | None = None


class ProductDetail(BaseModel):
    source: Literal["product"] = "product"
    location_id: int
    name: str | None = None
    url: str | None = None
    description: str | None = None
    categories: str | None = None
    product_code: str | None = None
    duration: str | None = None
    operator: str | None = None
    languages: str | None = None
    price: str | None = None
    price_numeric: float | None = None
    currency: str | None = None
    strikethrough_price: str | None = None
    price_suffix: str | None = None
    rating: float | None = None
    review_count: int | None = None
    rating_1_star: int | None = None
    rating_2_star: int | None = None
    rating_3_star: int | None = None
    rating_4_star: int | None = None
    rating_5_star: int | None = None
    ranking_position: int | None = None
    ranking_total: int | None = None
    photo_count: int | None = None
    thumbnail: str | None = None
    latest_review_date: int | None = None
    enriched_at: int | None = None


LocationDetailUnion = Annotated[
    Union[HotelDetail, RestaurantDetail, AttractionDetail, ProductDetail],
    Field(discriminator="source"),
]
