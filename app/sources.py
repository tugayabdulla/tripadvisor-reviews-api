from enum import Enum
from dataclasses import dataclass
from pydantic import BaseModel
from .models import HotelDetail, RestaurantDetail, AttractionDetail, ProductDetail

class Source(str, Enum):
    restaurant = "restaurant"
    hotel = "hotel"
    attraction = "attraction"
    product = "product"

@dataclass(frozen=True)
class SourceSpec:
    source_table: str
    review_table: str
    has_country: bool   # source table has a `country` text column
    has_url: bool
    has_latlong: bool   # source table has `latitude` and `longitude` columns
    detail_model: type[BaseModel]

SPECS: dict[Source, SourceSpec] = {
    Source.restaurant: SourceSpec("locations", "reviews", True, False, True, RestaurantDetail),
    Source.hotel:      SourceSpec("hotels", "hotel_reviews", False, True, True, HotelDetail),
    Source.attraction: SourceSpec("attractions", "attraction_reviews", False, True, True, AttractionDetail),
    Source.product:    SourceSpec("attraction_products", "attraction_product_reviews", False, True, False, ProductDetail),
}

def spec(source: Source) -> SourceSpec:
    return SPECS[source]
