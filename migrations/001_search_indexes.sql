-- Run once on the VPS as the tripadvisor user (pg_trgm is a trusted extension).
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_locations_name_trgm ON locations USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_hotels_name_trgm ON hotels USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_attractions_name_trgm ON attractions USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_attraction_products_name_trgm ON attraction_products USING gin (name gin_trgm_ops);
