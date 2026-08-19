from contextlib import asynccontextmanager
from fastapi import FastAPI
from .db import open_pool, close_pool
from .routes import health
from .routes import reviews
from .routes import locations

@asynccontextmanager
async def lifespan(app: FastAPI):
    open_pool()
    yield
    close_pool()

def create_app() -> FastAPI:
    app = FastAPI(title="TripAdvisor Reviews API", version="0.1.0", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(reviews.router)
    app.include_router(locations.router)
    return app

app = create_app()
