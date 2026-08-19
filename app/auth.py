import hmac
from fastapi import Header, HTTPException
from .config import settings

def require_rapidapi_secret(x_rapidapi_proxy_secret: str | None = Header(default=None)):
    expected = settings.rapidapi_proxy_secret
    if not expected or x_rapidapi_proxy_secret is None or not hmac.compare_digest(x_rapidapi_proxy_secret, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")
