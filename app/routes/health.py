from fastapi import APIRouter
from ..db import fetch_one

router = APIRouter()

@router.get("/health")
def health():
    try:
        row = fetch_one("SELECT 1 AS ok")
        db_ok = bool(row and row["ok"] == 1)
    except Exception:
        db_ok = False
    return {"status": "ok", "db": "ok" if db_ok else "down"}
