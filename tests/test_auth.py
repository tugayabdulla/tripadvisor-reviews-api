def test_auth_rejects_missing_secret():
    from app.auth import require_rapidapi_secret
    import pytest
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        require_rapidapi_secret(None)
    assert e.value.status_code == 401
