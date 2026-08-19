from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    db_host: str = "127.0.0.1"
    db_port: int = 5432
    db_name: str = "tripadvisor"
    db_user: str = "tripadvisor"
    db_pass: str = ""
    rapidapi_proxy_secret: str = ""
    pool_min: int = 1
    pool_max: int = 8

settings = Settings()
