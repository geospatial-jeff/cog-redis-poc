from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_uri: str


@lru_cache()
def get_settings() -> Settings:
    return Settings(_env_file=".env", _env_file_encoding="utf=8")
