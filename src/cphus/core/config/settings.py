from functools import lru_cache
from pathlib import Path

from pydantic import ConfigDict, SecretStr
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).parent.parent.parent.parent.parent


class Settings(BaseSettings):
    boligsiden_url: str
    boligportal_url: str
    firecrawl_api_key: SecretStr
    discord_bot_token: SecretStr
    discord_channel_id: int

    model_config = ConfigDict(env_file=BASE_DIR / ".env", encoding="utf-8")


@lru_cache
def get_settings():
    return Settings()
