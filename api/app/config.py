import os
from typing import Dict
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


def parse_shards_config(shards_env: str = "", default_url: str = "") -> Dict[str, str]:
    if not shards_env.strip():
        return {"default": default_url}
    shards = {}
    for item in shards_env.split(","):
        if "=" in item:
            k, v = item.split("=", 1)
            shards[k.strip()] = v.strip()
    return shards if shards else {"default": default_url}


class Settings(BaseSettings):
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/forge"
    )
    shards_config_env: str = os.getenv(
        "SHARDS_CONFIG", ""
    )
    max_queue_depth: int = int(os.getenv("MAX_QUEUE_DEPTH", "1000"))
    api_port: int = int(os.getenv("PORT", "8000"))

    model_config = ConfigDict(env_file=".env", extra="ignore")


settings = Settings()


def get_shards_map() -> Dict[str, str]:
    return parse_shards_config(settings.shards_config_env, settings.database_url)
