import os
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/forge"
    )
    max_queue_depth: int = int(os.getenv("MAX_QUEUE_DEPTH", "1000"))
    api_port: int = int(os.getenv("PORT", "8000"))

    model_config = ConfigDict(env_file=".env", extra="ignore")


settings = Settings()
