from __future__ import annotations

import os


class Settings:
    """Runtime configuration.

    DATABASE_URL is resolved from the individual PG_* variables used by the
    docker-compose deployment, and can be overridden directly for tests.
    """

    database_url: str
    schema_lock_mode: str

    def __init__(self) -> None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            user = os.environ.get("POSTGRES_USER", "failover")
            password = os.environ.get("POSTGRES_PASSWORD", "failover")
            db = os.environ.get("POSTGRES_DB", "failover")
            host = os.environ.get("POSTGRES_HOST", "db")
            port = os.environ.get("POSTGRES_PORT", "5432")
            url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        self.database_url = url
        # Only PostgreSQL row locks are used by the service layer.
        self.schema_lock_mode = "postgresql" if url.startswith("postgresql") else "sqlite"


settings = Settings()
