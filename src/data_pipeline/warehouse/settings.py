"""Environment settings for optional Mongo compatibility utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class MongoSettings:
    """Connection settings for the optional Mongo raw-payload mirror."""

    mongo_uri: str
    mongo_database: str
    @classmethod
    def from_env(cls) -> "MongoSettings":
        """Load Mongo settings from process env with a `.env` fallback."""

        load_dotenv(override=False)

        mongo_uri = os.getenv("MONGO_URI", "").strip()
        if not mongo_uri:
            raise ValueError("MONGO_URI cannot be blank.")

        mongo_database = os.getenv("MONGO_DATABASE", "").strip()
        if not mongo_database:
            raise ValueError("MONGO_DATABASE cannot be blank.")

        return cls(
            mongo_uri=mongo_uri,
            mongo_database=mongo_database,
        )


@dataclass(frozen=True)
class PostgresBronzeSettings:
    """Connection settings for the Postgres Bronze warehouse."""

    warehouse_postgres_dsn: str

    @classmethod
    def from_env(cls) -> "PostgresBronzeSettings":
        """Load the Bronze Postgres DSN without requiring legacy ELT settings."""

        load_dotenv(override=False)
        warehouse_postgres_dsn = os.getenv("WAREHOUSE_POSTGRES_DSN", "").strip()
        if not warehouse_postgres_dsn:
            raise ValueError("WAREHOUSE_POSTGRES_DSN cannot be blank.")
        return cls(warehouse_postgres_dsn=warehouse_postgres_dsn)
