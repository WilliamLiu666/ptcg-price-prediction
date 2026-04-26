from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_ENV_LOADED = False


def _parse_env_line(line: str) -> tuple[str, str] | None:
    """
    Parse a KEY=VALUE line from a .env file.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()

    if value and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return key, value


def load_local_env() -> None:
    """
    Load variables from repository .env once (without overriding existing env).
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    repo_root = Path(__file__).resolve().parent.parent
    env_paths = (
        repo_root / ".env",
        repo_root / "docker" / ".env",
    )

    for env_path in env_paths:
        if not env_path.exists():
            continue

        for line in env_path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(line)
            if parsed is None:
                continue

            key, value = parsed
            if key and key not in os.environ:
                os.environ[key] = value

    _ENV_LOADED = True


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str = "public"


def parse_bool_env(name: str, default: bool = False) -> bool:
    """
    Parse a boolean environment variable.
    """
    load_local_env()
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def get_ebay_credentials() -> tuple[str, str]:
    """
    Read required eBay credentials from environment variables.
    """
    load_local_env()
    client_id = os.getenv("EBAY_CLIENT_ID", "").strip()
    client_secret = os.getenv("EBAY_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        raise ValueError(
            "Missing eBay credentials. Set EBAY_CLIENT_ID and EBAY_CLIENT_SECRET "
            "in your environment before running this job."
        )

    return client_id, client_secret


def get_postgres_settings() -> PostgresSettings:
    """
    Read PostgreSQL connection settings from environment variables.
    """
    load_local_env()

    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "").strip()
    dbname = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    schema = os.getenv("DB_SCHEMA", "public").strip() or "public"

    if not all((host, port, dbname, user, password)):
        raise ValueError(
            "Missing PostgreSQL settings. Set DB_HOST, DB_PORT, DB_NAME, "
            "DB_USER, and DB_PASSWORD before running the app."
        )

    return PostgresSettings(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        schema=schema,
    )
