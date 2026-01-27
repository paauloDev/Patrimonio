from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class DbSettings:
    host: str
    user: str
    password: str
    database: str
    pool_size: int = 10


@dataclass(frozen=True)
class DriveSettings:
    folder_id: str
    credentials_json: str | None


@dataclass(frozen=True)
class SchedulerSettings:
    enabled: bool


@dataclass(frozen=True)
class Settings:
    flask_secret_key: str
    session_lifetime: timedelta
    log_level: str

    db: DbSettings
    drive: DriveSettings

    colaboradores_cache_ttl: int
    scheduler: SchedulerSettings


def load_settings(*, base_dir: Path) -> Settings:
    """Carrega configurações do ambiente.

    - Lê `.env` na raiz do projeto (se existir)
    - Valida variáveis essenciais do MySQL

    Observação: Mantemos defaults compatíveis com o comportamento atual,
    mas com avisos/mensagens de erro mais claras.
    """
    load_dotenv(dotenv_path=str(base_dir / ".env"), override=False)

    secret_key = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY") or "your_secret_key"

    # Sessão: default 1h (comportamento atual)
    session_hours = _int_env("SESSION_HOURS", default=1)

    log_level = os.getenv("LOG_LEVEL", "INFO")

    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    missing = [k for k, v in {
        "DB_HOST": db_host,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DB_NAME": db_name,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            "Variáveis de ambiente do banco ausentes: "
            + ", ".join(missing)
            + ". Ajuste seu .env."
        )

    pool_size = _int_env("DB_POOL_SIZE", default=10)

    drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID") or os.getenv("FOLDER_ID") or "1hUe5xKP4krWcVVHd71kreLs81XqevsQY"
    drive_credentials_json = os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON")

    cache_ttl = _int_env("COLAB_CACHE_TTL_SECONDS", default=300)

    # Scheduler: por padrão, ligado quando rodando standalone.
    scheduler_enabled = _bool_env("SCHEDULER_ENABLED", default=True)

    return Settings(
        flask_secret_key=secret_key,
        session_lifetime=timedelta(hours=session_hours),
        log_level=log_level,
        db=DbSettings(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            pool_size=pool_size,
        ),
        drive=DriveSettings(folder_id=drive_folder_id, credentials_json=drive_credentials_json),
        colaboradores_cache_ttl=cache_ttl,
        scheduler=SchedulerSettings(enabled=scheduler_enabled),
    )


def _int_env(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"Variável {name} deve ser um inteiro; recebido: {raw!r}") from e


def _bool_env(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise RuntimeError(f"Variável {name} deve ser boolean; recebido: {raw!r}")
