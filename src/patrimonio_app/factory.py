from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask

from .settings import load_settings
from .db import create_connection_pool
from .drive import DriveClient
from .colaboradores_cache import ColaboradoresCache
from .routes import register_routes
from .scheduler import configure_scheduler


def create_app() -> Flask:
    """Cria a aplicação Flask.

    Mantém compatibilidade com templates/estáticos que estão na raiz do projeto.
    """
    base_dir = Path(__file__).resolve().parents[2]
    settings = load_settings(base_dir=base_dir)

    app = Flask(
        __name__,
        template_folder=str(base_dir / "templates"),
        static_folder=str(base_dir / "static"),
        static_url_path="/static",
    )

    app.secret_key = settings.flask_secret_key
    app.permanent_session_lifetime = settings.session_lifetime

    # Logging básico (evita duplicar handlers em reloaders)
    _configure_logging(settings.log_level)

    pool = create_connection_pool(settings.db)
    drive = DriveClient.from_settings(settings.drive)
    colab_cache = ColaboradoresCache(pool=pool, ttl_seconds=settings.colaboradores_cache_ttl)

    register_routes(app=app, pool=pool, drive=drive, colaboradores_cache=colab_cache)

    if settings.scheduler.enabled:
        configure_scheduler(settings=settings.scheduler, pool=pool)

    # Carrega cache em segundo plano na inicialização
    try:
        colab_cache.refresh_async(force=True)
    except Exception:
        logging.exception("Falha ao disparar refresh_async do cache de colaboradores.")

    return app


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
