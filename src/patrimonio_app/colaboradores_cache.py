from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

from mysql.connector import pooling

from .db import db_connection


@dataclass
class ColaboradoresCache:
    pool: pooling.MySQLConnectionPool
    ttl_seconds: int = 300

    _cache: list[str] = None  # type: ignore[assignment]
    _last_load: float = 0.0
    _lock: threading.Lock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self._cache = []
        self._lock = threading.Lock()

    def get(self, *, prefix: str, limit: int = 20) -> list[str]:
        prefix = (prefix or "").strip()
        if len(prefix) < 2:
            return []

        self.refresh_if_needed()
        p = prefix.lower()
        with self._lock:
            return [n for n in self._cache if n.lower().startswith(p)][:limit]

    def refresh_if_needed(self) -> None:
        now = time.time()
        with self._lock:
            is_stale = (now - self._last_load) > self.ttl_seconds or not self._cache
        if is_stale:
            self.refresh(force=True)

    def refresh(self, *, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            if not force and (now - self._last_load) <= self.ttl_seconds and self._cache:
                return

        with db_connection(self.pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT colaborador FROM colaboradores ORDER BY colaborador")
                rows = cursor.fetchall()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        with self._lock:
            self._cache = [name for (name,) in rows]
            self._last_load = now

        logging.info(f"Cache de colaboradores carregado: {len(self._cache)} nomes.")

    def refresh_async(self, *, force: bool = True) -> None:
        threading.Thread(target=self.refresh, kwargs={"force": force}, daemon=True).start()
