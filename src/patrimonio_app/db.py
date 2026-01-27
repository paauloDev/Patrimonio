from __future__ import annotations

import logging
from contextlib import contextmanager

from mysql.connector import pooling

from .settings import DbSettings


def create_connection_pool(db: DbSettings) -> pooling.MySQLConnectionPool:
    pool = pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=db.pool_size,
        host=db.host,
        user=db.user,
        password=db.password,
        database=db.database,
    )

    # Pequenas migrações/garantias idempotentes
    _ensure_indexes(pool)
    _ensure_patrimonios_empresa_column(pool)

    return pool


@contextmanager
def db_connection(pool: pooling.MySQLConnectionPool):
    conn = pool.get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _ensure_indexes(pool: pooling.MySQLConnectionPool) -> None:
    """Cria índice usado para autocomplete de colaboradores (idempotente)."""
    from mysql.connector.errors import Error

    with db_connection(pool) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(1)
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE table_schema=DATABASE()
                  AND table_name='colaboradores'
                  AND index_name='idx_colaborador'
                """
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute("CREATE INDEX idx_colaborador ON colaboradores (colaborador)")
                conn.commit()
                logging.info("Índice idx_colaborador criado.")
        except Error as e:
            logging.warning(f"Não foi possível garantir índice idx_colaborador: {e}")
        finally:
            try:
                cursor.close()
            except Exception:
                pass


def _ensure_patrimonios_empresa_column(pool: pooling.MySQLConnectionPool) -> None:
    """Garante que a tabela patrimonios tenha a coluna `empresa`.

    Mantém compatibilidade com o template atual (usa posições: patrimonio[10]).
    """
    with db_connection(pool) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(1)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = DATABASE()
                  AND table_name = 'patrimonios'
                  AND column_name = 'empresa'
                """
            )
            exists = cursor.fetchone()[0] > 0
            if not exists:
                cursor.execute(
                    "ALTER TABLE patrimonios ADD COLUMN empresa VARCHAR(20) NOT NULL DEFAULT 'TRACK'"
                )
                conn.commit()
                logging.info("Coluna patrimonios.empresa criada com default TRACK.")
        finally:
            try:
                cursor.close()
            except Exception:
                pass
