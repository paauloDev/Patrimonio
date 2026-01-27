"""Aplicação Patrimônio.

Este pacote isola a configuração, rotas e jobs do Flask, mantendo `app.py`
como ponto de entrada simples.
"""

from .factory import create_app

__all__ = ["create_app"]
