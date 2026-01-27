"""Entrypoint da aplicação.

Mantém compatibilidade com `gunicorn app:app` e com execução direta:

    python app.py

O código da aplicação foi refatorado para `src/patrimonio_app/`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


from patrimonio_app import create_app  # noqa: E402


app = create_app()


if __name__ == "__main__":
    debug = (os.getenv("FLASK_DEBUG") or "1").strip().lower() in {"1", "true", "yes", "y", "on"}
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))

    # use_reloader=False evita duplicar scheduler/jobs
    app.run(debug=debug, use_reloader=False, host=host, port=port)