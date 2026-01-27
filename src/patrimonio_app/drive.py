from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from .settings import DriveSettings


@dataclass
class DriveClient:
    folder_id: str
    service: object | None  # googleapiclient.discovery.Resource

    @classmethod
    def from_settings(cls, settings: DriveSettings) -> "DriveClient":
        service = _build_drive_service(settings.credentials_json)
        if service:
            logging.info("Google Drive API inicializada com sucesso.")
        else:
            logging.warning("Credenciais do Google Drive ausentes/ inválidas; uploads serão ignorados.")
        return cls(folder_id=settings.folder_id, service=service)

    def create_folder_if_not_exists(self, folder_name: str, parent_id: str | None = None) -> str | None:
        if not self.service:
            return None
        parent_id = parent_id or self.folder_id

        try:
            query = (
                "mimeType='application/vnd.google-apps.folder' "
                f"and name='{folder_name}' and '{parent_id}' in parents"
            )
            response = self.service.files().list(q=query, spaces="drive").execute()
            if not response.get("files"):
                file_metadata = {
                    "name": folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                }
                folder = self.service.files().create(body=file_metadata, fields="id").execute()
                return folder.get("id")
            return response["files"][0].get("id")
        except Exception as e:
            msg = str(e)
            if "invalid_grant" in msg or "Invalid JWT" in msg or "Invalid JWT Signature" in msg:
                logging.error(
                    "Erro de autenticação no Google Drive (invalid_grant / JWT). "
                    "Isso normalmente indica credenciais incorretas/antigas (service account), chave privada malformada, "
                    "ou clock do servidor desalinhado. Atualize a chave do service account e o valor de GOOGLE_DRIVE_CREDENTIALS_JSON. "
                    f"Detalhe: {e}"
                )
                # Evita tentar novamente em loop durante a mesma execução
                self.service = None
            else:
                logging.error(f"Erro ao criar/listar pasta no Drive: {e}")
            return None

    def upload_files(self, *, folder_id: str, file_storages) -> None:
        """Faz upload de uma lista de FileStorage (Flask) para o Google Drive."""
        if not self.service or not folder_id:
            return

        # Usa um diretório temporário por requisição para evitar conflito de threads
        with tempfile.TemporaryDirectory(prefix="patrimonio_upload_") as tmp:
            tmp_path = Path(tmp)
            for fs in file_storages:
                if not fs:
                    continue

                filename = getattr(fs, "filename", None) or "arquivo"
                local_path = tmp_path / filename
                fs.save(str(local_path))

                file_metadata = {"name": filename, "parents": [folder_id]}
                media = MediaFileUpload(str(local_path), mimetype=getattr(fs, "content_type", None))
                try:
                    self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()
                except Exception as e:
                    msg = str(e)
                    if "invalid_grant" in msg or "Invalid JWT" in msg or "Invalid JWT Signature" in msg:
                        logging.error(
                            "Falha ao enviar arquivo ao Drive por erro de autenticação (invalid_grant / JWT). "
                            "Verifique GOOGLE_DRIVE_CREDENTIALS_JSON e gere uma nova chave do service account se necessário. "
                            f"Detalhe: {e}"
                        )
                        self.service = None
                        return
                    logging.error(f"Falha ao enviar arquivo ao Drive: {e}")


def _build_drive_service(credentials_env: str | None):
    if not credentials_env:
        return None

    credentials_info = None

    # 1) JSON literal
    try:
        credentials_info = json.loads(credentials_env)
    except json.JSONDecodeError:
        credentials_info = None

    # 2) Caminho para arquivo
    if credentials_info is None and os.path.isfile(credentials_env):
        try:
            with open(credentials_env, "r", encoding="utf-8") as f:
                credentials_info = json.load(f)
        except Exception:
            credentials_info = None

    # 3) Base64
    if credentials_info is None:
        try:
            decoded = base64.b64decode(credentials_env)
            credentials_info = json.loads(decoded)
        except Exception:
            credentials_info = None

    if not credentials_info:
        return None

    credentials_info = _normalize_service_account_info(credentials_info)

    try:
        scopes = ["https://www.googleapis.com/auth/drive.file"]
        credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=scopes)
        return build("drive", "v3", credentials=credentials)
    except Exception as e:
        logging.error(
            "Falha ao inicializar Google Drive API. "
            "Verifique GOOGLE_DRIVE_CREDENTIALS_JSON (JSON, base64 ou caminho) e se a chave do service account está correta. "
            f"Detalhe: {e}"
        )
        return None


def _normalize_service_account_info(credentials_info):
    """Normaliza campos do JSON de service account vindos de variáveis de ambiente.

    Casos comuns em .env:
    - private_key com '\\n' literais (precisa virar '\n')
    - espaços/aspas extras

    Não valida credenciais (isso é feito pelo Google). Apenas evita erros por formatação.
    """
    if not isinstance(credentials_info, dict):
        return credentials_info

    pk = credentials_info.get("private_key")
    if isinstance(pk, str):
        # Se veio como string com \n literais, converte para newlines reais.
        # Se já estiver normalizado, o replace não altera.
        pk_norm = pk.replace("\\n", "\n").strip()
        credentials_info["private_key"] = pk_norm

        # Heurística de diagnóstico (não imprime segredo)
        if "BEGIN PRIVATE KEY" not in pk_norm or "END PRIVATE KEY" not in pk_norm:
            logging.warning(
                "GOOGLE_DRIVE_CREDENTIALS_JSON parece conter 'private_key' malformada (sem BEGIN/END). "
                "Se você colou no .env, prefira apontar para um arquivo .json de credenciais, "
                "ou use base64 do JSON completo."
            )

    # Remove espaços accidentais
    for k in ("client_email", "private_key_id", "project_id", "client_id"):
        v = credentials_info.get(k)
        if isinstance(v, str):
            credentials_info[k] = v.strip()

    return credentials_info
