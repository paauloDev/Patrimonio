import logging
import os

import requests
from dotenv import load_dotenv


load_dotenv()


def obter_token(timeout_seconds: int = 15):
    """Obtém AccessToken da API systemsatx.

    Mantém assinatura compatível (chamada sem argumentos), mas adiciona:
    - timeout
    - validação de variáveis SATX_USERNAME/SATX_PASSWORD
    - tratamento de exceções de rede
    """
    username = os.getenv("SATX_USERNAME")
    password = os.getenv("SATX_PASSWORD")
    if not username or not password:
        logging.error("SATX_USERNAME/SATX_PASSWORD não configurados no ambiente (.env).")
        return None

    auth_url = "https://integration.systemsatx.com.br/Login"
    params = {"Username": username, "Password": password}

    try:
        auth_response = requests.post(auth_url, params=params, timeout=timeout_seconds)
    except requests.RequestException as e:
        logging.error(f"Erro de rede ao autenticar na systemsatx: {e}")
        return None

    if auth_response.status_code != 200:
        logging.error(
            "Erro na autenticação systemsatx: %s %s",
            auth_response.status_code,
            auth_response.text,
        )
        return None

    try:
        auth_data = auth_response.json()
    except ValueError:
        logging.error("Resposta de autenticação não é JSON válido.")
        return None

    token = auth_data.get("AccessToken")
    if not token:
        logging.error("AccessToken não encontrado na resposta de autenticação.")
        return None

    return token

if __name__ == '__main__':
    obter_token()