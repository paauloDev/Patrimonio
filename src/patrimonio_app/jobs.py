from __future__ import annotations

import logging
import time


def log_execution_time(func):
    def wrapper():
        start_time = time.time()
        func()
        elapsed_time = time.time() - start_time
        logging.info(f"Job {func.__name__} executado em {elapsed_time:.2f} segundos.")

    return wrapper


def routeviolation_completo():
    from authtoken import obter_token
    from routeviolation import routeviolation, verificar_violações_por_velocidade

    token = obter_token()
    if token:
        routeviolation(token)
        verificar_violações_por_velocidade(token)
    else:
        logging.error("Não foi possível obter o token (routeviolation).")


def refresh_mv_job():
    from routeviolation import refresh_mv

    start_time = time.time()
    refresh_mv()
    elapsed_time = time.time() - start_time
    logging.info(f"Job refresh_mv executado em {elapsed_time:.2f} segundos.")


def tags_job():
    try:
        from datetime import datetime

        from authtoken import obter_token
        from tags import (
            consultar_api_escola,
            consultar_api_veiculo,
            corrigir_ordem_em_toda_tabela_aluno,
            criar_tabela_aluno,
            criar_tabela_escola,
            criar_tabela_veiculo,
            preencher_tabela_aluno,
        )

        data_ref = datetime.now()
        token = obter_token()
        if not token:
            logging.error("tags_job: falha ao obter token.")
            return

        criar_tabela_escola()
        criar_tabela_veiculo()
        criar_tabela_aluno()

        consultar_api_escola(data_ref, token=token)
        consultar_api_veiculo(data_ref, token=token)
        preencher_tabela_aluno(data_ref)
        corrigir_ordem_em_toda_tabela_aluno(data_ref.strftime("%Y-%m-%d"))

        logging.info("tags_job concluído com sucesso.")
    except Exception as e:
        logging.exception(f"Erro no tags_job: {e}")
