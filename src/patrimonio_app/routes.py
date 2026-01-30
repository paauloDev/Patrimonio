from __future__ import annotations

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from mysql.connector import pooling

from .colaboradores_cache import ColaboradoresCache
from .db import db_connection
from .drive import DriveClient


EMPRESAS_PATRIMONIO_VALIDAS = {"TRACK", "RAPTOR"}


def register_routes(
    *,
    app: Flask,
    pool: pooling.MySQLConnectionPool,
    drive: DriveClient,
    colaboradores_cache: ColaboradoresCache,
) -> None:
    @app.before_request
    def _before_request():
        session.permanent = True
        if "user" not in session and request.endpoint not in {"login", "static"}:
            return redirect(url_for("login"))

    @app.get("/autocomplete_colaboradores")
    def autocomplete_colaboradores():
        term = (request.args.get("term") or "").strip()
        return jsonify(colaboradores_cache.get(prefix=term))

    @app.get("/autocomplete_nomes")
    def autocomplete_nomes():
        term = request.args.get("term") or ""
        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT nome FROM patrimonios WHERE nome LIKE %s", (f"%{term}%",))
                results = cursor.fetchall()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        return jsonify([row[0] for row in results])

    @app.get("/autocomplete_etiquetas")
    def autocomplete_etiquetas():
        term = request.args.get("term") or ""
        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT etiqueta FROM patrimonios WHERE etiqueta LIKE %s",
                    (f"%{term}%",),
                )
                results = cursor.fetchall()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        return jsonify([row[0] for row in results])

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/cadastrar_patrimonio")
    def cadastrar_patrimonio():
        return render_template("cadastro.html")

    @app.get("/listar_patrimonios")
    def listar_patrimonios():
        somente_estoque = (request.args.get("estoque") or "").strip().lower() in {"1", "true", "sim", "yes"}
        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                if somente_estoque:
                    cursor.execute(
                        """
                        SELECT *
                        FROM patrimonios
                        WHERE (colaborador IS NULL OR TRIM(colaborador) = '')
                          AND (colaborador2 IS NULL OR TRIM(colaborador2) = '')
                        """
                    )
                else:
                    cursor.execute("SELECT * FROM patrimonios")
                patrimonios = cursor.fetchall()

                # Totais gerais
                cursor.execute("SELECT SUM(valor) FROM patrimonios")
                valor_total_geral = cursor.fetchone()[0] or 0

                # Totais de estoque
                cursor.execute(
                    """
                    SELECT COUNT(1), COALESCE(SUM(valor), 0)
                    FROM patrimonios
                    WHERE (colaborador IS NULL OR TRIM(colaborador) = '')
                      AND (colaborador2 IS NULL OR TRIM(colaborador2) = '')
                    """
                )
                total_estoque, valor_total_estoque = cursor.fetchone()

                # Totais alocados (qualquer colaborador preenchido)
                cursor.execute(
                    """
                    SELECT COUNT(1), COALESCE(SUM(valor), 0)
                    FROM patrimonios
                    WHERE NOT ((colaborador IS NULL OR TRIM(colaborador) = '')
                           AND (colaborador2 IS NULL OR TRIM(colaborador2) = ''))
                    """
                )
                total_alocados, valor_total_alocados = cursor.fetchone()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        total_patrimonios = len(patrimonios)
        # Para a página de estoque, o “valor_total” mostrado deve ser o do recorte.
        valor_total = valor_total_estoque if somente_estoque else valor_total_geral
        return render_template(
            "listar.html",
            patrimonios=patrimonios,
            valor_total=valor_total,
            total_patrimonios=total_patrimonios,
            somente_estoque=somente_estoque,
            total_estoque=total_estoque,
            total_alocados=total_alocados,
            valor_total_estoque=valor_total_estoque,
            valor_total_alocados=valor_total_alocados,
        )

    @app.get("/estoque")
    def estoque():
        # Mantém uma URL amigável para estoque, reutilizando a listagem.
        return redirect(url_for("listar_patrimonios", estoque=1))

    @app.get("/colaboradores")
    def colaboradores_page():
        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT cpf, colaborador FROM colaboradores ORDER BY colaborador")
                colaboradores = cursor.fetchall()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        return render_template("colaboradores.html", colaboradores=colaboradores)

    @app.post("/cadastrar_colaborador")
    def cadastrar_colaborador():
        cpf = (request.form.get("cpf") or "").strip()
        nome = (request.form.get("colaborador") or "").strip()
        if not cpf or not nome:
            return "Erro: 'cpf' e 'colaborador' são obrigatórios.", 400

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(1) FROM colaboradores WHERE cpf = %s", (cpf,))
                if cursor.fetchone()[0] > 0:
                    return f"Erro: CPF {cpf} já cadastrado.", 400

                cursor.execute(
                    "INSERT INTO colaboradores (cpf, colaborador) VALUES (%s, %s)",
                    (cpf, nome),
                )
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        colaboradores_cache.refresh_async(force=True)
        return redirect(url_for("colaboradores_page"))

    @app.post("/editar_colaborador")
    def editar_colaborador():
        cpf = (request.form.get("cpf") or "").strip()
        nome = (request.form.get("colaborador") or "").strip()
        if not cpf or not nome:
            return "Erro: 'cpf' e 'colaborador' são obrigatórios.", 400

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM colaboradores WHERE cpf = %s", (cpf,))
                if not cursor.fetchone():
                    return f"Erro: CPF {cpf} não encontrado.", 404

                cursor.execute("UPDATE colaboradores SET colaborador = %s WHERE cpf = %s", (nome, cpf))
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        colaboradores_cache.refresh_async(force=True)
        return redirect(url_for("colaboradores_page"))

    @app.post("/excluir_colaborador")
    def excluir_colaborador():
        cpf = (request.form.get("cpf") or "").strip()
        if not cpf:
            return "Erro: CPF é obrigatório.", 400

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM colaboradores WHERE cpf = %s", (cpf,))
                if not cursor.fetchone():
                    return f"Erro: CPF {cpf} não encontrado.", 404

                cursor.execute("DELETE FROM colaboradores WHERE cpf = %s", (cpf,))
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        colaboradores_cache.refresh_async(force=True)
        return redirect(url_for("colaboradores_page"))

    @app.post("/cadastrar")
    def cadastrar():
        nome = request.form["nome"]
        empresa = (request.form.get("empresa") or "TRACK").strip().upper()
        if empresa not in EMPRESAS_PATRIMONIO_VALIDAS:
            return "Erro: empresa inválida. Use TRACK ou RAPTOR.", 400

        colaborador = request.form.get("colaborador", "")
        colaborador2 = request.form.get("colaborador2", "")
        especificacao = request.form.get("especificacao", "")
        estado = request.form.get("estado", "")
        valor = request.form.get("valor", "")
        observacao = request.form.get("observacao", "")
        anexos = request.files.getlist("anexos")
        etiquetas = (request.form.get("etiqueta") or "").split(",")

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                for etiqueta in etiquetas:
                    etiqueta = etiqueta.strip()
                    if not etiqueta:
                        continue

                    cursor.execute("SELECT COUNT(*) FROM patrimonios WHERE etiqueta = %s", (etiqueta,))
                    if cursor.fetchone()[0] > 0:
                        return f"Erro: Etiqueta {etiqueta} já cadastrada!", 400

                    etiqueta_folder_id = drive.create_folder_if_not_exists(etiqueta, drive.folder_id)
                    folder_url = (
                        f"https://drive.google.com/drive/folders/{etiqueta_folder_id}" if etiqueta_folder_id else ""
                    )

                    # Upload (se habilitado)
                    if drive.service and etiqueta_folder_id:
                        drive.upload_files(folder_id=etiqueta_folder_id, file_storages=anexos)

                    cursor.execute(
                        """
                        INSERT INTO patrimonios (nome, colaborador, colaborador2, etiqueta, especificacao, estado, valor, observacao, url, empresa)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            nome,
                            colaborador,
                            colaborador2,
                            etiqueta,
                            especificacao,
                            estado,
                            valor,
                            observacao,
                            folder_url,
                            empresa,
                        ),
                    )
                    conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        return redirect(url_for("index"))

    @app.post("/editar_patrimonio")
    def editar_patrimonio():
        patrimonio_id = request.form["id"]
        nome = request.form["nome"]
        empresa = (request.form.get("empresa") or "TRACK").strip().upper()
        if empresa not in EMPRESAS_PATRIMONIO_VALIDAS:
            return "Erro: empresa inválida. Use TRACK ou RAPTOR.", 400

        colaborador = request.form.get("colaborador", "")
        colaborador2 = request.form.get("colaborador2", "")
        etiqueta = request.form.get("etiqueta", "")
        especificacao = request.form.get("especificacao", "")
        estado = request.form.get("estado", "")
        valor = request.form.get("valor", "")
        observacao = request.form.get("observacao", "")

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE patrimonios
                    SET nome = %s, empresa = %s, colaborador = %s, colaborador2 = %s, etiqueta = %s,
                        especificacao = %s, estado = %s, valor = %s, observacao = %s
                    WHERE id = %s
                    """,
                    (
                        nome,
                        empresa,
                        colaborador,
                        colaborador2,
                        etiqueta,
                        especificacao,
                        estado,
                        valor,
                        observacao,
                        patrimonio_id,
                    ),
                )
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        return "OK", 200

    @app.post("/devolver_estoque")
    def devolver_estoque():
        """Remove colaboradores do patrimônio (volta para 'estoque') sem excluir o registro."""
        patrimonio_id = (request.form.get("id") or "").strip()
        if not patrimonio_id:
            return "Erro: id é obrigatório.", 400

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT colaborador, colaborador2 FROM patrimonios WHERE id = %s", (patrimonio_id,))
                row = cursor.fetchone()
                if not row:
                    return f"Erro: patrimônio id {patrimonio_id} não encontrado.", 404

                # Mesmo que já esteja em estoque, a operação é idempotente.
                cursor.execute(
                    "UPDATE patrimonios SET colaborador = %s, colaborador2 = %s WHERE id = %s",
                    ("", "", patrimonio_id),
                )
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        return redirect(request.referrer or url_for("listar_patrimonios"))

    @app.post("/excluir_patrimonio")
    def excluir_patrimonio():
        patrimonio_id = (request.form.get("id") or "").strip()
        if not patrimonio_id:
            return "Erro: id é obrigatório.", 400

        with db_connection(pool) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM patrimonios WHERE id = %s", (patrimonio_id,))
                if not cursor.fetchone():
                    return f"Erro: patrimônio id {patrimonio_id} não encontrado.", 404

                cursor.execute("DELETE FROM patrimonios WHERE id = %s", (patrimonio_id,))
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        return redirect(url_for("listar_patrimonios"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            username = request.form["username"]
            password = request.form["password"]

            with db_connection(pool) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "SELECT * FROM usuarios WHERE username = %s AND password = %s",
                        (username, password),
                    )
                    user = cursor.fetchone()
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass

            if user:
                session["user"] = username
                return redirect(url_for("index"))
            error = "Erro: Credenciais inválidas!"

        return render_template("login.html", error=error)
