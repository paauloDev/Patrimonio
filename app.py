from flask import Flask, render_template, request, redirect, url_for, jsonify, session
import mysql.connector
from mysql.connector import pooling
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import shutil  # Adicionado para manipulação de arquivos e diretórios
from dotenv import load_dotenv  # Adicionado para carregar variáveis de ambiente
import json  # Adicionado para manipulação de JSON
from datetime import timedelta  # Adicionado para definir a duração da sessão
from apscheduler.schedulers.background import BackgroundScheduler  # Adicionado para agendamento de tarefas
import logging  # Adicionado para logs do APScheduler
import time  # Adicionado para medir o tempo de execução

# Novos imports da main.py
from grid import processar_grid
from ultima_execucao import atualizar_ultima_execucao
from routeviolation import routeviolation, verificar_violações_por_velocidade, refresh_mv

load_dotenv()  # Carregar variáveis de ambiente do arquivo .env

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Adicione uma chave secreta para a sessão
app.permanent_session_lifetime = timedelta(hours=1)  # Definir duração da sessão para 1 hora

# Configure suas credenciais de forma segura
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Configurar pool de conexões
db_config = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
}
connection_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=10, **db_config)

def get_db_connection():
    return connection_pool.get_connection()  # Obter conexão do pool

# Carregar credenciais do Google Drive a partir de uma variável de ambiente
GOOGLE_DRIVE_CREDENTIALS_JSON = os.getenv('GOOGLE_DRIVE_CREDENTIALS_JSON')
credentials_info = json.loads(GOOGLE_DRIVE_CREDENTIALS_JSON)
SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

# ID da pasta do Google Drive
FOLDER_ID = '1hUe5xKP4krWcVVHd71kreLs81XqevsQY'

# Criar pasta temporária no início do script
tmp_dir = 'c:/Users/Paulo/Desktop/Python/Patrimonio/tmp'
if not os.path.exists(tmp_dir):
    os.makedirs(tmp_dir)

def create_folder_if_not_exists(folder_name, parent_id):
    query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_id}' in parents"
    response = drive_service.files().list(q=query, spaces='drive').execute()
    if not response['files']:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')
    else:
        return response['files'][0]['id']

def create_index():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(1) IndexIsThere
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_schema=DATABASE() AND table_name='colaboradores' AND index_name='idx_colaborador'
    """)
    if cursor.fetchone()[0] == 0:
        cursor.execute("CREATE INDEX idx_colaborador ON colaboradores (colaborador)")
        conn.commit()
    cursor.close()
    conn.close()

# Chame a função create_index ao iniciar o aplicativo
create_index()

# Carregar colaboradores de um arquivo JSON
def load_colaboradores():
    global colaboradores
    with open('colaboradores.json', 'r') as f:
        colaboradores = json.load(f)

load_colaboradores()

@app.before_request
def before_request():
    session.permanent = True
    if 'user' not in session and request.endpoint not in ['login', 'static']:
        return redirect(url_for('login'))

@app.route('/autocomplete_colaboradores')
def autocomplete_colaboradores():
    term = request.args.get('term').lower()
    results = [colaborador for colaborador in colaboradores if term in colaborador.lower()]
    return jsonify(results)

@app.route('/autocomplete_nomes')
def autocomplete_nomes():
    term = request.args.get('term')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT nome FROM patrimonios WHERE nome LIKE %s", (f"%{term}%",))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([row[0] for row in results])

@app.route('/autocomplete_etiquetas')
def autocomplete_etiquetas():
    term = request.args.get('term')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT etiqueta FROM patrimonios WHERE etiqueta LIKE %s", (f"%{term}%",))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([row[0] for row in results])

@app.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('index.html')

@app.route('/cadastrar_patrimonio')
def cadastrar_patrimonio():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('cadastro.html')

@app.route('/listar_patrimonios')
def listar_patrimonios():
    if 'user' not in session:
        return redirect(url_for('login'))
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM patrimonios")
    patrimonios = cursor.fetchall()
    
    cursor.execute("SELECT SUM(valor) FROM patrimonios")
    valor_total = cursor.fetchone()[0] or 0
    
    total_patrimonios = len(patrimonios)
    
    cursor.close()
    conn.close()
    return render_template('listar.html', patrimonios=patrimonios, valor_total=valor_total, total_patrimonios=total_patrimonios)

@app.route('/cadastrar', methods=['POST'])
def cadastrar():
    nome = request.form['nome']
    colaborador = request.form['colaborador']
    colaborador2 = request.form['colaborador2']
    especificacao = request.form['especificacao']
    estado = request.form['estado']
    valor = request.form['valor']
    observacao = request.form['observacao']
    anexos = request.files.getlist('anexos')
    etiquetas = request.form['etiqueta'].split(',')

    conn = get_db_connection()
    cursor = conn.cursor()

    for etiqueta in etiquetas:
        etiqueta = etiqueta.strip()

        # Verificar se a etiqueta já existe
        cursor.execute("SELECT COUNT(*) FROM patrimonios WHERE etiqueta = %s", (etiqueta,))
        if cursor.fetchone()[0] > 0:
            cursor.close()
            conn.close()
            return f"Erro: Etiqueta {etiqueta} já cadastrada!", 400

        # Criar pasta da etiqueta se não existir
        etiqueta_folder_id = create_folder_if_not_exists(etiqueta, FOLDER_ID)
        folder_url = f"https://drive.google.com/drive/folders/{etiqueta_folder_id}"

        try:
            # Upload dos anexos para a pasta da etiqueta no Google Drive
            for anexo in anexos:
                if (anexo):
                    anexo_path = os.path.join(tmp_dir, anexo.filename)
                    anexo.save(anexo_path)
                    file_metadata = {
                        'name': anexo.filename,
                        'parents': [etiqueta_folder_id]
                    }
                    media = MediaFileUpload(anexo_path, mimetype=anexo.content_type)
                    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        finally:
            # Remover arquivos temporários após o upload
            shutil.rmtree(tmp_dir)
            os.makedirs(tmp_dir)

        cursor.execute("""
            INSERT INTO patrimonios (nome, colaborador, colaborador2, etiqueta, especificacao, estado, valor, observacao, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (nome, colaborador, colaborador2, etiqueta, especificacao, estado, valor, observacao, folder_url))
        conn.commit()

    cursor.close()
    conn.close()

    return redirect(url_for('index'))

@app.route('/editar_patrimonio', methods=['POST'])
def editar_patrimonio():
    patrimonio_id = request.form['id']
    nome = request.form['nome']
    colaborador = request.form['colaborador']
    colaborador2 = request.form['colaborador2']
    etiqueta = request.form['etiqueta']
    especificacao = request.form['especificacao']
    estado = request.form['estado']
    valor = request.form['valor']
    observacao = request.form['observacao']

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE patrimonios
        SET nome = %s, colaborador = %s, colaborador2 = %s, etiqueta = %s, especificacao = %s, estado = %s, valor = %s, observacao = %s
        WHERE id = %s
    """, (nome, colaborador, colaborador2, etiqueta, especificacao, estado, valor, observacao, patrimonio_id))
    conn.commit()
    cursor.close()
    conn.close()

    return 'OK', 200

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM usuarios WHERE username = %s AND password = %s", (username, password))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if user:
            session['user'] = username
            return redirect(url_for('index'))
        else:
            error = "Erro: Credenciais inválidas!"
    
    return render_template('login.html', error=error)

@app.route('/atualizar_colaboradores')
def atualizar_colaboradores():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT colaborador FROM colaboradores")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    colaboradores = [row[0] for row in results]
    
    with open('colaboradores.json', 'w', encoding='utf-8') as f:
        json.dump(colaboradores, f, ensure_ascii=False, indent=4)
    
    load_colaboradores()
    return 'Colaboradores atualizados com sucesso', 200

# Configurar logs do APScheduler
logging.basicConfig(level=logging.INFO)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

# Função wrapper para medir o tempo de execução
def log_execution_time(func):
    def wrapper():
        start_time = time.time()
        func()
        elapsed_time = time.time() - start_time
        logging.info(f"Job {func.__name__} executado em {elapsed_time:.2f} segundos.")
    return wrapper

def routeviolation_completo():
    from authtoken import obter_token
    token = obter_token()
    if token:
        routeviolation(token)
        verificar_violações_por_velocidade(token)
    else:
        print("❌ Não foi possível obter o token.")

def refresh_mv_job():
    start_time = time.time()
    refresh_mv()
    elapsed_time = time.time() - start_time
    logging.info(f"Job refresh_mv executado em {elapsed_time:.2f} segundos.")

# Configurar o agendador para atualizar a lista de colaboradores diariamente
scheduler = BackgroundScheduler()
scheduler.add_job(func=lambda: app.test_client().get('/atualizar_colaboradores'), trigger="interval", days=1)

# Dividir o job em três jobs separados
scheduler.add_job(
    func=log_execution_time(processar_grid),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)

scheduler.add_job(
    func=log_execution_time(atualizar_ultima_execucao),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)

scheduler.add_job(
    func=log_execution_time(routeviolation_completo),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)

scheduler.add_job(
    func=refresh_mv_job,
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)

try:
    scheduler.start()
    logging.info("Agendador iniciado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao iniciar o agendador: {e}")

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, host='0.0.0.0')  # Desativar reloader para evitar conflitos com o agendador
