import os
from dotenv import load_dotenv
load_dotenv()

import requests
import datetime
import mysql.connector
import pytz
from authtoken import obter_token

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return date_str

def to_iso(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return date_str

def nullify_date(date_str):
    if date_str in ["01/01/1 00:00:00", "01/01/0001 00:00:00"]:
         return None
    return date_str

def processar_grid():
    token = obter_token()
    if not token:
        return

    api_url = "https://integration.systemsatx.com.br/GlobalBus/Grid/List?paramClientIntegrationCode=1003"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
    except mysql.connector.Error as err:
        print("Erro ao conectar no banco de dados:", err)
        return

    cursor = conn.cursor(buffered=True)

    # Criação das tabelas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS graderumocerto (
        line VARCHAR(50),
        estimated_departure VARCHAR(50),
        estimated_arrival VARCHAR(50),
        real_departure VARCHAR(50),
        real_arrival VARCHAR(50),
        route_integration_code VARCHAR(255) NOT NULL,
        route_name VARCHAR(255),
        direction_name VARCHAR(255),
        shift VARCHAR(50),
        estimated_vehicle VARCHAR(255),
        real_vehicle VARCHAR(255),
        estimated_distance VARCHAR(50),
        travelled_distance VARCHAR(50),
        client_name VARCHAR(255),
        PRIMARY KEY (route_integration_code)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS historico_grades (
        id INT AUTO_INCREMENT PRIMARY KEY,
        line VARCHAR(50),
        estimated_departure VARCHAR(50),
        estimated_arrival VARCHAR(50),
        real_departure VARCHAR(50),
        real_arrival VARCHAR(50),
        route_integration_code VARCHAR(255),
        route_name VARCHAR(255),
        direction_name VARCHAR(255),
        shift VARCHAR(50),
        estimated_vehicle VARCHAR(255),
        real_vehicle VARCHAR(255),
        estimated_distance VARCHAR(50),
        travelled_distance VARCHAR(50),
        client_name VARCHAR(255),
        data_registro DATE,
        UNIQUE KEY idx_codigo_data (route_integration_code, data_registro)
    );
    """)
    conn.commit()

    update_query = """
    UPDATE graderumocerto
    SET estimated_departure = %s,
        estimated_arrival = %s,
        real_departure = %s,
        real_arrival = %s,
        real_vehicle = %s,
        estimated_distance = %s,
        travelled_distance = %s
    WHERE route_integration_code = %s
    """

    insert_query = """
    INSERT INTO graderumocerto (
        line, estimated_departure, estimated_arrival, real_departure, real_arrival, 
        route_name, direction_name, shift, estimated_vehicle, real_vehicle, 
        estimated_distance, travelled_distance, client_name, route_integration_code
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_historico_query = '''
    INSERT INTO historico_grades (
        line, estimated_departure, estimated_arrival, real_departure, real_arrival,
        route_integration_code, route_name, direction_name, shift,
        estimated_vehicle, real_vehicle, estimated_distance, travelled_distance, client_name, data_registro
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        estimated_departure = VALUES(estimated_departure),
        estimated_arrival = VALUES(estimated_arrival),
        real_departure = VALUES(real_departure),
        real_arrival = VALUES(real_arrival),
        real_vehicle = VALUES(real_vehicle),
        estimated_vehicle = VALUES(estimated_vehicle),
        estimated_distance = VALUES(estimated_distance),
        travelled_distance = VALUES(travelled_distance),
        route_name = VALUES(route_name),
        direction_name = VALUES(direction_name),
        shift = VALUES(shift),
        client_name = IFNULL(VALUES(client_name), client_name),
        line = VALUES(line)
    '''

    dias_a_verificar = 30
    for i in range(dias_a_verificar):
        data_alvo = datetime.datetime.now(pytz.timezone("America/Sao_Paulo")) - datetime.timedelta(days=i)
        data_formatada = data_alvo.strftime("%d/%m/%Y")
        data_iso = to_iso(data_formatada)

        payload = [{"PropertyName": "EffectiveDate", "Condition": "Equal", "Value": data_iso}]
        response_api = requests.post(api_url, headers=headers, json=payload)

        if response_api.status_code != 200:
            print(f"Erro na API para {data_formatada}: {response_api.status_code}")
            continue

        data = response_api.json()
        if not data:
            print(f"Nenhuma grade encontrada para {data_formatada}")
            continue

        # Obter todos os códigos de integração existentes de uma vez
        cursor.execute("SELECT route_integration_code, client_name FROM graderumocerto")
        existing_routes = {row[0]: row[1] for row in cursor.fetchall()}

        batch_data = []
        for item in data:
            line = item.get('LineIntegrationCode')
            estimated_departure = nullify_date(format_date(item.get('EstimatedDepartureDate')))
            estimated_arrival = nullify_date(format_date(item.get('EstimatedArrivalDate')))
            real_departure = nullify_date(format_date(item.get('RealDepartureDate')))
            real_arrival = nullify_date(format_date(item.get('RealdArrivalDate')))
            route_integration_code = item.get('RouteIntegrationCode')
            route_name = item.get('RouteName')
            direction_name = item.get('DirectionName')
            shift = item.get('Shift')
            estimated_vehicle = item.get('EstimatedVehicle')
            real_vehicle = item.get('RealVehicle')
            estimated_distance = item.get('EstimatedDistance')
            travelled_distance = item.get('TravelledDistance')

            client_name = item.get('ClientName') or existing_routes.get(route_integration_code)
            if client_name is not None:
                client_name = client_name.strip()

            batch_data.append((
                line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                route_integration_code, route_name, direction_name, shift,
                estimated_vehicle, real_vehicle, estimated_distance, travelled_distance,
                client_name, data_alvo.date()
            ))

        # Inserir dados no histórico em lote
        cursor.executemany(insert_historico_query, batch_data)
        conn.commit()  # Commit único para o lote

        # Filtrar dados para não atualizar grades com real_arrival válido ou inválido (como 01/01/0001)
        cursor.execute("SELECT route_integration_code FROM historico_grades WHERE real_arrival IS NOT NULL AND real_arrival != '01/01/0001'")
        routes_with_real_arrival = {row[0] for row in cursor.fetchall()}

        update_data = []
        insert_data = []
        for item in batch_data:
            if item[5] in existing_routes and item[5] not in routes_with_real_arrival:
                update_data.append((
                    item[1], item[2], item[3], item[4], item[10], item[11], item[12], item[5]
                ))
            elif item[5] not in existing_routes:
                insert_data.append((
                    item[0], item[1], item[2], item[3], item[4], item[6], item[7], item[8], item[9], item[10],
                    item[11], item[12], item[13], item[5]
                ))

        if update_data:
            cursor.executemany(update_query, update_data)
        if insert_data:
            cursor.executemany(insert_query, insert_data)
        conn.commit()  # Commit único após inserções e atualizações

        print(f"✅ Grades processadas para {data_formatada}")

    # Etapa para atualizar travelled_distance no banco de dados apenas para os últimos 7 dias processados
    update_travelled_distance_query = """
    UPDATE historico_grades
    SET travelled_distance = FLOOR(estimated_distance)
    WHERE real_arrival IS NOT NULL 
      AND travelled_distance = 0
      AND STR_TO_DATE(estimated_departure, '%d/%m/%Y %H:%i:%s') >= DATE_SUB(NOW(), INTERVAL 7 DAY);
    """
    cursor.execute(update_travelled_distance_query)
    conn.commit()

    cursor.close()
    conn.close()

if __name__ == '__main__':
    processar_grid()
