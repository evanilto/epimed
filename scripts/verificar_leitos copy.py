import os
import psycopg2
import requests
from datetime import datetime
from dotenv import load_dotenv

# Configurações do banco de dados
load_dotenv()

EPIMED_DB_CONFIG = {
    'dbname': os.getenv("epimed_dbname"),
    'user': os.getenv("epimed_user"),
    'password': os.getenv("epimed_password"),
    'host': os.getenv("epimed_host"),
    'port': os.getenv("epimed_port")
}

AGHU_DB_CONFIG = {
    'dbname': os.getenv("aghu_dbname"),
    'user': os.getenv("aghu_user"),
    'password': os.getenv("aghu_password"),
    'host': os.getenv("aghu_host"),
    'port': os.getenv("aghu_port")
}

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

# URL do WebService HL7
WEB_SERVICE_URL = "https://api.hospital.com/hl7"

""" def gerar_mensagem_hl7(leito_id, descricao, unidade): """
def gerar_mensagem_hl7(leito_id):
    """Gera uma mensagem HL7 com MSH, PID e PV1 para o leito"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    msh = f"MSH|^~\\&|LeitoSystem|Local|SistemaDestino|Destino|{timestamp}||ADT^A01|{leito_id}|P|2.5"
    """ pid = f"PID|||LEITO{leito_id}||LEITO VIRTUAL||19000101|U|||ENDERECO HOSPITAL||5555555555"
    pv1 = f"PV1||I|{unidade}^{descricao}^{leito_id}|||||||||||||||" """
    pid = f"PID|||LEITO{leito_id}||LEITO VIRTUAL||19000101|U|||ENDERECO HOSPITAL||5555555555"
    pv1 = f"PV1||I|LEITO^{leito_id}|||||||||||||||"
    return f"{msh}\n{pid}\n{pv1}"

def conectar_db(config):
    return psycopg2.connect(**config)

def obter_leitos_aghu(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT lto_id FROM "agh"."ain_leitos" WHERE lto_id = %s', ('CTI I001',))
        """ return {row[0]: (row[1], row[2]) for row in cursor.fetchall()} """
        """ return {row[0]: (row[1], row[2]) for row in cursor.fetchall()} """
        return {row[0] for row in cursor.fetchall()}

def obter_leitos_epimed(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT unitcode FROM leitos WHERE unitcode = %s', ('CTI I001',))
        """ return {row[0]: (row[1], row[2]) for row in cursor.fetchall()} """
        """ return {row[0]: (row[1], row[2]) for row in cursor.fetchall()} """
        return {row[0] for row in cursor.fetchall()}


""" def inserir_leito_local(conexao, leito_id, descricao, unidade): """
def inserir_leito_local(conexao, leito_id):
    with conexao.cursor() as cursor:
               cursor.execute("INSERT INTO leitos (clientid, bedcode) VALUES (%s, %s)", (leito_id, leito_id,))
    conexao.commit()

def salvar_log_envio(leito_id, status, resposta, conexao):
    with conexao.cursor() as cursor:
        cursor.execute("""
            INSERT INTO log_envio_hl7 (lto_id, data_envio, status, resposta)
            VALUES (%s, NOW(), %s, %s)
        """, (leito_id, status, resposta))
    conexao.commit()

def enviar_mensagem_hl7(mensagem):
    print(mensagem)
    """ response = requests.post(WEB_SERVICE_URL, data=mensagem, headers={'Content-Type': 'text/plain'})
    response.raise_for_status()
    return response.text """

def sincronizar_leitos():
    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        leitos_epimed = obter_leitos_epimed(conn_epimed)
        leitos_aghu = obter_leitos_aghu(conn_aghu)

        """ novos_leitos = {
            lid: info for lid, info in leitos_aghu.items()
            if lid not in leitos_epimed
        } """

        novos_leitos = leitos_aghu - leitos_epimed  # Diferença de conjuntos

        print(f"Novos leitos detectados: {len(novos_leitos)}")

        """ for leito_id, (descricao, unidade) in novos_leitos.items(): """
        for leito_id in novos_leitos:

            """ mensagem = gerar_mensagem_hl7(leito_id, descricao, unidade) """
            mensagem = gerar_mensagem_hl7(leito_id)
            try:
                resposta = enviar_mensagem_hl7(mensagem)
                status = "sucesso"
                print(f"Leito {leito_id} enviado com sucesso.")
                """ inserir_leito_local(conn_epimed, leito_id, descricao, unidade) """
                inserir_leito_local(conn_epimed, leito_id)
            except requests.RequestException as e:
                resposta = str(e)
                status = "erro"
                print(f"Erro ao enviar leito {leito_id}: {resposta}")

            salvar_log_envio(leito_id, status, resposta, conn_epimed)

    finally:
        conn_epimed.close()
        conn_aghu.close()

# Execução principal
if __name__ == "__main__":
    sincronizar_leitos()