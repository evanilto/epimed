import os
import psycopg2
import requests
import logging
from logging.handlers import TimedRotatingFileHandler
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

# URL do WebService HL7
WEB_SERVICE_URL = "https://api.hospital.com/hl7"

# Caminho base para salvar os logs
LOG_DIR = "/var/www/html/epimed/logs"

# Gera o nome do arquivo com a data atual
data_hoje = datetime.now().strftime("%Y-%m-%d")
LOG_NAME = f"sincronizar_leitos_{data_hoje}.log"
LOG_PATH = os.path.join(LOG_DIR, LOG_NAME)

# Certifique-se de que o diretório existe
os.makedirs(LOG_DIR, exist_ok=True)

# Criar handler com modo append para não sobrescrever
handler = logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Criar logger personalizado
logger = logging.getLogger("leito_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False  # Evita duplicação de mensagens no console

# Função para registrar logs com diferentes níveis
def registrar_log(mensagem, nivel="info"):
    if nivel == "info":
        logger.info(mensagem)
    elif nivel == "error":
        logger.error(mensagem)
    elif nivel == "warning":
        logger.warning(mensagem)

def gerar_mensagem_hl7(leito_id, ind_situacao):
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
        cursor.execute("""
            SELECT
                unidades_funcionais.seq AS unitcode,
                unidades_funcionais.descricao AS unitname,
                leitos.lto_id AS bedcode,
                leitos.lto_id AS bedname,
                CASE
                    WHEN leitos.ind_leito_extra = 'N' THEN 1
                    WHEN leitos.ind_leito_extra = 'S' THEN 2
                END AS typebedcode,
                CASE
                    WHEN leitos.ind_situacao = 'I' THEN 0
                    WHEN leitos.ind_situacao = 'A' THEN 1
                END AS bedstatus
            FROM AGH.AIN_LEITOS AS leitos
            INNER JOIN AGH.AGH_UNIDADES_FUNCIONAIS unidades_funcionais
                ON leitos.unf_seq = unidades_funcionais.seq
        """)
        return {row[0]: row[1:] for row in cursor.fetchall()}

def obter_leitos_epimed(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT clientid, bedcode, bedstatus FROM leitos')
        return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

def inserir_leito_epimed(conexao, leito_id, ind_situacao):
    with conexao.cursor() as cursor:
               cursor.execute("INSERT INTO leitos (clientid, bedcode, bedstatus) VALUES (%s, %s, %s)", (leito_id, leito_id, ind_situacao))
    conexao.commit()

def atualizar_status_leito(conexao, leito_id, nova_situacao):
    with conexao.cursor() as cursor:
        cursor.execute(
            "UPDATE leitos SET bedstatus = %s WHERE clientid = %s",
            (nova_situacao, leito_id)
        )
    conexao.commit()

def salvar_log_envio(leito_id, mensagem, conexao):
    with conexao.cursor() as cursor:
        cursor.execute("""
            INSERT INTO log_envio_hl7 (lto_id, data_envio, status, mensagem, id_log)
            VALUES (%s, NOW(), 'enviado', %s, 1)
            RETURNING id
        """, (leito_id, mensagem))
        log_id = cursor.fetchone()[0]
    conexao.commit()
    return log_id

def salvar_log_resposta(log_id, resposta, conexao):
    with conexao.cursor() as cursor:
        cursor.execute(
            "UPDATE log_envio_hl7 SET resposta = %s, status = %s, id_log = %s WHERE id = %s",
            (resposta, 'recebido', log_id, 2)
        )
    conexao.commit()

def enviar_mensagem_hl7(mensagem):
    print(mensagem)
    """ response = requests.post(WEB_SERVICE_URL, data=mensagem, headers={'Content-Type': 'text/plain'})
    response.raise_for_status()
    return response.text """
    return 'ACK'

def obter_data_criacao(conexao, lto_id):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT dthr_lancamento FROM "agh"."ain_extrato_leitos" WHERE lto_lto_id = %s ORDER BY dthr_lancamento ASC limit 1', (lto_id,))
        row = cursor.fetchone()
        return row[0] if row else None

def obter_data_ativacao(conexao, lto_id):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT jn_date_time FROM "agh"."ain_leitos_jn" WHERE lto_id = %s AND ind_situacao = %s ORDER BY jn_date_time DESC LIMIT 1', (lto_id, 'I'))
        row = cursor.fetchone()
        return row[0] if row else None

def obter_data_inativacao(conexao, lto_id):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT jn_date_time FROM "agh"."ain_leitos_jn" WHERE lto_id = %s AND ind_situacao = %s ORDER BY jn_date_time DESC LIMIT 1', (lto_id, 'A'))
        row = cursor.fetchone()
        return row[0] if row else None

def verificar_leitos_novos():

    registrar_log("(1)-INICIANDO ROTINA DE VERIFICAÇÃO DE LEITOS NOVOS.")

    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        leitos_epimed = obter_leitos_epimed(conn_epimed)
        leitos_aghu = obter_leitos_aghu(conn_aghu)

        novos_leitos = {
            lid: info for lid, info in leitos_aghu.items()
            if lid not in leitos_epimed
        }

        if not novos_leitos:
            msg = "Nenhum novo leito detectado."
            print(msg)
            registrar_log(msg)
            return

        """ print(f"Novos leitos detectados: {len(novos_leitos)}")
        print(novos_leitos)
        input("Pressione Enter para continuar...") """

        registrar_log(f"{len(novos_leitos)} novo(s) leito(s) detectado(s).")

        for leito_id, ind_situacao in novos_leitos.items():
            if ind_situacao == 'A':

                dt_ativacao = obter_data_criacao(conn_aghu, leito_id)

                if dt_ativacao:
                    data_formatada = dt_ativacao.strftime("%Y-%m-%d %H:%M:%S")
                    registrar_log(f"Leito {leito_id} está ativo desde {data_formatada}.")
                else:
                    registrar_log(f"Leito {leito_id} ativo, mas sem data de ativação encontrada.", nivel="warning")

                try:
                    mensagem = gerar_mensagem_hl7(leito_id, ind_situacao)
                    log_id = salvar_log_envio(leito_id, mensagem, conn_epimed)
                    resposta = enviar_mensagem_hl7(mensagem)
                    salvar_log_resposta(log_id, resposta, conn_epimed)
                    msg = f"Leito {leito_id} enviado com sucesso."
                    status = "sucesso"
                    print(msg)
                    registrar_log(msg)
                except requests.RequestException as e:
                    resposta = str(e)
                    status = "erro"
                    msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                    print(msg)
                    registrar_log(msg, nivel="error")

                if status == "sucesso":
                    try:
                        inserir_leito_epimed(conn_epimed, leito_id, ind_situacao)
                        registrar_log(f"Leito {leito_id} inserido na base local após envio.")
                    except Exception as e:
                        registrar_log(f"Erro ao inserir leito {leito_id} na base local: {e}", nivel="error")

            else:
                try:
                    inserir_leito_epimed(conn_epimed, leito_id, ind_situacao)
                    registrar_log(f"Leito inativo {leito_id} inserido na base local.")
                except Exception as e:
                    registrar_log(f"Erro ao inserir leito inativo {leito_id}: {e}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")
        print("Rotina de inclusão de leitos novos executada com sucesso!!!")

def verificar_alteracoes_status():

    registrar_log("(2)-INICIANDO ROTINA DE VERIFICAÇÃO DE MUDANÇA DE STATUS DO LEITO.")

    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        leitos_epimed = obter_leitos_epimed(conn_epimed)  # {clientid: (bedcode, bedstatus)}
        leitos_aghu = obter_leitos_aghu(conn_aghu)        # {clientid: ind_situacao}

        alteracoes = {}

        for leito_id in leitos_epimed:
            if leito_id in leitos_aghu:
                _, bedstatus_epimed = leitos_epimed[leito_id]  # bedstatus = ind_situacao local
                ind_situacao_aghu = leitos_aghu[leito_id]

                if bedstatus_epimed != ind_situacao_aghu:
                    alteracoes[leito_id] = (bedstatus_epimed, ind_situacao_aghu)

        if not alteracoes:
            registrar_log("Nenhuma alteração de status detectada.")
            print("Nenhuma alteração de status detectada.")
            return

        registrar_log(f"{len(alteracoes)} leito(s) com alteração de situação detectado(s).")

        for leito_id, (sit_epimed, sit_aghu) in alteracoes.items():
            registrar_log(f"Leito {leito_id}: situação mudou de '{sit_epimed}' para '{sit_aghu}'.")

            if sit_epimed == 'I':
                dt_alt = obter_data_ativacao(conn_aghu, leito_id)

            if sit_epimed == 'A':
                dt_alt = obter_data_inativacao(conn_aghu, leito_id)

            print(sit_aghu)
            print(dt_alt)
                
            try:
                mensagem = gerar_mensagem_hl7(leito_id, sit_aghu)  # passe o novo status
                log_id = salvar_log_envio(leito_id, mensagem, conn_epimed)
                resposta = enviar_mensagem_hl7(mensagem)
                salvar_log_resposta(log_id, resposta, conn_epimed)
                status = "sucesso"
                registrar_log(f"Mensagem HL7 enviada com sucesso para leito {leito_id}.")
            except requests.RequestException as e:
                resposta = str(e)
                status = "erro"
                registrar_log(f"Erro ao enviar mensagem HL7 para leito {leito_id}: {resposta}", nivel="error")

            try:
                atualizar_status_leito(conn_epimed, leito_id, sit_aghu)
                registrar_log(f"Situação do leito {leito_id} atualizada para '{sit_aghu}' na base local.")
            except Exception as e:
                registrar_log(f"Erro ao atualizar leito {leito_id} na base local: {e}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")
        print("Rotina de verificação de alterações de status concluída com sucesso!!!")

####################
# Execução principal
if __name__ == "__main__":
    verificar_leitos_novos()
    verificar_alteracoes_status()