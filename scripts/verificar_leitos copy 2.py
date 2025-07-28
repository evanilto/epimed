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
        cursor.execute('SELECT lto_id, ind_situacao FROM "agh"."ain_leitos"')
        return {row[0]: (row[1]) for row in cursor.fetchall()}

def obter_leitos_epimed(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT clientid, bedcode FROM leitos')
        return {row[0]: (row[1]) for row in cursor.fetchall()}

def obter_data_ativacao(conexao, lto_id):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT dthr_lancamento FROM "agh"."ain_extrato_leitos" WHERE lto_lto_id = %s ORDER BY dthr_lancamento ASC limit 1', (lto_id,))
        row = cursor.fetchone()
        return row[0] if row else None

def inserir_leito_epimed(conexao, leito_id):
    with conexao.cursor() as cursor:
               cursor.execute("INSERT INTO leitos (clientid, bedcode) VALUES (%s, %s)", (leito_id, leito_id))
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

def sincronizar_leitos_novos():
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

                dt_ativacao = obter_data_ativacao(conn_aghu, leito_id)

                if dt_ativacao:
                    data_formatada = dt_ativacao.strftime("%Y-%m-%d %H:%M:%S")
                    registrar_log(f"Leito {leito_id} está ativo desde {data_formatada}.")
                else:
                    registrar_log(f"Leito {leito_id} ativo, mas sem data de ativação encontrada.", nivel="warning")

                mensagem = gerar_mensagem_hl7(leito_id)
                try:
                    resposta = enviar_mensagem_hl7(mensagem)
                    status = "sucesso"
                    msg = f"Leito {leito_id} enviado com sucesso."
                    print(msg)
                    registrar_log(msg)
                except requests.RequestException as e:
                    resposta = str(e)
                    status = "erro"
                    msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                    print(msg)
                    registrar_log(msg, nivel="error")

                salvar_log_envio(leito_id, status, resposta, conn_epimed)

                if status == "sucesso":
                    try:
                        inserir_leito_epimed(conn_epimed, leito_id)
                        registrar_log(f"Leito {leito_id} inserido na base local após envio.")
                    except Exception as e:
                        registrar_log(f"Erro ao inserir leito {leito_id} na base local: {e}", nivel="error")

            else:
                try:
                    inserir_leito_epimed(conn_epimed, leito_id)
                    registrar_log(f"Leito inativo {leito_id} inserido na base local.")
                except Exception as e:
                    registrar_log(f"Erro ao inserir leito inativo {leito_id}: {e}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")
        print("Rotina executado com sucesso!!!")


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

                dt_ativacao = obter_data_ativacao(conn_aghu, leito_id)

                if dt_ativacao:
                    data_formatada = dt_ativacao.strftime("%Y-%m-%d %H:%M:%S")
                    registrar_log(f"Leito {leito_id} está ativo desde {data_formatada}.")
                else:
                    registrar_log(f"Leito {leito_id} ativo, mas sem data de ativação encontrada.", nivel="warning")

                mensagem = gerar_mensagem_hl7(leito_id)
                try:
                    resposta = enviar_mensagem_hl7(mensagem)
                    status = "sucesso"
                    msg = f"Leito {leito_id} enviado com sucesso."
                    print(msg)
                    registrar_log(msg)
                except requests.RequestException as e:
                    resposta = str(e)
                    status = "erro"
                    msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                    print(msg)
                    registrar_log(msg, nivel="error")

                salvar_log_envio(leito_id, status, resposta, conn_epimed)

                if status == "sucesso":
                    try:
                        inserir_leito_epimed(conn_epimed, leito_id)
                        registrar_log(f"Leito {leito_id} inserido na base local após envio.")
                    except Exception as e:
                        registrar_log(f"Erro ao inserir leito {leito_id} na base local: {e}", nivel="error")

            else:
                try:
                    inserir_leito_epimed(conn_epimed, leito_id)
                    registrar_log(f"Leito inativo {leito_id} inserido na base local.")
                except Exception as e:
                    registrar_log(f"Erro ao inserir leito inativo {leito_id}: {e}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")
        print("Rotina executado com sucesso!!!")

def sincronizar_leitos_alterados():
    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        leitos_epimed = obter_leitos_epimed(conn_epimed)  # dict {lto_id: ind_situacao}
        leitos_aghu = obter_leitos_aghu(conn_aghu)        # dict {lto_id: ind_situacao}

        alteracoes = {
            lid: (sit_epimed, sit_aghu)
            for lid, sit_epimed in leitos_epimed.items()
            if lid in leitos_aghu and sit_epimed != leitos_aghu[lid]
        }

        if not alteracoes:
            msg = "Nenhuma alteração de situação detectada nos leitos existentes."
            print(msg)
            registrar_log(msg)
            return

        registrar_log(f"{len(alteracoes)} leito(s) com alteração de situação detectado(s).")

        for leito_id, (sit_epimed, sit_aghu) in alteracoes.items():
            registrar_log(f"Leito {leito_id}: situação mudou de '{sit_epimed}' para '{sit_aghu}'.")

            # Enviar mensagem HL7 independente da nova situação
            mensagem = gerar_mensagem_hl7(leito_id)
            try:
                resposta = enviar_mensagem_hl7(mensagem)
                status = "sucesso"
                registrar_log(f"Mensagem HL7 enviada com sucesso para leito {leito_id}.")
            except requests.RequestException as e:
                resposta = str(e)
                status = "erro"
                registrar_log(f"Erro ao enviar mensagem HL7 para leito {leito_id}: {resposta}", nivel="error")

            # Salva log do envio, independentemente do sucesso
            salvar_log_envio(leito_id, status, resposta, conn_epimed)

            # Atualiza a situação do leito na base local
            try:
                atualizar_ind_situacao_leito(conn_epimed, leito_id, sit_aghu)
                registrar_log(f"Situação do leito {leito_id} atualizada para '{sit_aghu}' na base local.")
            except Exception as e:
                registrar_log(f"Erro ao atualizar leito {leito_id} na base local: {e}", nivel="error")


    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Verificação de alterações encerrada.")

####################
# Execução principal
if __name__ == "__main__":
    sincronizar_leitos_novos()
    sincronizar_leitos_alterados()