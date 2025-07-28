import os
import psycopg2
import requests
import logging
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

API_TOKEN = os.getenv("HL7_API_TOKEN")
HL7_ENDPOINT = os.getenv("HL7_API_ENDPOINT")
WEB_SERVICE_URL = "https://api.hospital.com/hl7"

LOG_DIR = "/var/www/html/epimed/logs"
os.makedirs(LOG_DIR, exist_ok=True)

data_hoje = datetime.now().strftime("%Y-%m-%d")
LOG_NAME = f"sincronizar_leitos_{data_hoje}.log"
LOG_PATH = os.path.join(LOG_DIR, LOG_NAME)

handler = logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger("leito_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

def registrar_log(mensagem, nivel="info"):
    if nivel == "info":
        logger.info(mensagem)
    elif nivel == "error":
        logger.error(mensagem)
    elif nivel == "warning":
        logger.warning(mensagem)

def gerar_mensagem_hl7(unitcode, unitname, bedcode, bedname,
                       activebeddate, disablebeddate, updatetimestamp,
                       clientid, typebedcode, bedstatus):

    if activebeddate:
        dt = datetime.strptime(activebeddate, "%Y-%m-%d %H:%M:%S")
        activebeddate = dt.strftime("%Y%m%d%H%M%S")
    else:
        activebeddate = ""

    if disablebeddate:
        dt = datetime.strptime(disablebeddate, "%Y-%m-%d %H:%M:%S")
        disablebeddate = dt.strftime("%Y%m%d%H%M%S")
    else:
        disablebeddate = ""
    
    dt = datetime.strptime(updatetimestamp, "%Y-%m-%d %H:%M:%S")
    updatetimestamp = dt.strftime("%Y%m%d%H%M%S")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    msh = f"MSH|^~\\&|HUAP||EPIMED||{timestamp}|||ORU^R01|20190409220503_ORU_65870_95936689|P|2.5||||||ASCII"
    pv1 = f"PV1|1||{unitcode}^^{unitname}||||||||||||||||||||||||||||||"
    pid = f"PID|1||||||||||||||||||||||"
    obr = f"OBR|1|{clientid}|||||{updatetimestamp}||||||||||||||||||||||||||||"
    obx = f"OBX|1|ST|{bedcode}^{bedname}||{typebedcode}^{bedstatus}|||||||{activebeddate}||{disablebeddate}||||||"

    return f"{msh}\n{pv1}\n{pid}\n{obr}\n{obx}"

def enviar_mensagem_hl7(mensagem):
    print(mensagem)
    # Simulação de envio para ambiente de teste/desenvolvimento

""" O endpoint de login
    O endpoint de envio da msg
    Tipo de requisição: application/json ou x-www-form-urlencoded?
    Campos exigidos: "username" e "password"? Ou "client_id" e "client_secret"?
    A API aceita texto puro HL7 (text/plain) ou exige application/hl7-v2?
    A quebra de linha pode ser \n ou precisa ser \r (caracter típico HL7)?
    Exemplo do curl da documentação (ou payload esperado)
        curl -X POST https://api.exemplo.com/login \
         -H "Content-Type: application/x-www-form-urlencoded" \
         -d "username=meunome&password=minhasenha"

         Resposta: {"access_token": "eyJhbGciOi..."}

         curl -X POST https://api.exemplo.com/hl7 \
            -H "Authorization: Bearer eyJhbGciOi..." \
            -H "Content-Type: text/plain" \
            --data-binary @mensagem.hl7

    Como o token vem na resposta (campo "access_token" ou "token"?)
    O token vai no cabeçalho como:
        Authorization: Bearer TOKEN
        ou Authorization: Token TOKEN
        ou outro, como X-API-KEY: TOKEN

    Requisitos adicionais do servidor
        Autorização por IP: O servidor exige IPs autorizados para acesso?
        Token tem tempo de expiração? (precisa renovar?)
        HTTPS obrigatório? (sempre sim, mas confirmar)
        Timeout ou limite de requisições por minuto?

    Resposta esperada
        O servidor retorna o quê se a mensagem for aceita?
        Um código 200 OK com texto?
        Um ACK HL7?
        Um ID de protocolo?
        Em caso de erro, como a API responde? (status 400, 401, 500?)

LOGIN_URL = "https://api.seuservico.com/login"
HL7_ENDPOINT = "https://api.seuservico.com/hl7"

# 1. Obter token via login
def obter_token(usuario, senha):
    login_payload = {
        "username": usuario,
        "password": senha
    }
    try:
        resposta = requests.post(LOGIN_URL, json=login_payload, timeout=10)
        resposta.raise_for_status()
        dados = resposta.json()
        return dados.get("access_token")  # ou "token", dependendo da API
    except Exception as e:
        print(f"Erro ao obter token: {e}")
        return None

# 2. Enviar mensagem HL7 com token
def enviar_hl7_com_token(token, mensagem_hl7):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/plain"
    }
    try:
        resposta = requests.post(HL7_ENDPOINT, data=mensagem_hl7, headers=headers, timeout=10)
        if resposta.status_code == 200:
            print("✅ Mensagem HL7 enviada com sucesso.")
        else:
            print(f"⚠️ Erro {resposta.status_code}: {resposta.text}")
    except Exception as e:
        print(f"❌ Erro na requisição HL7: {e}")
    """
     
    """
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Authorization": "Token SEU_TOKEN",  # ou "JWT SEU_TOKEN"
        "X-API-KEY": "SEU_TOKEN",  # ou "x-auth-token": "SEU_TOKEN"
        "Content-Type": "application/hl7-v2"
        "Content-Type": "text/plain"
    }
    
     try:
        response = requests.post(
            WEB_SERVICE_URL,
            data=mensagem,
            headers=headers,
            timeout=10  # evitar travar para sempre
        )
        response.raise_for_status()
        print("Mensagem enviada com sucesso:", response.status_code)
    except Exception as e:
        print("Erro ao enviar mensagem:", e) """

    return 'ACK'

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
                leitos.ind_leito_extra AS typebedcode,
                leitos.ind_situacao
            FROM AGH.AIN_LEITOS AS leitos
            INNER JOIN AGH.AGH_UNIDADES_FUNCIONAIS unidades_funcionais
                ON leitos.unf_seq = unidades_funcionais.seq
        """)
        return {row[2]: row[:] for row in cursor.fetchall()}  # bedname como chave

def obter_leitos_epimed(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT clientid, bedcode, bedstatus FROM leitos')
        return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

def inserir_leito_epimed(conexao, leito_id, ind_situacao, activebeddate=None):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO leitos (clientid, bedcode, bedstatus, activebeddate)
                VALUES (%s, %s, %s, %s)
                """,
                (leito_id, leito_id, ind_situacao, activebeddate)
            )
        conexao.commit()
        registrar_log(f"Leito {leito_id} inserido na base local.", nivel="info")
    except Exception as e:
        conexao.rollback()
        registrar_log(f"Erro ao inserir leito {leito_id} na base local: {e}", nivel="error")
        raise

def salvar_log_envio(leito_id, conexao):
    try:
        with conexao.cursor() as cursor:
            cursor.execute("""
                INSERT INTO log_envio_hl7 (lto_id, data_envio, status, id_log)
                VALUES (%s, NOW(), 'pendente', 1)
                RETURNING id
            """, (leito_id,))
            log_id = cursor.fetchone()[0]
        conexao.commit()
        registrar_log(f"Log de envio criado para leito {leito_id} com id {log_id}.", nivel="info")
        return log_id
    except Exception as e:
        conexao.rollback()
        registrar_log(f"Erro ao salvar log de envio para leito {leito_id}: {e}", nivel="error")
        raise

def salvar_log_resposta(log_id, mensagem, resposta, conexao):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                UPDATE log_envio_hl7
                SET mensagem = %s, resposta = %s, status = %s, id_log = %s
                WHERE id = %s
                """,
                (mensagem, resposta, 'enviado', log_id, log_id)
            )
        conexao.commit()
        registrar_log(f"Log de resposta atualizado para id {log_id}.", nivel="info")
    except Exception as e:
        conexao.rollback()
        registrar_log(f"Erro ao atualizar log de resposta para id {log_id}: {e}", nivel="error")
        raise
        
def atualizar_status_leito(conexao, leito_id, nova_situacao, activebeddate=None, disablebeddate=None):
    try:
        with conexao.cursor() as cursor:
            campos = ["bedstatus = %s"]
            valores = [nova_situacao]

            if activebeddate is not None:
                campos.append("activebeddate = %s")
                valores.append(activebeddate)

            if disablebeddate is not None:
                campos.append("disablebeddate = %s")
                valores.append(disablebeddate)

            valores.append(leito_id)

            query = f"UPDATE leitos SET {', '.join(campos)} WHERE clientid = %s"
            cursor.execute(query, tuple(valores))
        conexao.commit()
        registrar_log(f"Status do leito {leito_id} atualizado para {nova_situacao}.", nivel="info")
    except Exception as e:
        conexao.rollback()
        registrar_log(f"Erro ao atualizar status do leito {leito_id}: {e}", nivel="error")
        raise

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

        registrar_log(f"{len(novos_leitos)} novo(s) leito(s) detectado(s).")

        for leito_id, info in novos_leitos.items():
            unitcode, unitname, bedcode, bedname, typebedcode, ind_situacao = info

            activebeddate = disablebeddate = None
            updatetimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "pendente"  

            dt_criacao = obter_data_criacao(conn_aghu, leito_id)

            if ind_situacao == "A":  
                activebeddate = dt_criacao.strftime("%Y-%m-%d %H:%M:%S")
                registrar_log(f"Leito {leito_id} está ATIVO desde {activebeddate}.")
            else:
                registrar_log(f"Leito {leito_id} INATIVO, com data de criacao em {dt_criacao}", nivel="warning")

            try:

                if ind_situacao == "A":  
                    log_id = salvar_log_envio(leito_id, conn_epimed)
                    clientid = log_id

                    status_map = {"A": "1", "I": "0"}
                    type_map = {"N": "1", "S": "2"}
                    mensagem = gerar_mensagem_hl7(
                        unitcode, unitname, bedcode, bedname,
                        activebeddate, disablebeddate, updatetimestamp,
                        clientid, type_map.get(typebedcode), status_map.get(ind_situacao)
                    )

                    resposta = enviar_mensagem_hl7(mensagem)
                    salvar_log_resposta(log_id, mensagem, resposta, conn_epimed)

                inserir_leito_epimed(conn_epimed, leito_id, ind_situacao, activebeddate)
                status = "sucesso"
            except requests.RequestException as e:
                resposta = str(e)
                status = "erro"
                msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                registrar_log(msg, nivel="error")

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
        leitos_epimed = obter_leitos_epimed(conn_epimed)  
        leitos_aghu = obter_leitos_aghu(conn_aghu)        

        alteracoes = {}

        for leito_id in leitos_epimed:
            if leito_id in leitos_aghu:
                _, bedstatus_epimed = leitos_epimed[leito_id]
                *_, bedstatus_aghu = leitos_aghu[leito_id]

                if bedstatus_epimed != bedstatus_aghu:
                    alteracoes[leito_id] = bedstatus_aghu

        if not alteracoes:
            registrar_log("Nenhuma alteração de status detectada.")
            print("Nenhuma alteração de status detectada.")
            return

        registrar_log(f"{len(alteracoes)} leito(s) com alteração de situação detectado(s).")

        for leito_id, novo_status in alteracoes.items():
            dados_leito = leitos_aghu[leito_id]
            unitcode, unitname, bedcode, bedname, typebedcode, ind_situacao = dados_leito

            activebeddate = disablebeddate = None

            if novo_status == "S":  # Ativo
                dt = obter_data_ativacao(conn_aghu, leito_id)
                activebeddate = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None
            elif novo_status == "N":  # Inativo
                dt = obter_data_inativacao(conn_aghu, leito_id)
                disablebeddate = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

            updatetimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            registrar_log(f"Leito {leito_id}: novo status {novo_status}, gerando mensagem HL7.")

            try:
                log_id = salvar_log_envio(leito_id, conn_epimed)
                clientid = log_id  

                status_map = {"A": "1", "I": "0"}
                type_map = {"N": "1", "S": "2"}
                mensagem = gerar_mensagem_hl7(
                    unitcode, unitname, bedcode, bedname,
                    activebeddate, disablebeddate, updatetimestamp,
                    clientid, type_map.get(typebedcode), status_map.get(ind_situacao)
                )

                resposta = enviar_mensagem_hl7(mensagem)
                salvar_log_resposta(log_id, mensagem, resposta, conn_epimed)
                atualizar_status_leito(conn_epimed, leito_id, novo_status, activebeddate, disablebeddate)
            except requests.RequestException as e:
                resposta = str(e)
                registrar_log(f"Erro ao enviar mensagem HL7 para leito {leito_id}: {resposta}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")
        print("Rotina de verificação de alterações de status concluída com sucesso!!!")

if __name__ == "__main__":
    verificar_leitos_novos()
    verificar_alteracoes_status()
