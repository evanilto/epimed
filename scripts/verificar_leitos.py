import os
import psycopg2
import requests
import logging
import xml.etree.ElementTree as ET
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

def gerar_mensagem_hl7(unitcode, unitname, unittypecode, bedcode, bedname,
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

    msh = f"MSH|^~&|HUAP||EPIMED||{timestamp}||ORU^R01|20190409220503_ORU_65870_95936689|P|2.5||||||ASCII"
    pid = f"PID|1||||||||||||||||||||||"
    pv1 = f"PV1|1||{unitcode}^^{unitname}||||||||||||||||||||||||||||||"
    obr = f"OBR|1|{clientid}|||||{updatetimestamp}||||||||||||||||||||||||||||"
    obx = f"OBX|1|ST|{bedcode}^{bedname}||{typebedcode}^{bedstatus}|||||||{activebeddate}||{disablebeddate}||||||"

    # unittypecode não é considerado nesse momento
    #pv1 = f"PV1|1||{unitcode}^{unittypecode}^{unitname}||||||||||||||||||||||||||||||"

    return f"{msh}\n{pid}\n{pv1}\n{obr}\n{obx}"

def enviar_mensagem_hl7(log_id, mensagem, conexao):

    namespaces = {
    's': 'http://www.w3.org/2003/05/soap-envelope',
    'a': 'http://www.w3.org/2005/08/addressing',
    't': 'http://tempuri.org/'
}
    url = os.getenv("EPIMED_ENDPOINT")
    token = os.getenv("EPIMED_TOKEN")
    integrationId = os.getenv("EPIMED_INTEGRATION_ID")

    headers = {
        "Content-Type": "application/soap+xml; charset=utf-8"
    }

    soap_body = f'''<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
        xmlns:tem="http://tempuri.org/">
        <soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>http://tempuri.org/IEwsClient/SendHl7Message_DynamicToken</wsa:Action>
            <wsa:To>{url}</wsa:To>
        </soap:Header>
        <soap:Body>
            <tem:SendHl7Message_DynamicToken>
                <tem:dynamicToken>{token}</tem:dynamicToken>
                <tem:integrationId>{integrationId}</tem:integrationId>
                <tem:message><![CDATA[{mensagem}]]></tem:message>
            </tem:SendHl7Message_DynamicToken>
        </soap:Body>
    </soap:Envelope>'''

    try:
        ack_code = None
        response = None

        response = requests.post(
            url,
            data=soap_body.encode("utf-8"),
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso!")
        print("\n📨 Status Code:", response.status_code)
        #print("\n📨 Headers:")
        print("\n📨 Body:", soap_body)
        #for k, v in response.headers.items():
        #    print(f"   {k}: {v}")
        #print("\n📨 Corpo da resposta (raw XML):")
        #print(response.text)

        # Parseia o XML
        root = ET.fromstring(response.content)

        # Localiza o elemento com a resposta HL7
        hl7_elem = root.find('.//t:SendHl7Message_DynamicTokenResult', namespaces)

        if hl7_elem is not None and hl7_elem.text:
            hl7_resp = hl7_elem.text.strip()
            print("📦 Resposta HL7:")
            print(hl7_resp)
            
            # Quebra em linhas HL7
            hl7_lines = hl7_resp.splitlines()

            # Procura o segmento MSA e extrai o ACK code
            for line in hl7_lines:
                if line.startswith("MSA"):
                    parts = line.split("|")
                    if len(parts) > 1:
                        ack_code = parts[1]
                    break

            if ack_code == "AA":
                print("✅ ACK recebido com sucesso (AA - Application Accept).")
            elif ack_code == "AE":
                print("⚠️ ACK com erro de aplicação (AE - Application Error).")
            elif ack_code == "AR":
                print("❌ ACK rejeitado (AR - Application Reject).")
            elif ack_code:
                print(f"ACK com código desconhecido: {ack_code}")
            else:
                print("ACK não encontrado no segmento MSA.")

        else:
            print("❌ Conteúdo HL7 não encontrado na resposta.")

        salvar_log_resposta(log_id, mensagem, hl7_resp, conexao)

    except requests.exceptions.HTTPError as http_err:
        print("❌ Erro HTTP:", http_err)
        print("📨 Corpo da resposta de erro:")
        print(response.text)
        raise
    except Exception as e:
        print("❌ Erro geral:", e)
        raise

    return ack_code

def conectar_db(config):
    return psycopg2.connect(**config)

def obter_leitos_aghu(conexao):
    with conexao.cursor() as cursor:
        cursor.execute("""
            SELECT
                unidades_funcionais.seq AS unitcode,
                unidades_funcionais.descricao AS unitname,
                unidades_funcionais.ind_unid_cti AS unittypecode,
                leitos.lto_id AS bedcode,
                leitos.lto_id AS bedname,
                leitos.ind_leito_extra AS typebedcode,
                leitos.ind_situacao
            FROM AGH.AIN_LEITOS AS leitos
            INNER JOIN AGH.AGH_UNIDADES_FUNCIONAIS unidades_funcionais
                ON leitos.unf_seq = unidades_funcionais.seq
        """)
        return {row[3]: row[:] for row in cursor.fetchall()}  # bedname como chave

def obter_leitos_epimed(conexao):
    with conexao.cursor() as cursor:
        cursor.execute('SELECT clientid, bedcode, bedstatus FROM leitos')
        return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

def inserir_leito_epimed(conexao, leito_id, ind_situacao, activebeddate=None, disablebeddate=None):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO leitos (clientid, bedcode, bedstatus, activebeddate, disablebeddate)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (leito_id, leito_id, ind_situacao, activebeddate, disablebeddate)
            )

        registrar_log(f"Leito {leito_id} inserido na base local.", nivel="info")

    except Exception as e:
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

        registrar_log(f"Log de envio criado para leito {leito_id} com id {log_id}.", nivel="info")

        return log_id

    except Exception as e:
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

        registrar_log(f"Log de resposta atualizado para id {log_id}.", nivel="info")

    except Exception as e:
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

        registrar_log(f"Status do leito {leito_id} atualizado para {nova_situacao}.", nivel="info")

    except Exception as e:
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
           registrar_log("Nenhum novo leito detectado.")
           print("Nenhum novo leito detectado.")
           print("Rotina de inclusão de leitos novos executada com sucesso!")
           return

        registrar_log(f"{len(novos_leitos)} novo(s) leito(s) detectado(s).")
        print(f"Detectados {len(novos_leitos)} novo(s) leito(s).")

        for leito_id, info in novos_leitos.items():
            unitcode, unitname, unittypecode, bedcode, bedname, typebedcode, ind_situacao = info

            activebeddate = disablebeddate = None
            updatetimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "pendente"  

            #verifica se alguma vez esteve inativo
            dti = None
            dta = obter_data_ativacao(conn_aghu, leito_id)
            if dta:
                dti = obter_data_inativacao(conn_aghu, leito_id)
            disablebeddate = dti.strftime("%Y-%m-%d %H:%M:%S") if dti else None

            if ind_situacao == "A":  

                dta = obter_data_ativacao(conn_aghu, leito_id)
                if not dta:
                    dta = obter_data_criacao(conn_aghu, leito_id)

                activebeddate = dta.strftime("%Y-%m-%d %H:%M:%S")

                registrar_log(f"Leito {leito_id} está ATIVO desde {activebeddate}.")
            else:

                dti = obter_data_inativacao(conn_aghu, leito_id)
                if dti:
                   disablebeddate = dti.strftime("%Y-%m-%d %H:%M:%S") if dti else None
                   registrar_log(f"Leito {leito_id} INATIVO, com data de inativação em {dti}", nivel="warning")
                else:
                    dta = obter_data_criacao(conn_aghu, leito_id)
                    registrar_log(f"Leito {leito_id} INATIVO, com data de criação em {dta}", nivel="warning")

            try:
                resposta = None

                with conn_epimed: #commit e rollback automáticos

                    if ind_situacao == "A":  #só envia leitos ativos
                    #if ind_situacao in ("A", "I") :  #carga inicial de leitos ativos e inativos

                        log_id = salvar_log_envio(leito_id, conn_epimed)
                        clientid = log_id

                        status_map = {"A": "1", "I": "0"}
                        type_map = {"N": "1", "S": "2"}
                        unittype_map = {"N": "GS", "S": "GE"}
                        mensagem = gerar_mensagem_hl7(
                            unitcode, unitname, unittype_map.get(unittypecode), bedcode, bedname,
                            activebeddate, disablebeddate, updatetimestamp,
                            clientid, type_map.get(typebedcode), status_map.get(ind_situacao)
                        )

                        resposta = enviar_mensagem_hl7(log_id, mensagem, conn_epimed)
                        #resposta = 'AA' #carga inicial

                        if resposta == "AA":  # ACK de sucesso
                            inserir_leito_epimed(conn_epimed, leito_id, ind_situacao, activebeddate, disablebeddate)
                            registrar_log(f"Leito {leito_id} recebido com sucesso!", nivel="info")
                        else:
                            msg = f"Erro ao enviar leito {leito_id}: ACK recebido com código {resposta}"
                            registrar_log(msg, nivel="error")

            except requests.RequestException as e:
                resposta = str(e)
                msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                registrar_log(msg, nivel="error")

        print("Rotina de inclusão de leitos novos executada com sucesso!")

    except Exception as e:
        registrar_log(f"❌ Erro na rotina de inclusão de leitos novos: {str(e)}", nivel="error")
        print(f"❌ Erro na rotina de inclusão de leitos novos: {str(e)}")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")

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
           print("Rotina de verificação de alterações de status concluída com sucesso!")
           return

        registrar_log(f"{len(alteracoes)} leito(s) com alteração de situação detectado(s).")
        print(f"Detectados {len(alteracoes)} leito(s) com alteração de situação.")

        for leito_id, novo_status in alteracoes.items():
            dados_leito = leitos_aghu[leito_id]
            unitcode, unitname, unittypecode, bedcode, bedname, typebedcode, ind_situacao = dados_leito

            activebeddate = disablebeddate = None

            if novo_status == "A":  # Ativo

                dta = obter_data_ativacao(conn_aghu, leito_id)
                if not dta:
                    dta = obter_data_criacao(conn_aghu, leito_id)

                activebeddate = dta.strftime("%Y-%m-%d %H:%M:%S") if dta else None

                registrar_log(f"Leito {leito_id} está ATIVO desde {activebeddate}.")

            elif novo_status == "I":  # Inativo

                dti = obter_data_inativacao(conn_aghu, leito_id)

                disablebeddate = dti.strftime("%Y-%m-%d %H:%M:%S") if dti else None

                # envia também a data de ativação anterior à desativação
                dta = obter_data_ativacao(conn_aghu, leito_id)
                if not dta:
                    dta = obter_data_criacao(conn_aghu, leito_id)

                activebeddate = dta.strftime("%Y-%m-%d %H:%M:%S") if dta else None

                registrar_log(f"Leito {leito_id} INATIVO, desde {disablebeddate}")

            updatetimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            registrar_log(f"Leito {leito_id}: novo status {novo_status}, gerando mensagem HL7.")

            try:

                resposta = None

                with conn_epimed: #commit e rollback automáticos

                    log_id = salvar_log_envio(leito_id, conn_epimed)
                    clientid = log_id  

                    status_map = {"A": "1", "I": "0"}
                    type_map = {"N": "1", "S": "2"}
                    unittype_map = {"N": "GS", "S": "GE"}
                    mensagem = gerar_mensagem_hl7(
                        unitcode, unitname, unittype_map.get(unittypecode), bedcode, bedname,
                        activebeddate, disablebeddate, updatetimestamp,
                        clientid, type_map.get(typebedcode), status_map.get(ind_situacao)
                    )

                    resposta = enviar_mensagem_hl7(log_id, mensagem, conn_epimed)
                    #resposta = "AA" #testes

                    if resposta == "AA":  # ACK de sucesso
                        atualizar_status_leito(conn_epimed, leito_id, novo_status, activebeddate, disablebeddate)
                        registrar_log(f"Leito {leito_id} recebido com sucesso!", nivel="info")
                    else:
                        msg = f"Erro ao enviar leito {leito_id}: ACK recebido com código {resposta}"
                        registrar_log(msg, nivel="error")

            except requests.RequestException as e:
                resposta = str(e)
                msg = f"Erro ao enviar leito {leito_id}: {resposta}"
                registrar_log(msg, nivel="error")

        print("Rotina de verificação de alterações de status concluída com sucesso!")

    except Exception as e:
        registrar_log(f"❌ Erro na rotina de verificação de alterações de status: {str(e)}", nivel="error")
        print(f"❌ Erro na rotina de verificação de alterações de status: {str(e)}")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos de dados encerradas.")

#-----------------------------------------------------------------------------------------------#
# Main                                                                                          #
#                                                                                               #
# Informa somente leitos novos ativos                                                           #
# Recupera sempre as datas mais recentes de alterações de status dos leitos                     #
#                                                                                               #
#-----------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    verificar_leitos_novos()
    verificar_alteracoes_status()
#-----------------------------------------------------------------------------------------------#