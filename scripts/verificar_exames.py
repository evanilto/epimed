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
LOG_NAME = f"sincronizar_exames_{data_hoje}.log"
LOG_PATH = os.path.join(LOG_DIR, LOG_NAME)

handler = logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger("leito_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

# ---------------------------
# Rotinas de conexão e log
# ---------------------------
def conectar_db(config):
    import psycopg2
    return psycopg2.connect(**config)

def registrar_log(mensagem, nivel="info"):
    print(f"[{nivel.upper()}] {datetime.now()} - {mensagem}")

# ---------------------------
# Rotinas de consulta AGHU
# ---------------------------
def obter_internacoes_aghu(conn_aghu):
    internacoes = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT codigo, medicalrecord, hospitaladmissiondate, ind_alta
            FROM vw_epimed;
        """)
        for codigo, prontuario, dthrinternacao, ind_alta in cur.fetchall():
            internacoes[codigo] = (prontuario, dthrinternacao, ind_alta)
    return internacoes

def obter_admissoes_aghu(conn_aghu, codigo_internacao):
    admissoes = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT unitcode, bedcode, unitadmissiondatetime
            FROM vw_epimed_admissoes
            WHERE codigo = %s;
        """, (codigo_internacao,))
        for unitcode, bedcode, dthrentradaleito in cur.fetchall():
            admissoes[(unitcode, bedcode, dthrentradaleito)] = (unitcode, bedcode, dthrentradaleito)
    return admissoes

def obter_exames_aghu(conn_aghu, prontuario):
    exames = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
                   result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo
            FROM vw_exames
            WHERE prontuario = %s;
        """, (prontuario,))
        for row in cur.fetchall():
            idexame, dthrexame, descricao_usual, valor, tipo_inf_valor, \
            result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo = row
            exames[(idexame, dthrexame)] = (descricao_usual, valor, tipo_inf_valor, 
                                            result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo)
    return exames

# ---------------------------
# Rotinas de consulta Epimed
# ---------------------------
def obter_internacoes_epimed(conn_epimed):
    internacoes = {}
    with conn_epimed.cursor() as cur:
        cur.execute("""
            SELECT id, codigo, prontuario, dthrinternacao, ind_alta
            FROM internacoes;
        """)
        for id_, codigo, prontuario, dthrinternacao, ind_alta in cur.fetchall():
            internacoes[codigo] = (id_, prontuario, dthrinternacao, ind_alta)
    return internacoes

def obter_admissoes_epimed(conn_epimed, int_id):
    admissoes = {}
    with conn_epimed.cursor() as cur:
        cur.execute("""
            SELECT id, unitcode, bedcode, dthrentradaleito
            FROM admissoes
            WHERE int_id = %s;
        """, (int_id,))
        for id_, unitcode, bedcode, dthrentradaleito in cur.fetchall():
            admissoes[(unitcode, bedcode, dthrentradaleito)] = id_
    return admissoes

def obter_exames_epimed(conn_epimed, adm_id):
    exames = set()
    with conn_epimed.cursor() as cur:
        cur.execute("""
            SELECT idexame, dthrexame
            FROM exames
            WHERE adm_id = %s;
        """, (adm_id,))
        for idexame, dthrexame in cur.fetchall():
            exames.add((idexame, dthrexame))
    return exames

# ---------------------------
# Rotinas de inserção Epimed
# ---------------------------
def inserir_internacao_epimed(conn_epimed, codigo, prontuario, dthrinternacao, ind_alta):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO internacoes (codigo, prontuario, dthrinternacao, ind_alta)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (codigo, prontuario, dthrinternacao, ind_alta))
        int_id = cur.fetchone()[0]
    registrar_log(f"Internação {codigo} inserida com sucesso.")
    return int_id

def inserir_admissao_epimed(conn_epimed, int_id, unitcode, bedcode, dthrentradaleito):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO admissoes (int_id, unitcode, bedcode, dthrentradaleito)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (int_id, unitcode, bedcode, dthrentradaleito))
        adm_id = cur.fetchone()[0]
    registrar_log(f"Admissão {unitcode}-{bedcode} inserida com sucesso.")
    return adm_id

def inserir_exame_epimed(conn_epimed, adm_id, idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
                         result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO exames (adm_id, idexame, idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
                                result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (adm_id, idexame, idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
              result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo))
    registrar_log(f"Exame {idexame} inserido com sucesso.")

# ---------------------------
# Rotina HL7
# ---------------------------
def gerar_mensagem_hl7_exame(prontuario, idexame, dthrexame, valor, descricao_usual):
    return f"HL7_EXAME|{prontuario}|{idexame}|{dthrexame}|{valor}|{descricao_usual}"

def enviar_mensagem_hl7(log_id, mensagem, conn_epimed):
    print(f"Enviando HL7: {mensagem}")
    return "AA"

# ---------------------------
# Rotina principal com transações
# ---------------------------
def processar_novas_internacoes_e_exames():
    registrar_log("Iniciando rotina de processamento de internações, admissões e exames.")

    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        internacoes_epimed = obter_internacoes_epimed(conn_epimed)
        internacoes_aghu = obter_internacoes_aghu(conn_aghu)

        for codigo, dados_internacao in internacoes_aghu.items():
            prontuario, dthrinternacao, ind_alta = dados_internacao

            with conn_epimed:  # Transação
                if codigo in internacoes_epimed:
                    int_id, _, _, _ = internacoes_epimed[codigo]
                else:
                    int_id = inserir_internacao_epimed(conn_epimed, codigo, prontuario, dthrinternacao, ind_alta)
                    internacoes_epimed[codigo] = (int_id, prontuario, dthrinternacao, ind_alta)

                admissoes_aghu = obter_admissoes_aghu(conn_aghu, codigo)
                admissoes_epimed = obter_admissoes_epimed(conn_epimed, int_id)

                for adm_key, adm_dados in admissoes_aghu.items():
                    unitcode, bedcode, dthrentradaleito = adm_dados
                    if adm_key in admissoes_epimed:
                        adm_id = admissoes_epimed[adm_key]
                    else:
                        adm_id = inserir_admissao_epimed(conn_epimed, int_id, unitcode, bedcode, dthrentradaleito)
                        admissoes_epimed[adm_key] = adm_id

                    exames_aghu = obter_exames_aghu(conn_aghu, prontuario)
                    exames_epimed = obter_exames_epimed(conn_epimed, adm_id)

                    for ex_key, ex_dados in exames_aghu.items():
                        idexame, dthrexame = ex_key
                        if (idexame, dthrexame) in exames_epimed:
                            continue

                        descricao_usual, valor, tipo_inf_valor, result_sigla_exa, \
                        result_material_exa_cod, ind_anulacao_laudo = ex_dados

                        mensagem = gerar_mensagem_hl7_exame(prontuario, idexame, dthrexame, valor, descricao_usual)
                        resposta = enviar_mensagem_hl7(None, mensagem, conn_epimed)

                        if resposta != "AA":
                            raise Exception(f"Falha ao enviar HL7 do exame {idexame} do prontuario {prontuario}")

                        # Insere exame somente após HL7 enviado com sucesso
                        inserir_exame_epimed(conn_epimed, adm_id, idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
                                             result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo)

        registrar_log("Rotina de processamento concluída com sucesso!")

    except Exception as e:
        registrar_log(f"Erro na rotina: {str(e)}", nivel="error")
        conn_epimed.rollback()  # Rollback em caso de erro
    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões com os bancos encerradas.")


#-----------------------------------------------------------------------------------------------#
# Main                                                                                          #
#                                                                                               #
# Informa somente leitos novos ativos                                                           #
# Recupera sempre as datas mais recentes de alterações de status dos leitos                     #
#                                                                                               #
#-----------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    processar_internacoes_e_exames()
#-----------------------------------------------------------------------------------------------#