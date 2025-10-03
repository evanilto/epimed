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

def conectar_db(config):
    return psycopg2.connect(**config)

def registrar_log(msg, nivel="info"):
    print(f"[{nivel.upper()}] {datetime.now()} - {msg}")

# ----------------------------
# ROTINAS AUXILIARES
# ----------------------------

def obter_internacoes_aghu(conn_aghu):
    """Retorna todas internações da view AGHU (vw_epimed)"""
    internacoes = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT codigo, medicalrecord, hospitaladmissiondate, ind_alta
            FROM vw_epimed;
        """)
        for codigo, prontuario, dthrinternacao, ind_alta in cur.fetchall():
            internacoes[(prontuario, dthrinternacao)] = (codigo, ind_alta)
    return internacoes

def obter_internacoes_epimed(conn_epimed):
    """Internações já cadastradas na base Epimed"""
    internacoes = {}
    with conn_epimed.cursor() as cur:
        cur.execute("SELECT id, codigo, prontuario, dthrinternacao FROM internacoes;")
        for int_id, codigo, prontuario, dthrinternacao in cur.fetchall():
            internacoes[(prontuario, dthrinternacao)] = int_id
    return internacoes

def obter_admissoes_aghu(conn_aghu, prontuario, dthrinternacao):
    """Admissões de uma internação específica na AGHU"""
    admissoes = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT unitcode, bedcode, unitadmissiondatetime
            FROM vw_epimed
            WHERE medicalrecord=%s AND hospitaladmissiondate=%s;
        """, (prontuario, dthrinternacao))
        for unitcode, bedcode, dthrentradaleito in cur.fetchall():
            admissoes[(unitcode, bedcode, dthrentradaleito)] = None
    return admissoes

def obter_admissoes_epimed(conn_epimed, int_id):
    """Admissões cadastradas para uma internação"""
    admissoes = {}
    with conn_epimed.cursor() as cur:
        cur.execute("""
            SELECT id, unitcode, bedcode, dthrentradaleito
            FROM admissoes
            WHERE int_id=%s;
        """, (int_id,))
        for adm_id, unitcode, bedcode, dthrentradaleito in cur.fetchall():
            admissoes[(unitcode, bedcode, dthrentradaleito)] = adm_id
    return admissoes

def obter_exames_aghu(conn_aghu, prontuario, dthrentradaleito):
    """Exames AGHU para determinada admissão"""
    exames = {}
    with conn_aghu.cursor() as cur:
        cur.execute("""
            SELECT campo_laudo_nome, criado_em, descricao_usual, are_valor, tipo_inf_valor,
                   result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo
            FROM vw_exames
            WHERE prontuario=%s
              AND criado_em BETWEEN %s - interval '4 hours' AND %s + interval '3 hours';
        """, (prontuario, dthrentradaleito, dthrentradaleito))
        for row in cur.fetchall():
            idexame, dthrexame = row[0], row[1]
            exames[(idexame, dthrexame)] = row
    return exames

# ----------------------------
# ROTINAS DE INSERÇÃO
# ----------------------------

def inserir_internacao_epimed(conn_epimed, codigo, prontuario, dthrinternacao, ind_alta):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO internacoes (codigo, prontuario, dthrinternacao, ind_alta)
            VALUES (%s, %s, %s, %s) RETURNING id;
        """, (codigo, prontuario, dthrinternacao, ind_alta))
        return cur.fetchone()[0]

def inserir_admissao_epimed(conn_epimed, int_id, unitcode, bedcode, dthrentradaleito):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO admissoes (int_id, unitcode, bedcode, dthrentradaleito)
            VALUES (%s, %s, %s, %s) RETURNING id;
        """, (int_id, unitcode, bedcode, dthrentradaleito))
        return cur.fetchone()[0]

def inserir_exame_epimed(conn_epimed, adm_id, exame):
    with conn_epimed.cursor() as cur:
        cur.execute("""
            INSERT INTO exames (adm_id, idexame, dthrexame, descricao_usual, valor, tipo_inf_valor,
                                result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (adm_id, exame[0], exame[1], exame[2], exame[3], exame[4], exame[5], exame[6], exame[7]))

# ----------------------------
# ROTINA PRINCIPAL
# ----------------------------

def verificar_admissoes_leitos():
    registrar_log("Iniciando rotina de internações, admissões e exames.")
    
    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)
    
    try:
        # Obter internações
        internacoes_epimed = obter_internacoes_epimed(conn_epimed)
        internacoes_aghu = obter_internacoes_aghu(conn_aghu)
        
        # Novas internações
        novas_internacoes = {k:v for k,v in internacoes_aghu.items() if k not in internacoes_epimed}
        todas_internacoes = internacoes_epimed.copy()  # incluir internações existentes

        # Processar novas internações
        for (prontuario, dthrinternacao), (codigo, ind_alta) in novas_internacoes.items():
            log_id = salvar_log_envio(prontuario, conn_epimed)
            msg_hl7 = gerar_mensagem_hl7(prontuario)
            ack = enviar_mensagem_hl7(log_id, msg_hl7, conn_epimed)
            if ack == "AA":
                int_id = inserir_internacao_epimed(conn_epimed, codigo, prontuario, dthrinternacao, ind_alta)
                todas_internacoes[(prontuario, dthrinternacao)] = int_id
                registrar_log(f"Internação {prontuario} inserida com sucesso!")
            else:
                registrar_log(f"Falha ao enviar HL7 da internação {prontuario}", nivel="error")
        
        # Processar admissões e exames para todas internações (novas ou existentes)
        for (prontuario, dthrinternacao), int_id in todas_internacoes.items():
            admissoes_epimed = obter_admissoes_epimed(conn_epimed, int_id)
            admissoes_aghu = obter_admissoes_aghu(conn_aghu, prontuario, dthrinternacao)
            
            # Novas admissões
            for (unitcode, bedcode, dthrentradaleito) in admissoes_aghu.keys():
                if (unitcode, bedcode, dthrentradaleito) not in admissoes_epimed:
                    msg_hl7_adm = gerar_mensagem_hl7(prontuario, unitcode, bedcode)
                    ack_adm = enviar_mensagem_hl7(int_id, msg_hl7_adm, conn_epimed)
                    if ack_adm == "AA":
                        adm_id = inserir_admissao_epimed(conn_epimed, int_id, unitcode, bedcode, dthrentradaleito)
                        registrar_log(f"Admissão {unitcode}-{bedcode} inserida com sucesso!")
                        admissoes_epimed[(unitcode, bedcode, dthrentradaleito)] = adm_id
                    else:
                        registrar_log(f"Falha ao enviar HL7 da admissão {unitcode}-{bedcode}", nivel="error")
            
            # Processar exames para todas admissões
            for (unitcode, bedcode, dthrentradaleito), adm_id in admissoes_epimed.items():
                exames = obter_exames_aghu(conn_aghu, prontuario, dthrentradaleito)
                for (idexame, dthrexame), exame in exames.items():
                    with conn_epimed.cursor() as cur:
                        cur.execute("""
                            SELECT id FROM exames WHERE adm_id=%s AND idexame=%s AND dthrexame=%s;
                        """, (adm_id, idexame, dthrexame))
                        if cur.fetchone():
                            continue  # já existe

                    msg_hl7_ex = gerar_mensagem_hl7(prontuario, unitcode, bedcode, exame)
                    ack_ex = enviar_mensagem_hl7(int_id, msg_hl7_ex, conn_epimed)
                    if ack_ex == "AA":
                        inserir_exame_epimed(conn_epimed, adm_id, exame)
                        registrar_log(f"Exame {idexame} inserido com sucesso!")
                    else:
                        registrar_log(f"Falha ao enviar HL7 do exame {idexame}", nivel="error")

        registrar_log("Rotina concluída com sucesso!")

    except Exception as e:
        registrar_log(f"Erro na rotina: {str(e)}", nivel="error")
    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("Conexões encerradas.")

#-----------------------------------------------------------------------------------------------#
# Main                                                                                          #
#                                                                                               #
# Informa somente leitos novos ativos                                                           #
# Recupera sempre as datas mais recentes de alterações de status dos leitos                     #
#                                                                                               #
#-----------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    verificar_admissoes_leitos()
#-----------------------------------------------------------------------------------------------#