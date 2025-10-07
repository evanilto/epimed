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

# ==========================================
# Funções auxiliares de conexão e logging
# ==========================================
def conectar_db(config):
    return psycopg2.connect(**config)

def registrar_log(mensagem, nivel="info"):
    print(f"[{nivel.upper()}] {datetime.now():%Y-%m-%d %H:%M:%S} - {mensagem}")

    if nivel == "info":
        logger.info(mensagem)
    elif nivel == "error":
        logger.error(mensagem)
    elif nivel == "warning":
        logger.warning(mensagem)
        
def salvar_log_envio(exame_id, conexao):
    try:
        with conexao.cursor() as cursor:
            cursor.execute("""
                INSERT INTO log_envio_exames_hl7 (exame_id, data_envio, status)
                VALUES (%s, NOW(), 'pendente')
                RETURNING id
            """, (exame_id,))
            log_id = cursor.fetchone()[0]

        registrar_log(f"Log de envio criado para o exame {exame_id} com id {log_id}.", nivel="info")

        return log_id

    except Exception as e:
        registrar_log(f"Erro ao salvar log de envio para o exame {exame_id}: {e}", nivel="error")
        raise

def salvar_log_resposta(log_id, mensagem, resposta, conexao):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                UPDATE log_envio_exames_hl7
                SET mensagem = %s, resposta = %s, status = %s
                WHERE id = %s
                """,
                (mensagem, resposta, 'enviado', log_id)
            )

        registrar_log(f"Log de resposta atualizado para id {log_id}.", nivel="info")

    except Exception as e:
        registrar_log(f"Erro ao atualizar log de resposta para id {log_id}: {e}", nivel="error")
        raise
  
# =====================================================================
# FUNÇÕES DE OBTENÇÃO DE DADOS
# =====================================================================
def obter_internacoes_aghu(conn):
    """Obtém todas as internações da view AGHU"""
    internacoes = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                hospitaladmissionnumber,
                medicalrecord,
                hospitaladmissiondate,
                CASE WHEN medicaldischargedate IS NOT NULL THEN 'S' ELSE 'N' END AS medicaldischarge
            FROM vw_epimed;
        """)
        for hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge in cur.fetchall():
            internacoes[(medicalrecord, hospitaladmissionnumber, hospitaladmissiondate)] = (hospitaladmissionnumber, medicaldischarge)
    return internacoes


def obter_internacoes_epimed(conn):
    """Obtém todas as internações já registradas em Epimed"""
    internacoes = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge
            FROM internacoes;
        """)
        for hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge in cur.fetchall():
            internacoes[(medicalrecord, hospitaladmissionnumber, hospitaladmissiondate)] = (hospitaladmissionnumber, medicaldischarge)
    return internacoes


def obter_admissoes_aghu(conn):
    """Obtém admissões do AGHU"""
    admissoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                i.hospitaladmissionnumber,
                i.medicalrecord,
                v.unitcode,
                v.bedcode,
                v.unitadmissiondatetime
            FROM vw_epimed v
            JOIN internacoes i ON i.medicalrecord = v.medicalrecord;
        """)
        for row in cur.fetchall():
            admissoes.append({
                "hospitaladmissionnumber": row[0],
                "medicalrecord": row[1],
                "unitcode": row[2],
                "bedcode": row[3],
                "unitadmissiondatetime": row[4]
            })
    return admissoes


def obter_admissoes_epimed(conn):
    """Obtém admissões já registradas em Epimed"""
    admissoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime
            FROM admissoes;
        """)
        for row in cur.fetchall():
            admissoes.append({
                "hospitaladmissionnumber": row[0],
                "unitcode": row[1],
                "bedcode": row[2],
                "unitadmissiondatetime": row[3]
            })
    return admissoes


def obter_exames_epimed(conn):
    """Obtém exames já inseridos em Epimed"""
    exames = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT adm_id, idexame, dthrexame
            FROM exames;
        """)
        for row in cur.fetchall():
            exames.append({
                "adm_id": row[0],
                "idexame": row[1],
                "dthrexame": row[2]
            })
    return exames


def obter_exames_aghu(conn):
    """Obtém exames dentro do intervalo de ±4h em relação à admissão"""
    exames = []
    with conn.cursor() as cur:
        cur.execute("""
            select
                a.id as adm_id,
                a.hospitaladmissionnumber,
                i.medicalrecord,
                ve.campo_laudo_nome as idexame,
                ve.descricao_usual,
                ve.are_valor as valor,
                ve.tipo_inf_valor,
                ve.result_sigla_exa,
                ve.result_material_exa_cod,
                ve.ind_anulacao_laudo,
                ve.criado_em as dthrexame
            from
                admissoes a
            join internacoes i on i.hospitaladmissionnumber = a.hospitaladmissionnumber
            join vw_exames ve  on CAST(i.medicalrecord AS int) = ve.prontuario
            where
                ve.ind_anulacao_laudo <> 'S'
                AND ve.criado_em BETWEEN (a.unitadmissiondatetime - interval '4 hours')
                                     AND (a.unitadmissiondatetime + interval '3 hours');
        """)
        for row in cur.fetchall():
            exames.append({
                "adm_id": row[0],
                "hospitaladmissionnumber": row[1],
                "medicalrecord": row[2],
                "idexame": row[3],
                "descricao_usual": row[4],
                "valor": row[5],
                "tipo_inf_valor": row[6],
                "result_sigla_exa": row[7],
                "result_material_exa_cod": row[8],
                "ind_anulacao_laudo": row[9],
                "dthrexame": row[10]
            })
    return exames

# =====================================================================
# FUNÇÕES DE INSERÇÃO E ENVIO HL7
# =====================================================================

def gerar_mensagem_hl7(exame):
    """Gera mensagem HL7 simulada"""
    return f"HL7|{exame['medicalrecord']}|{exame['idexame']}|{exame['dthrexame']}"

def enviar_mensagem_hl7(mensagem):
    """Simula envio de mensagem HL7"""
    print(f"Enviando HL7: {mensagem}")
    return "AA"  # sucesso simulado


def inserir_internacao(conn, hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO internacoes (hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (medicalrecord, hospitaladmissionnumber, hospitaladmissiondate) DO NOTHING;
        """, (hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge))


def inserir_admissao(conn, hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO admissoes (hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime) DO NOTHING;
        """, (hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime))


def inserir_exame(conn, exame):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO exames (
                adm_id, idexame, dthrexame, descricao_usual, valor,
                tipo_inf_valor, result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (adm_id, idexame, dthrexame) DO NOTHING;
        """, (
            exame["adm_id"], exame["idexame"], exame["dthrexame"],
            exame["descricao_usual"], exame["valor"], exame["tipo_inf_valor"],
            exame["result_sigla_exa"], exame["result_material_exa_cod"], exame["ind_anulacao_laudo"]
        ))

# =====================================================================
# ROTINA PRINCIPAL
# =====================================================================

def verificar_e_enviar_exames():
    registrar_log("INICIANDO ROTINA DE VERIFICAÇÃO DE INTERNAÇÕES, ADMISSÕES E EXAMES.")

    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        registrar_log("Obtendo internações Epimed!")
        internacoes_epimed = obter_internacoes_epimed(conn_epimed)
        registrar_log("Obtendo internações AGHU!")
        internacoes_aghu = obter_internacoes_aghu(conn_epimed)
        registrar_log("Obtendo admissões Epimed!")
        admissoes_epimed = obter_admissoes_epimed(conn_epimed)
        registrar_log("Obtendo admissões AGHU!")
        admissoes_aghu = obter_admissoes_aghu(conn_epimed)
        registrar_log("Obtendo exames Epimed!")
        exames_epimed = obter_exames_epimed(conn_epimed)
        registrar_log("Obtendo exames AGHU")
        exames_aghu = obter_exames_aghu(conn_epimed)

        novas_internacoes = [k for k in internacoes_aghu.keys() if k not in internacoes_epimed]
        registrar_log(f"Qtd de novas internações: {len(novas_internacoes)}")

        novas_admissoes = [
            a for a in admissoes_aghu
            if not any(
                a["hospitaladmissionnumber"] == e["hospitaladmissionnumber"] and
                a["unitcode"] == e["unitcode"] and 
                a["bedcode"] == e["bedcode"] and 
                a["unitadmissiondatetime"] == e["unitadmissiondatetime"]
                for e in admissoes_epimed
            )
        ]
        registrar_log(f"Qtd de novas admissões: {len(novas_admissoes)}")

        novos_exames = [
            e for e in exames_aghu
            if not any(
                e["adm_id"] == ex["adm_id"] and 
                e["idexame"] == ex["idexame"] and 
                e["dthrexame"] == ex["dthrexame"]
                for ex in exames_epimed
            )
        ]
        registrar_log(f"Qtd de novos exames: {len(novos_exames)}")

        with conn_epimed:
            # novas internações
            for (medicalrecord, hospitaladmissionnumber, hospitaladmissiondate) in novas_internacoes:
                _, medicaldischarge = internacoes_aghu[(medicalrecord, hospitaladmissionnumber, hospitaladmissiondate)]
                inserir_internacao(conn_epimed, hospitaladmissionnumber, medicalrecord, hospitaladmissiondate, medicaldischarge)
                #registrar_log(f"Internação {hospitaladmissionnumber} inserida com sucesso!")

            # novas admissões
            for adm in novas_admissoes:
                inserir_admissao(conn_epimed, adm["hospitaladmissionnumber"], adm["unitcode"], adm["bedcode"], adm["unitadmissiondatetime"])
                #registrar_log(f"Admissão em {adm['bedcode']} inserida com sucesso!")

            # novos exames (com HL7)
            for exame in novos_exames:
                log_id = salvar_log_envio(exame['idexame'], conn_epimed)

                mensagem = gerar_mensagem_hl7(exame)
                resposta = enviar_mensagem_hl7(mensagem)
                if resposta == "AA":
                    inserir_exame(conn_epimed, exame)
                    registrar_log(f"Exame {exame['idexame']} inserido com sucesso!")
                else:
                    raise Exception(f"Erro HL7 para exame {exame['idexame']}")

                salvar_log_resposta(exame['idexame'], mensagem, None, conexao)


        registrar_log("✅ Rotina concluída com sucesso (commit realizado)!")

    except Exception as e:
        conn_epimed.rollback()
        registrar_log(f"❌ Erro: {e}", nivel="error")

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("CONEXÕES ENCERRADAS.")

# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    verificar_e_enviar_exames()
