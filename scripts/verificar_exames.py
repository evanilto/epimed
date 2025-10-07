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
def obter_internacoes_epimed(conn):
    """Obtém todas as internações existentes na base Epimed."""
    internacoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                hospitaladmissionnumber,
                medicalrecord,
                hospitaladmissiondate,
                medicaldischarge
            FROM public.internacoes;
        """)
        for row in cur.fetchall():
            internacoes.append({
                "hospitaladmissionnumber": row[0],
                "medicalrecord": row[1],
                "hospitaladmissiondate": row[2],
                "medicaldischarge": row[3]
            })
    return internacoes


def obter_internacoes_aghu(conn):
    """Obtém todas as internações da view vw_epimed (AGHU)."""
    internacoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                hospitaladmissionnumber,
                medicalrecord,
                hospitaladmissiondate,
                CASE 
                    WHEN medicaldischargedate IS NOT NULL THEN 'S'
                    ELSE 'N'
                END AS medicaldischarge
            FROM vw_epimed;
        """)
        for row in cur.fetchall():
            internacoes.append({
                "hospitaladmissionnumber": row[0],
                "medicalrecord": row[1],
                "hospitaladmissiondate": row[2],
                "medicaldischarge": row[3]
            })
    return internacoes


def obter_admissoes_epimed(conn):
    """Obtém todas as admissões cadastradas na base Epimed."""
    admissoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                id,
                hospitaladmissionnumber,
                unitcode,
                bedcode,
                unitadmissiondatetime
            FROM public.admissoes;
        """)
        for row in cur.fetchall():
            admissoes.append({
                "id": row[0],
                "hospitaladmissionnumber": row[1],
                "unitcode": row[2],
                "bedcode": row[3],
                "unitadmissiondatetime": row[4]
            })
    return admissoes


def obter_admissoes_aghu(conn):
    admissoes = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                hospitaladmissionnumber,
                unitcode,
                bedcode,
                unitadmissiondatetime
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
    """Obtém todos os exames cadastrados na base Epimed."""
    exames = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                adm_id,
                idexame,
                dthrexame,
                descricao_usual,
                valor,
                tipo_inf_valor,
                result_sigla_exa,
                result_material_exa_cod,
                ind_anulacao_laudo
            FROM public.exames;
        """)
        for row in cur.fetchall():
            exames.append({
                "adm_id": row[0],
                "idexame": row[1],
                "dthrexame": row[2],
                "descricao_usual": row[3],
                "valor": row[4],
                "tipo_inf_valor": row[5],
                "result_sigla_exa": row[6],
                "result_material_exa_cod": row[7],
                "ind_anulacao_laudo": row[8]
            })
    return exames


def obter_exames_aghu(conn):
    """Obtém apenas os exames dentro do intervalo de ±4h da admissão."""
    exames = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                a.id AS adm_id,
                a.hospitaladmissionnumber,
                i.medicalrecord,
                ve.campo_laudo_nome AS idexame,
                ve.descricao_usual,
                ve.are_valor AS valor,
                ve.tipo_inf_valor,
                ve.result_sigla_exa,
                ve.result_material_exa_cod,
                ve.ind_anulacao_laudo,
                ve.criado_em AS dthrexame
            FROM admissoes a
            JOIN internacoes i 
                ON i.hospitaladmissionnumber = a.hospitaladmissionnumber
            JOIN vw_exames ve 
                ON ve.prontuario::varchar = i.medicalrecord
               AND ve.criado_em BETWEEN a.unitadmissiondatetime - INTERVAL '4 hours'
                                   AND a.unitadmissiondatetime + INTERVAL '3 hours'
            WHERE ve.ind_anulacao_laudo <> 'S';
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


# =========================================================
# INSERIR UMA INTERNAÇÃO
# =========================================================
def inserir_internacoes(conn, internacoes):
    if not internacoes:
        registrar_log("Nenhuma nova internação para inserir.")
        return 0

    count = 0
    for i in internacoes:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.internacoes (
                        hospitaladmissionnumber,
                        medicalrecord,
                        hospitaladmissiondate,
                        medicaldischarge
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hospitaladmissionnumber) DO NOTHING;
                """, (
                    i["hospitaladmissionnumber"],
                    i["medicalrecord"],
                    i["hospitaladmissiondate"],
                    i["medicaldischarge"]
                ))
            conn.commit()
            count += 1
            registrar_log(f"Internação {i['hospitaladmissionnumber']} inserida com sucesso.")
        except Exception as e:
            conn.rollback()
            registrar_log(f"Erro ao inserir internação {i['hospitaladmissionnumber']}: {e}", nivel="error")
    return count


# =========================================================
# INSERIR UMA ADMISSÃO
# =========================================================
def inserir_admissoes(conn, admissoes):
    if not admissoes:
        registrar_log("Nenhuma nova admissão para inserir.")
        return 0

    count = 0
    for a in admissoes:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.admissoes (
                        hospitaladmissionnumber,
                        unitcode,
                        bedcode,
                        unitadmissiondatetime
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime) DO NOTHING;
                """, (
                    a["hospitaladmissionnumber"],
                    a["unitcode"],
                    a["bedcode"],
                    a["unitadmissiondatetime"]
                ))
            conn.commit()
            count += 1
            registrar_log(f"Admissão da internação {a['hospitaladmissionnumber']} inserida com sucesso.")
        except Exception as e:
            conn.rollback()
            registrar_log(f"Erro ao inserir admissão da internação {a['hospitaladmissionnumber']}: {e}", nivel="error")
    return count


# =========================================================
# INSERIR UM EXAME
# =========================================================
def inserir_exame(conn, exame):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.exames (
                    adm_id,
                    idexame,
                    dthrexame,
                    descricao_usual,
                    valor,
                    tipo_inf_valor,
                    result_sigla_exa,
                    result_material_exa_cod,
                    ind_anulacao_laudo
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (adm_id, idexame, dthrexame) DO NOTHING;
            """, (
                exame["adm_id"],
                exame["idexame"],
                exame["dthrexame"],
                exame["descricao_usual"],
                exame["valor"],
                exame["tipo_inf_valor"],
                exame["result_sigla_exa"],
                exame["result_material_exa_cod"],
                exame["ind_anulacao_laudo"]
            ))
        conn.commit()
        registrar_log(f"Exame {exame['idexame']} da admissão {exame['adm_id']} inserido com sucesso.")
        return True
    except Exception as e:
        conn.rollback()
        registrar_log(f"Erro ao inserir exame {exame['idexame']} da admissão {exame['adm_id']}: {e}", nivel="error")
        return False

# =====================================================================
# ROTINA PRINCIPAL
# =====================================================================
def verificar_e_enviar_exames():
    registrar_log("INICIANDO ROTINA DE VERIFICAÇÃO DE INTERNAÇÕES, ADMISSÕES E EXAMES.")

    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    try:
        # === 1. OBTER DADOS ===
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

        # === 2. INTERNACOES NOVAS ===
        chaves_internacoes_epimed = {
            (i["medicalrecord"], i["hospitaladmissionnumber"], i["hospitaladmissiondate"])
            for i in internacoes_epimed
        }

        novas_internacoes = [
            i for i in internacoes_aghu
            if (i["medicalrecord"], i["hospitaladmissionnumber"], i["hospitaladmissiondate"])
            not in chaves_internacoes_epimed
        ]

        registrar_log(f"Novas internações detectadas: {len(novas_internacoes)}")

        # === 3. ADMISSOES NOVAS ===
        chaves_admissoes_epimed = {
            (a["hospitaladmissionnumber"], a["unitcode"], a["bedcode"], a["unitadmissiondatetime"])
            for a in admissoes_epimed
        }

        novas_admissoes = [
            a for a in admissoes_aghu
            if (a["hospitaladmissionnumber"], a["unitcode"], a["bedcode"], a["unitadmissiondatetime"])
            not in chaves_admissoes_epimed
        ]

        registrar_log(f"Novas admissões detectadas: {len(novas_admissoes)}")

        # === 4. EXAMES NOVOS ===
        chaves_exames_epimed = {
            (e["adm_id"], e["idexame"], e["dthrexame"])
            for e in exames_epimed
        }

        novas_exames = [
            e for e in exames_aghu
            if (e["adm_id"], e["idexame"], e["dthrexame"]) not in chaves_exames_epimed
        ]

        registrar_log(f"Novos exames detectados: {len(novas_exames)}")

        # === 5. INSERÇÕES ===
        # 5.1 Inserir internações novas
        inserir_internacoes(conn_epimed, novas_internacoes)

        # 5.2 Inserir admissões novas
        inserir_admissoes(conn_epimed, novas_admissoes)

        # 5.3 Enviar HL7 e inserir exames
        for e in novas_exames:
            try:
                mensagem = gerar_mensagem_hl7(e)
                ack = enviar_mensagem_hl7(mensagem)

                if ack == "AA":
                    sucesso = inserir_exame(conn_epimed, e)  # um exame por vez
                    if sucesso:
                        registrar_log(f"Exame {e['idexame']} inserido com sucesso (ACK=AA).")
                    else:
                        registrar_log(f"Falha ao inserir exame {e['idexame']} mesmo com ACK=AA.", nivel="error")
                else:
                    registrar_log(f"Exame {e['idexame']} falhou no envio (ACK={ack}).", nivel="error")

            except Exception as erro:
                conn_epimed.rollback()  # desfaz apenas o que estava pendente
                registrar_log(f"Erro ao processar exame {e['idexame']}: {erro}", nivel="error")

        registrar_log("Rotina concluída com sucesso ✅")

    except Exception as e:
        conn_epimed.rollback()
        registrar_log(f"Erro durante sincronização: {str(e)}", nivel="error")
        raise

    finally:
        conn_epimed.close()
        conn_aghu.close()
        registrar_log("CONEXÕES ENCERRADAS.")


# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    verificar_e_enviar_exames()
