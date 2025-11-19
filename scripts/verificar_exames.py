import os
import psycopg2
import requests
import logging
import traceback
import xml.etree.ElementTree as ET
from psycopg2.extras import RealDictCursor
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

logger = logging.getLogger("exames_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)

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
                INSERT INTO exa.log_envio_hl7 (exame_id, data_envio, status)
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
                UPDATE exa.log_exames_hl7
                SET mensagem = %s, resposta = %s, status = %s
                WHERE id = %s
                """,
                (mensagem, resposta, 'enviado', log_id)
            )
        registrar_log(f"Log de resposta atualizado para id {log_id}.", nivel="info")
    except Exception as e:
        registrar_log(f"Erro ao atualizar log de resposta para id {log_id}: {e}", nivel="error")
        raise
  
def obter_data_ultimo_processamento(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT MAX(data_inicio) 
                        FROM exa.controle_processamento ctrl
                        WHERE status = 'SUCESSO';""")
        res = cur.fetchone()
        return res[0] if res and res[0] else datetime(2025, 1, 1)

def registrar_inicio_processamento(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO exa.controle_processamento (data_inicio, status) VALUES (%s, %s) RETURNING id;",
            (datetime.now(), 'EM_EXECUCAO')
        )
        pid = cur.fetchone()[0]
    conn.commit()
    return pid

def registrar_fim_processamento(conn, pid, status):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE exa.controle_processamento SET data_fim = %s, status = %s WHERE id = %s;",
            (datetime.now(), status, pid)
        )
    conn.commit()

def registrar_auditoria(conn, novas_internacoes, novas_admissoes, novos_exames, duracao, status, mensagem=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO exa.log_execucoes 
            (data_execucao, novas_internacoes, novas_admissoes, novos_exames, duracao, status, mensagem)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (datetime.now(), novas_internacoes, novas_admissoes, novos_exames, duracao, status, mensagem))
    conn.commit()
    registrar_log(f"Log de auditoria registrado: {status}")

def obter_internacoes_baselocal(conn, data_referencia=None):
    internacoes = []
    with conn.cursor() as cur:
        if data_referencia:
            cur.execute("""
                SELECT hospitaladmissionnumber,
                 medicalrecord,
                 hospitaladmissiondate,
                 medicaldischargedate
                FROM exa.internacoes
                WHERE hospitaladmissiondate >= %s;
            """, (data_referencia,))
        else:
            cur.execute("""
                SELECT hospitaladmissionnumber,
                medicalrecord,
                hospitaladmissiondate,
                medicaldischargedate
                FROM exa.internacoes;""")
        for row in cur.fetchall():
            internacoes.append({
                "hospitaladmissionnumber": row[0],
                "medicalrecord": row[1],
                "hospitaladmissiondate": row[2],
                "medicaldischargedate": row[3]
            })
    return internacoes

def obter_internacoes_aghu(conn, data_referencia=None):
    """Obtém internações do AGHU (via view no banco Epimed), filtrando por data se informado."""
    internacoes = []
    with conn.cursor() as cur:
        if data_referencia:
            cur.execute("""
                SELECT 
                    medicalrecord,
                    hospitaladmissionnumber,
                    hospitaladmissiondate,
                    medicaldischargedate
                FROM public.vw_epimed
                WHERE hospitaladmissiondate >= %s
                AND medicaldischargedate is null;
            """, (data_referencia,))
        else:
            cur.execute("""
                SELECT 
                    medicalrecord,
                    hospitaladmissionnumber,
                    hospitaladmissiondate,
                    medicaldischargedate
                FROM public.vw_epimed;
            """)

        for row in cur.fetchall():
            internacoes.append({
                "medicalrecord": row[0],
                "hospitaladmissionnumber": row[1],
                "hospitaladmissiondate": row[2],
                "medicaldischargedate": row[3]

            })
    return internacoes

def obter_admissoes_baselocal(conn, data_referencia=None):
    admissoes = []
    with conn.cursor() as cur:
        if data_referencia:
            cur.execute("""
                SELECT id, hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime
                FROM exa.admissoes
                WHERE unitadmissiondatetime >= %s;
            """, (data_referencia,))
        else:
            cur.execute("SELECT id, hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime FROM exa.admissoes;")
        for row in cur.fetchall():
            admissoes.append({
                "id": row[0],
                "hospitaladmissionnumber": row[1],
                "unitcode": row[2],
                "bedcode": row[3],
                "unitadmissiondatetime": row[4]
            })
    return admissoes

def obter_admissoes_aghu(conn, data_referencia=None):
    """Obtém admissões do AGHU (via view no banco Epimed), filtrando por data."""
    admissoes = []
    with conn.cursor() as cur:
        if data_referencia:
            cur.execute("""
                SELECT 
                    hospitaladmissionnumber,
                    unitcode,
                    bedcode,
                    unitadmissiondatetime
                FROM public.vw_epimed
                WHERE unitadmissiondatetime >= %s;
            """, (data_referencia,))
        else:
            cur.execute("""
                SELECT 
                    hospitaladmissionnumber,
                    unitcode,
                    bedcode,
                    unitadmissiondatetime
                FROM public.vw_epimed;
            """)

        for row in cur.fetchall():
            admissoes.append({
                "hospitaladmissionnumber": row[0],
                "unitcode": row[1],
                "bedcode": row[2],
                "unitadmissiondatetime": row[3]
            })
    return admissoes

def obter_exames_baselocal(conn, data_referencia=None):
    exames = []
    with conn.cursor() as cur:
        if data_referencia:
            cur.execute("""
                SELECT adm_id, idexame, dthrcoleta, nome_exame, valor, tipo_inf_valor,
                       result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo
                FROM exa.exames
                WHERE dthrcoleta >= %s;
            """, (data_referencia,))
        else:
            cur.execute("""
                SELECT adm_id, idexame, dthrcoleta, nome_exame, valor, tipo_inf_valor,
                       result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo
                FROM exa.exames;
            """)
        for row in cur.fetchall():
            exames.append({
                "adm_id": row[0],
                "idexame": row[1],
                "dthrcoleta": row[2],
                "nome_exame": row[3],
                "valor": row[4],
                "tipo_inf_valor": row[5],
                "result_sigla_exa": row[6],
                "result_material_exa_cod": row[7],
                "ind_anulacao_laudo": row[8]
            })
    return exames

def obter_exames_aghu(conn, data_referencia=None):
    """Obtém exames dentro do intervalo de ±4h da última admissão de cada internação."""

    exames = []
    with conn.cursor() as cur:

        if data_referencia:
            cur.execute("""
                WITH ultima_admissao AS (
                    SELECT DISTINCT ON (hospitaladmissionnumber)
                           id,
                           hospitaladmissionnumber,
                           unitcode,
                           unitadmissiondatetime
                    FROM exa.admissoes
                    ORDER BY hospitaladmissionnumber, unitadmissiondatetime DESC
                )
                SELECT 
                    a.id AS adm_id,
                    a.hospitaladmissionnumber,
                    ve.prontuario,
                    ve.ise_soe_seq AS soe_seq,
                    ve.sigla AS idexame,
                    ve.descricao_usual AS nome_exame,
                    ve.are_valor AS valor,
                    ve.tipo_inf_valor,
                    ve.unidade,
                    ve.result_sigla_exa,
                    ve.result_material_exa_cod,
                    ve.ind_anulacao_laudo,
                    ve.dthr_programada,
                    ve.dthr_liberacao
                FROM exa.internacoes i
                JOIN ultima_admissao a 
                    ON a.hospitaladmissionnumber = i.hospitaladmissionnumber
                JOIN exa.vw_exames ve 
                    ON ve.prontuario = i.medicalrecord
                   AND ve.dthr_programada BETWEEN a.unitadmissiondatetime - INTERVAL '4 hours'
                                       AND a.unitadmissiondatetime + INTERVAL '24 hours'
                WHERE ve.ind_anulacao_laudo <> 'S'
                  AND ve.dthr_programada >= %s
                ORDER BY ve.dthr_programada;
            """, (data_referencia,))
        
        else:
            cur.execute("""
                WITH ultima_admissao AS (
                    SELECT DISTINCT ON (hospitaladmissionnumber)
                           id,
                           hospitaladmissionnumber,
                           unitcode,
                           unitadmissiondatetime
                    FROM exa.admissoes
                    ORDER BY hospitaladmissionnumber, unitadmissiondatetime DESC
                )
                SELECT 
                    a.id AS adm_id,
                    a.hospitaladmissionnumber,
                    ve.prontuario,
                    ve.ise_soe_seq AS soe_seq,
                    ve.sigla AS idexame,
                    ve.descricao_usual AS nome_exame,
                    ve.are_valor AS valor,
                    ve.tipo_inf_valor,
                    ve.unidade,
                    ve.result_sigla_exa,
                    ve.result_material_exa_cod,
                    ve.ind_anulacao_laudo,
                    ve.dthr_programada,
                    ve.dthr_liberacao
                FROM exa.internacoes i
                JOIN ultima_admissao a 
                    ON a.hospitaladmissionnumber = i.hospitaladmissionnumber
                JOIN exa.vw_exames ve 
                    ON ve.prontuario::varchar = i.medicalrecord
                   AND ve.dthr_programada BETWEEN a.unitadmissiondatetime - INTERVAL '4 hours'
                                       AND a.unitadmissiondatetime + INTERVAL '3 hours'
                WHERE ve.ind_anulacao_laudo <> 'S'
                ORDER BY ve.dthr_programada;
            """)

        for row in cur.fetchall():
            exames.append({
                "adm_id": row[0],
                "hospitaladmissionnumber": row[1],
                "medicalrecord": row[2],
                "soe_seq": row[3],
                "idexame": row[4],
                "nome_exame": row[5],
                "valor": row[6],
                "tipo_inf_valor": row[7],
                "unidade": row[8],
                "result_sigla_exa": row[9],
                "result_material_exa_cod": row[10],
                "ind_anulacao_laudo": row[11],
                "dthrcoleta": row[12]
            })

    return exames

def gerar_mensagem_hl7(exame):
    """Gera mensagem HL7 simulada"""
    #return f"HL7|{exame['medicalrecord']}|{exame['idexame']}|{exame['dthrexame']}"

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    msh = f"MSH|^~&|HUAP||EPIMED||{timestamp}||ORU^R01|20190409220503_ORU_65870_95936689|P|2.5|||||BR|ASCII"
    pid = f"PID|1|{exame['medicalrecord']}|1235||^Integração HL7 Brasil||19910408000000|M|"
    pv1 = f"PV1|1||||||||||||||||||{exame['hospitaladmissionnumber']}||||||||||||||||||||||||||"
    orc = f"ORC|1|{exame['soe_seq']}|||||||||||||||||"
    obr = f"OBR|1|||||||||||||||||||||||||||"
    obx = f"OBX|1|NM|{exame['idexame']}^{exame['nome_exame']}||{exame['tipo_inf_valor']}|{exame['unidade']}||||||||{exame['dthrcoleta']}"

    return f"{msh}\n{pid}\n{pv1}\n{obr}\n{obx}"

def enviar_mensagem_hl7(mensagem):
    """Simula envio de mensagem HL7"""
    print(f"Enviando HL7: {mensagem}")
    return "AA"  # sucesso simulado

def inserir_internacoes(conn, internacoes):
    if not internacoes:
        registrar_log("Nenhuma nova internação para inserir.")
        return 0
    count = 0
    for i in internacoes:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exa.internacoes (
                    hospitaladmissionnumber,
                    medicalrecord,
                    hospitaladmissiondate,
                    medicaldischargedate,
                    criado_em
                ) VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (hospitaladmissionnumber) DO NOTHING;
            """, (i["hospitaladmissionnumber"], i["medicalrecord"], i["hospitaladmissiondate"], i["medicaldischargedate"]))
        conn.commit()
        count += 1
        
    return count

def inserir_admissoes(conn, admissoes):
    if not admissoes:
        registrar_log("Nenhuma nova admissão para inserir.")
        return 0
    count = 0
    for a in admissoes:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exa.admissoes (
                    hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime, criado_em
                ) VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (hospitaladmissionnumber, unitcode, bedcode, unitadmissiondatetime) DO NOTHING;
            """, (a["hospitaladmissionnumber"], a["unitcode"], a["bedcode"], a["unitadmissiondatetime"]))
        conn.commit()
        count += 1
       
    return count

def inserir_exame(conn, exame):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO exa.exames (
                adm_id, medicalrecord, idexame, dthrcoleta, nome_exame, valor, tipo_inf_valor,
                result_sigla_exa, result_material_exa_cod, ind_anulacao_laudo, criado_em
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (adm_id, idexame, dthrcoleta) DO NOTHING;
        """, (exame["adm_id"], exame["medicalrecord"], exame["idexame"], exame["dthrcoleta"], exame["nome_exame"],
                exame["valor"], exame["tipo_inf_valor"], exame["result_sigla_exa"],
                exame["result_material_exa_cod"], exame["ind_anulacao_laudo"]))
    conn.commit()
    return True

# =====================================================================
# ROTINA PRINCIPAL
# =====================================================================
def verificar_e_enviar_exames():
    registrar_log("INICIANDO ROTINA DE VERIFICAÇÃO DE INTERNAÇÕES, ADMISSÕES E EXAMES.")

    inicio_total = datetime.now()
    conn_epimed = conectar_db(EPIMED_DB_CONFIG)
    conn_aghu = conectar_db(AGHU_DB_CONFIG)

    qnt_internacoes = 0
    qnt_admissoes = 0
    qnt_exames = 0
    status_execucao = 'SUCESSO'
    mensagem_execucao = None

    # === CONTROLE DE PROCESSAMENTO ===
    registrar_log("Iniciando rotina de sincronização…")

    ultima_data = obter_data_ultimo_processamento(conn_epimed)
    registrar_log(f"Último processamento bem-sucedido em: {ultima_data}")

    id_proc = registrar_inicio_processamento(conn_epimed)
    data_inicio = datetime.now()
    registrar_log(f"Novo processamento iniciado às: {data_inicio}")

    try:
        # === ETAPA 1: COLETA DE DADOS ===
        registrar_log("=== ETAPA 1 — COLETA DE DADOS ===")
        registrar_log(f"Obtendo dados atualizados desde {ultima_data}")

        registrar_log("Buscando internações Epimed…")
        internacoes_epimed = obter_internacoes_baselocal(conn_epimed, ultima_data)
        registrar_log(f"Internações Epimed obtidas: {len(internacoes_epimed)}")

        registrar_log("Buscando internações AGHU…")
        internacoes_aghu = obter_internacoes_aghu(conn_epimed, ultima_data)
        registrar_log(f"Internações AGHU obtidas: {len(internacoes_aghu)}")

        registrar_log("Buscando admissões Epimed…")
        admissoes_epimed = obter_admissoes_baselocal(conn_epimed, ultima_data)
        registrar_log(f"Admissões Epimed obtidas: {len(admissoes_epimed)}")

        registrar_log("Buscando admissões AGHU…")
        admissoes_aghu = obter_admissoes_aghu(conn_epimed, ultima_data)
        registrar_log(f"Admissões AGHU obtidas: {len(admissoes_aghu)}")

        registrar_log("Buscando exames Epimed…")
        exames_epimed = obter_exames_baselocal(conn_epimed, ultima_data)
        registrar_log(f"Exames Epimed obtidos: {len(exames_epimed)}")

        registrar_log("Buscando exames AGHU…")
        exames_aghu = obter_exames_aghu(conn_epimed, ultima_data)
        registrar_log(f"Exames AGHU obtidos: {len(exames_aghu)}")

        # === ETAPA 2: INTERNACOES NOVAS ===
        registrar_log("=== ETAPA 2 — INTERNACOES NOVAS ===")

        chaves_internacoes_epimed = {
            (i["medicalrecord"], i["hospitaladmissionnumber"])
            for i in internacoes_epimed
        }

        novas_internacoes = [
            i for i in internacoes_aghu
            if (i["medicalrecord"], i["hospitaladmissionnumber"])
            not in chaves_internacoes_epimed
        ]

        registrar_log(
            f"Novas internações detectadas: {len(novas_internacoes)}"
        )

        # === ETAPA 3: ADMISSOES NOVAS ===
        registrar_log("=== ETAPA 3 — ADMISSÕES NOVAS ===")

        chaves_admissoes_epimed = {
            (
                a["hospitaladmissionnumber"],
                a["unitcode"],
                a["bedcode"],
                a["unitadmissiondatetime"].replace(tzinfo=None, microsecond=0)
            )
            for a in admissoes_epimed
        }

        novas_admissoes = [
            a for a in admissoes_aghu
            if (
                a["hospitaladmissionnumber"],
                a["unitcode"],
                a["bedcode"],
                a["unitadmissiondatetime"].replace(tzinfo=None, microsecond=0)
            ) not in chaves_admissoes_epimed
        ]

        registrar_log(
            f"Novas admissões detectadas: {len(novas_admissoes)}"
        )

        # === ETAPA 4: EXAMES NOVOS ===
        registrar_log("=== ETAPA 4 — EXAMES NOVOS ===")

        chaves_exames_epimed = {
            (e["adm_id"], e["idexame"], e["dthrcoleta"].replace(tzinfo=None, microsecond=0))
            for e in exames_epimed
        }

        novos_exames = [
            e for e in exames_aghu
            if (e["adm_id"], e["idexame"], e["dthrcoleta"].replace(tzinfo=None, microsecond=0))
            not in chaves_exames_epimed
        ]

        registrar_log(
            f"Novos exames detectados: {len(novos_exames)}"
        )

        # === ETAPA 5: INSERÇÕES ===
        registrar_log("=== ETAPA 5 — INSERÇÕES ===")

        with conn_epimed:

            # --- INTERNACOES ---
            if novas_internacoes:
                registrar_log(f"Inserindo {len(novas_internacoes)} novas internações…")
                inserir_internacoes(conn_epimed, novas_internacoes)
                registrar_log("Internações inseridas com sucesso.")
            else:
                registrar_log("Nenhuma nova internação para inserir.")

            # --- ADMISSOES ---
            if novas_admissoes:
                registrar_log(f"Inserindo {len(novas_admissoes)} novas admissões…")
                inserir_admissoes(conn_epimed, novas_admissoes)
                registrar_log("Admissões inseridas com sucesso.")
            else:
                registrar_log("Nenhuma nova admissão para inserir.")

            # --- EXAMES ---
            if novos_exames:
                registrar_log(f"Processando {len(novos_exames)} novos exames…")

                for e in novos_exames:
                    try:
                        registrar_log(f"Gerando HL7 para exame {e['idexame']}…")
                        mensagem = gerar_mensagem_hl7(e)

                        registrar_log("Enviando HL7…")
                        ack = enviar_mensagem_hl7(mensagem)

                        if ack == "AA":
                            registrar_log(f"ACK=AA recebido. Inserindo exame {e['idexame']}…")
                            inserir_exame(conn_epimed, e)

                        else:
                            registrar_log(
                                f"Exame {e['idexame']} rejeitado (ACK={ack}).",
                                nivel="error"
                            )

                    except Exception as erro:
                        registrar_log(
                            f"Erro ao processar exame {e['idexame']}: {erro}",
                            nivel="error"
                        )
                        raise

                registrar_log("Todos os exames processados.")
            else:
                registrar_log("Nenhum novo exame para inserir.")

        # === FINALIZAÇÃO ===
        registrar_fim_processamento(conn_epimed, id_proc, "SUCESSO")
        registrar_log("Rotina concluída com sucesso ✔️")

    except Exception as e:
        conn_epimed.rollback()
        status_execucao = 'ERRO'
        mensagem_execucao = str(e)
        registrar_log(f"Erro durante sincronização: {mensagem_execucao}", nivel="error")

        if id_proc is not None:
            registrar_fim_processamento(conn_epimed, id_proc, 'ERRO')

    finally:
        duracao_total = datetime.now() - inicio_total

        # log de auditoria (mesmo que ocorra erro)
        try:
            with conn_epimed.cursor() as cur:
                cur.execute("""
                    INSERT INTO exa.log_execucoes 
                    (data_execucao, novas_internacoes, novas_admissoes, novos_exames, duracao, status, mensagem)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (datetime.now(), qnt_internacoes, qnt_admissoes, qnt_exames, duracao_total, status_execucao, mensagem_execucao))
            conn_epimed.commit()
            registrar_log("Log de auditoria registrado com sucesso.")
        except Exception as erro_auditoria:
            conn_epimed.rollback()
            registrar_log(f"Erro ao registrar log de auditoria: {erro_auditoria}", nivel="error")

        conn_epimed.close()
        conn_aghu.close()
        registrar_log("CONEXÕES ENCERRADAS.")

# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    verificar_e_enviar_exames()