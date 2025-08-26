#!/bin/bash

# Diretórios base
BASE_DIR="/var/www/html/epimed"
LOG_DIR="$BASE_DIR/logs"
SESSION_DIR="$BASE_DIR/session"
BACKUP_DIR="$BASE_DIR/backup"
DATE=$(date +"%Y-%m-%d")
BACKUP_FILE="$BACKUP_DIR/backup_log_$DATE.tar.gz"
LOG_FILE="$BACKUP_DIR/log_backup_$DATE.txt"

# Garante que o diretório de backup existe
mkdir -p "$BACKUP_DIR"

# Função de log
log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1" | tee -a "$LOG_FILE"
}

log "==== Início do backup de logs ===="

# Compacta apenas o conteúdo do diretório de logs, sem incluir o diretório backup
log "Compactando arquivos de $LOG_DIR em $BACKUP_FILE..."
tar -czvf "$BACKUP_FILE" -C "$LOG_DIR" . >> "$LOG_FILE" 2>&1

if [ $? -eq 0 ]; then
    log "Arquivos compactados com sucesso em $BACKUP_FILE."

    # Remove arquivos de log (exceto o diretório backup, que já está fora)
    log "Removendo arquivos de $LOG_DIR..."
    rm -rf "$LOG_DIR"/*

    if [ $? -eq 0 ]; then
        log "Arquivos removidos com sucesso de $LOG_DIR."
    else
        log "Erro ao remover os arquivos do diretório de origem."
    fi
else
    log "Erro ao compactar os arquivos. Verifique permissões ou diretório inexistente."
fi

# Remove arquivos de sessão
log "Removendo arquivos de sessão em $SESSION_DIR..."
find "$SESSION_DIR" -mindepth 1 -delete

if [ $? -eq 0 ]; then
    log "Arquivos de sessão removidos com sucesso."
else
    log "Erro ao remover os arquivos de sessão."
fi

log "==== Fim do backup de logs ===="
