#!/bin/bash

CMD="my_clash_royale"
LOG_FILE="/mnt/drive/projects/my_clash_royale/app.log"

log_command() {
    local command="$1"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Executing: $1" >> $LOG_FILE 2>&1
    eval $command >> $LOG_FILE 2>&1
}


while true; do
    log_command "$CMD"
    sleep 600
done
