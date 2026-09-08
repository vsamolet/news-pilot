#!/usr/bin/env bash
# Разовый запуск pipeline.py с логированием — предназначен для cron.
# Пример crontab-строки (проверка раз в 15 минут):
#   */15 * * * * /Users/vitaly/news-pilot/run_pipeline.sh >> /Users/vitaly/news-pilot/logs/cron.log 2>&1
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

mkdir -p logs

source .venv/bin/activate
python pipeline.py >> "logs/pipeline_$(date +%F).log" 2>&1
