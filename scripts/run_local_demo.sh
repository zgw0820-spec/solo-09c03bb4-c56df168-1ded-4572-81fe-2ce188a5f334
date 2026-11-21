#!/usr/bin/env bash
# Local end-to-end demo without Docker: SQLite + uvicorn + seeded incident.
# Requires the project virtualenv at .venv (see README.md).
set -euo pipefail

cd "$(dirname "$0")/.."
DB="$(pwd)/demo.db"
rm -f "$DB"
export DATABASE_URL="sqlite:///$DB"

.venv/bin/python -m scripts.seed
echo
echo "启动 Web 服务： http://127.0.0.1:8000  （Ctrl+C 退出）"
exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
