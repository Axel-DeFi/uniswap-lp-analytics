#!/usr/bin/env bash
set -euo pipefail

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r backend/requirements.txt

echo "✔ Python env ready. Start API with:"
echo "source .venv/bin/activate && uvicorn backend.app.main:app --reload"
