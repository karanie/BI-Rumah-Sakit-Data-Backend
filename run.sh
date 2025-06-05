#!/bin/sh

. .venv/bin/activate
# uvicorn dummyapi:app --host 0.0.0.0 --port 8000 &
# python -u main.py worker api-pollers &

# HTTP API. Flask is deprecated. Please use FastAPI
# uv run python main.py app flask &
python main.py app fastapi
