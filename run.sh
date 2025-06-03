#!/bin/sh

uv run python -u main.py worker api-pollers &

# HTTP API. Flask is deprecated. Please use FastAPI
# uv run python main.py app flask &
uv run python main.py app fastapi
