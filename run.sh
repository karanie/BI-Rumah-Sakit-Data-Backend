#!/bin/sh

uv run python main.py init datastore-dbms

uv run python -m uvicorn dummyapi:app --host 0.0.0.0 &
uv run python main.py app flask &
uv run python main.py app fastapi
