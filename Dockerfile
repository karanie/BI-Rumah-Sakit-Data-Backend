FROM python:3.13-bookworm AS build
ENV PATH="/root/.local/bin:${PATH}"
WORKDIR /code
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
COPY pyproject.toml uv.lock .
RUN sh -c 'uv sync --extra fastapi --extra psql'
COPY . .
CMD ["./run.sh" ]
