FROM ghcr.io/astral-sh/uv:bookworm AS build
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1
WORKDIR /code
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock ./
RUN uv sync --extra fastapi --extra psql
COPY . .

FROM build AS production
WORKDIR /code
COPY --from=build /code/ ./
RUN chmod +x ./run.sh
CMD ["./run.sh" ]
