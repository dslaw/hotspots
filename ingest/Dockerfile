FROM ghcr.io/astral-sh/uv:0.5-python3.13-alpine

WORKDIR /worker

ENV UV_COMPILE_BYTECODE=1

COPY uv.lock pyproject.toml .
RUN uv sync --frozen --no-install-project --no-dev

COPY src ./src/

ENTRYPOINT ["uv", "run", "--no-dev", "python", "-m", "src.main"]
