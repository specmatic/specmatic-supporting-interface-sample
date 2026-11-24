FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app

COPY uv.lock pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim-bookworm

ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --system --user-group --create-home nonroot
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY order_service ./order_service

USER nonroot
EXPOSE 8080
CMD ["python", "-m", "order_service"]
