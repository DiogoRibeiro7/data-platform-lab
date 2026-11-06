FROM python:3.11.18-slim AS dependencies

# Keep the build tool isolated from the application's locked dependencies.
RUN python -m venv /opt/poetry \
    && /opt/poetry/bin/pip install --no-cache-dir poetry==2.5.1 \
    && python -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /app
COPY python/pyproject.toml python/poetry.lock python/README.md ./
RUN /opt/poetry/bin/poetry check --lock \
    && /opt/poetry/bin/poetry sync --only main --extras "analytics mongodb" \
        --no-root --no-interaction --no-ansi
COPY python/src ./src
RUN /opt/poetry/bin/poetry install --only-root --no-interaction --no-ansi

FROM python:3.11.18-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=dependencies /opt/venv /opt/venv
COPY --from=dependencies /app /app
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENTRYPOINT ["python", "-m", "data_platform_lab.ons_pay_demo"]
