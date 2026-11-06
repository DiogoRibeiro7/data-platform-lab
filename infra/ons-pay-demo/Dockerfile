FROM python:3.11.18-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY python/pyproject.toml python/README.md ./
COPY python/src ./src
RUN python -m pip install --no-cache-dir uv \
    && uv pip install --system --no-cache '.[analytics,mongodb]'
ENTRYPOINT ["python", "-m", "data_platform_lab.ons_pay_demo"]
