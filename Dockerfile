FROM ghcr.io/astral-sh/uv:debian

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    python3-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock .python-version* ./

RUN uv sync --frozen --no-dev --no-install-project

COPY . .

RUN mkdir -p data/datasets visualizations logs

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "run.py"]