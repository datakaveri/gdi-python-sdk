# syntax=docker/dockerfile:1.7

FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    gdal-bin \
    libffi-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libssl-dev \
    python3-opencv \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install numpy==1.26.4 gdal==3.6.2 poetry==2.0.1

WORKDIR /app

COPY pyproject.toml poetry.lock README.md ./

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install --only main --no-root

COPY . .

RUN poetry install --only-root

CMD ["gdi"]
