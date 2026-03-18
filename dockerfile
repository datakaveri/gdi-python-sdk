# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ARG GDAL_VERSION=3.8.4
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    GDAL_CONFIG=/usr/local/bin/gdal-config \
    CPLUS_INCLUDE_PATH=/usr/local/include \
    C_INCLUDE_PATH=/usr/local/include \
    LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    libcurl4-openssl-dev \
    libexpat1-dev \
    libffi-dev \
    libgeos-dev \
    libjpeg-dev \
    libopenjp2-7-dev \
    libpng-dev \
    libproj-dev \
    libsqlite3-dev \
    libssl-dev \
    libtiff-dev \
    libwebp-dev \
    libxml2-dev \
    pkg-config \
    proj-bin \
    python3-opencv \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz" -o /tmp/gdal.tar.gz && \
    tar -xzf /tmp/gdal.tar.gz -C /tmp && \
    cd /tmp/gdal-${GDAL_VERSION} && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local && \
    cmake --build build -j"$(nproc)" && \
    cmake --build build --target install && \
    ldconfig && \
    rm -rf /tmp/gdal*

RUN /usr/local/bin/gdal-config --version | grep -Fx "${GDAL_VERSION}"

COPY asset_files/ /tmp/asset_files/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install numpy==1.26.4 poetry==2.0.1 && \
    pip install --no-deps "/tmp/asset_files/GDAL-${GDAL_VERSION}-cp311-cp311-linux_x86_64.whl" && \
    rm -rf /tmp/asset_files

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/root/.cache/pypoetry \
    poetry lock && \
    poetry install --only main --no-root
COPY . .
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    poetry lock && \
    poetry install --only-root
CMD ["gdi"]