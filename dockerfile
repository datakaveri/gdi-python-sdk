
FROM python:3.12-slim


# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    libssl-dev \
    libgeos-dev \
    libproj-dev \
    && pip install --upgrade pip


# Install GDAL dependencies
RUN apt-get update && apt-get install -y libgdal-dev g++ --no-install-recommends && \
    apt-get clean -y

    RUN apt --yes install gdal-bin
# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip install gdal==3.6.2
# Install Poetry
RUN pip install poetry

# Set up the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Disable Poetry's virtualenvs and install dependencies
RUN poetry config virtualenvs.create false && poetry lock && poetry install
RUN pip3 install numpy==1.26.4
RUN pip install tqdm



# Run the gdi command
CMD ["gdi"]