FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    libssl-dev \
    libgeos-dev \
    libproj-dev \
    && pip install --upgrade pip

    RUN apt-get update && apt-get install -y libgdal-dev gdal-bin nodejs npm && apt-get clean
    ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
    ENV C_INCLUDE_PATH=/usr/include/gdal
    RUN pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
    
# Install Poetry
RUN pip install poetry

# Set up the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Disable Poetry's virtualenvs and install dependencies
RUN poetry config virtualenvs.create false && poetry install

RUN pip install tqdm

# Run the gdi command
CMD ["gdi"]