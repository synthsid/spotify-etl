FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system packages
USER root

# Install git (and clean up to keep image slim)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python deps as airflow user
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts into container
COPY scripts/ /opt/airflow/scripts/