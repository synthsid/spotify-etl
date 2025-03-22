FROM apache/airflow:2.8.1-python3.10

USER root
COPY requirements.txt .

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ /opt/airflow/scripts/
