FROM apache/airflow:2.8.1-python3.11

USER root

# Cài đặt các công cụ hệ thống cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl netcat-traditional postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --user \
    "apache-airflow-providers-trino" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-apache-kafka" \
    "apache-airflow-providers-postgres" \
    --constraint "${CONSTRAINT_URL}"