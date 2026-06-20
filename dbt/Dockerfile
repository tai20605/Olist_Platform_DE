FROM python:3.11-slim

WORKDIR /dbt

RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY dbt/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN pip install --no-cache-dir \
    dbt-core==1.8.5 \
    dbt-trino==1.8.5

RUN mkdir -p /root/.dbt

COPY dbt /dbt

CMD ["sleep", "infinity"]