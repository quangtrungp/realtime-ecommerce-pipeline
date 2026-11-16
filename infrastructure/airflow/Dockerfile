FROM apache/airflow:2.10.2-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Thư viện Python cho Airflow DAG
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt

# Cài dbt trong môi trường riêng để không xung đột với Airflow
USER root

RUN python -m venv /opt/dbt/venv \
    && chown -R airflow:0 /opt/dbt

USER airflow

RUN /opt/dbt/venv/bin/pip install --no-cache-dir \
    dbt-bigquery \
    dbt-postgres

USER root

RUN ln -s /opt/dbt/venv/bin/dbt /usr/local/bin/dbt

USER airflow