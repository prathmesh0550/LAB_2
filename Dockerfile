FROM apache/airflow:2.10.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir --upgrade pip==24.2 setuptools wheel

RUN pip install --no-cache-dir \
    "typing_extensions>=4.7,<5.0" \
    "pydantic>=1.10,<2.0" \
    "yfinance==0.2.40" \
    "apache-airflow-providers-snowflake==5.4.0" \
    "snowflake-connector-python==3.10.0" \
    "dbt-core==1.8.4" \
    "dbt-snowflake==1.8.4"
