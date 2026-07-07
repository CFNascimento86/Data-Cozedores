FROM apache/airflow:2.9.0-python3.10

USER root

# Dependências ODBC + compilação
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Instalar ODBC Driver 17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

USER airflow

# Dependências Python
RUN pip install --no-cache-dir \
 pyodbc \
 pandas \
 pyarrow \
 boto3 \
 azure-storage-file-datalake \
 minio \
 sqlalchemy
