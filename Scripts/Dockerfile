FROM python:3.13-slim

WORKDIR /app

# Dependências do ODBC + dependências para compilar python-snap7
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    make \
    libc-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY app/reader_cozedores_snap7_sqlserver.py /app/

CMD ["python", "reader_cozedores_snap7_sqlserver.py"]
