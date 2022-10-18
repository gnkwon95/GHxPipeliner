FROM apache/airflow:2.3.0
COPY requirements.txt .
COPY keys/ ./keys/
RUN pip install --no-cache-dir -r requirements.txt
