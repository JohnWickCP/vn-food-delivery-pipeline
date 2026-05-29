FROM apache/airflow:2.8.0-python3.9

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins
