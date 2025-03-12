FROM apache/airflow:2.9.2

# Install packages
COPY requirements/*.txt /req/
RUN cd /req && pip install --no-cache-dir -r requirements.txt