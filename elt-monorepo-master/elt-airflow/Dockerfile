# Use the official Airflow image
FROM apache/airflow:2.9.1-python3.10

USER root

# Install SQL Server ODBC driver
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list \
    > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    unixodbc-dev \
    gcc \
    g++ \
    && apt-get clean -y

# Optional: Oracle Instant Client (UNCOMMENT if using oracledb)
# RUN apt-get install -y libaio1

USER airflow

# Install Python dependencies (as airflow user)
COPY --chown=airflow:airflow airflow_requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt