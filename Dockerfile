FROM apache/airflow:2.7.3-python3.11
USER airflow

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install pyspark==3.5.0 requests apache-airflow-providers-apache-spark lxml python-decouple pandas

# Upgrade HTTP provider (if needed)
RUN pip install --upgrade apache-airflow-providers-http

# Install OpenLineage provider
RUN pip install --upgrade "apache-airflow-providers-openlineage>=1.8.0" --no-cache-dir

# Switch to root to install system packages
USER root

# Install procps (for 'ps' command) and OpenJDK 11
RUN apt-get update && \
    apt-get install -y procps openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow
