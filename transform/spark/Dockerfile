FROM apache/spark:4.2.0-java21-python3

USER root

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements-spark.txt \
    && rm /tmp/requirements-spark.txt

USER spark
WORKDIR /workspace
ENV PYTHONPATH=/workspace
