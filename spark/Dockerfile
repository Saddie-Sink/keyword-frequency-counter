FROM apache/spark-py:v3.5.1

USER root
WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary==2.9.9 kafka-python-ng==2.2.3

COPY spark/ /app/spark/
ENV PYTHONPATH=/app

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/spark/keyword_streaming.py"]
