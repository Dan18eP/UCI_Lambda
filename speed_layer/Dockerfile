FROM apache/spark:3.5.0

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2

ENV SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2"

COPY stream_processor.py .

USER 185

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "stream_processor.py"]
