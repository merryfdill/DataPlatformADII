FROM tabulario/spark-iceberg:latest

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs /home/iceberg/jobs
