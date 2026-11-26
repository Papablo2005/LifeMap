# archivo: spark/Dockerfile
# Imagen oficial de Apache Spark 3.5.1 (Scala 2.12, Java 17, Ubuntu 20.04 con Python 3.8). El contexto es la RAÍZ del repo.
FROM apache/spark:3.5.1
USER root
# numpy: lo exige MLlib. pymongo: los jobs escriben resultados en MongoDB.
# pandas y pyarrow (Entrega 2): pyspark.ml.functions (vector_to_array, usado por riesgo y streaming) importa pandas.
# Esta imagen trae Python 3.8: numpy 1.24.4, pandas 2.0.3 y pyarrow 14.0.2 son versiones compatibles con 3.8.
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir numpy==1.24.4 pymongo==4.8.0 pandas==2.0.3 pyarrow==14.0.2
ENV PYTHONPATH=/opt/jobs
COPY spark/jobs /opt/jobs
COPY common /opt/jobs/common
RUN chmod -R a+rX /opt/jobs && mkdir -p /data /models
# Se queda como root: así puede escribir en los volúmenes compartidos /data y /models
# (aceptable en un proyecto académico).