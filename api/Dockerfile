# archivo: api/Dockerfile
# El contexto de construcción es la RAÍZ del repo (ver docker-compose.yml) para copiar common/.
FROM python:3.11-slim
# Java: pyspark lo necesita para cargar el modelo en /predict
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ /app/
COPY common/ /app/common/
# Hash del commit desplegado (lo inyecta Jenkins); /health lo devuelve. Va al final para no romper la caché.
ARG GIT_COMMIT=desconocido
ENV GIT_COMMIT=$GIT_COMMIT
EXPOSE 5000
# 1 proceso con 4 hilos: el modelo de Spark se carga una sola vez
CMD ["gunicorn", "-w", "1", "--threads", "4", "--timeout", "180", "-b", "0.0.0.0:5000", "app:app"]
