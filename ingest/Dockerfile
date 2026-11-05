# archivo: ingest/Dockerfile
# El contexto de construcción es la RAÍZ del repo (ver docker-compose.yml) para poder copiar common/ y tests/.
FROM python:3.11-slim-bookworm
WORKDIR /app
# Los workers de Dask importan limpieza.py y common/ desde aquí
ENV PYTHONPATH=/app
# Primero las librerías: Docker reutiliza esta capa si requirements.txt no cambia
COPY ingest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Después el código (cambia seguido, así que va al final)
COPY ingest/ /app/
COPY common/ /app/common/
COPY tests/ /app/tests/
COPY pytest.ini /app/pytest.ini
