FROM ghcr.io/binkhq/python:3.11
ARG PIP_INDEX_URL
ARG APP_NAME
ARG APP_VERSION
ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
WORKDIR /app
ADD alembic.ini admin/wsgi.py /app/
ADD alembic /app/alembic/
RUN pip install --no-cache ${APP_NAME}==$(echo ${APP_VERSION} | cut -c 2-)
