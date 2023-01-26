FROM ghcr.io/binkhq/python:3.10-poetry as build

WORKDIR /src
RUN poetry config virtualenvs.create false
RUN poetry self add poetry-dynamic-versioning[plugin]
ADD . .
RUN poetry build

FROM ghcr.io/binkhq/python:3.10

WORKDIR /app
ENV PIP_INDEX_URL=https://269fdc63-af3d-4eca-8101-8bddc22d6f14:b694b5b1-f97e-49e4-959e-f3c202e3ab91@pypi.tools.uksouth.bink.sh/simple
ARG wheel=cosmos-0.0.0-py3-none-any.whl
COPY --from=build /src/dist/$wheel .
COPY --from=build /src/admin/wsgi.py .
RUN pip install "$wheel[admin]" && rm $wheel
ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
CMD [ "gunicorn", "--workers=2", "--threads=2", "--error-logfile=-", \
    "--access-logfile=-", "--bind=0.0.0.0:9000", "wsgi:app" ]
