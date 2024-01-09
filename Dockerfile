# To disable linkerd-await, run with:
# -e LINKERD_AWAIT_DISABLED=true

FROM ghcr.io/binkhq/python:3.11-poetry as build
WORKDIR /src
ADD . .
RUN poetry build

FROM ghcr.io/binkhq/python:3.11 as main
ARG PIP_INDEX_URL
WORKDIR /app
COPY --from=build /src/alembic/ ./alembic/
COPY --from=build /src/alembic.ini .
COPY --from=build /src/dist/*.whl .
# gcc required for hiredis
RUN export wheel=$(find -type f -name "*.whl") && \
    apt update && \
    apt -y install gcc && \
    pip install "$wheel" && \
    rm $wheel && \
    apt -y autoremove gcc
ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "echo", "cosmos" ]

FROM ghcr.io/binkhq/python:3.11 as admin
ARG PIP_INDEX_URL
WORKDIR /app
COPY --from=build /src/dist/*.whl .
COPY --from=build /src/admin/wsgi.py .
RUN export wheel=$(find -type f -name "*.whl") && pip install "$wheel[admin]" && rm "$wheel"
ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "gunicorn", "--error-logfile=-", "--access-logfile=-", "--bind=0.0.0.0:9000", "wsgi:app" ]
