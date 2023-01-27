
FROM ghcr.io/binkhq/python:3.10-poetry as build

WORKDIR /src
ADD . .
RUN poetry build

FROM ghcr.io/binkhq/python:3.10

ENV PIP_INDEX_URL=https://269fdc63-af3d-4eca-8101-8bddc22d6f14:b694b5b1-f97e-49e4-959e-f3c202e3ab91@pypi.tools.uksouth.bink.sh/simple
WORKDIR /app
ARG wheel=cosmos-*-py3-none-any.whl
COPY --from=build /src/alembic/ ./alembic/
COPY --from=build /src/alembic.ini .
COPY --from=build /src/dist/$wheel .
# gcc required for hiredis
RUN apt update && apt -y install gcc && pip install $wheel && rm $wheel && apt -y autoremove gcc
ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "echo", "cosmos" ]
