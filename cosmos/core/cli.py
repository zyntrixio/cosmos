import importlib
import logging
import os

import typer
import uvicorn

from prometheus_client import CollectorRegistry
from prometheus_client import start_http_server as start_prometheus_server
from prometheus_client import values
from prometheus_client.multiprocess import MultiProcessCollector
from retry_tasks_lib.utils.error_handler import job_meta_handler
from rq import Worker

from cosmos.core.config import redis_raw, settings

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def api(
    mod_name: str,
    port: int = typer.Option(..., "--port", "-p", help="Port to bind to"),
) -> None:  # pragma: no cover
    mod = f"cosmos.{mod_name}.api.app"
    try:
        importlib.import_module(mod)
        uvicorn.run(f"{mod}:app", port=port, reload=False)
    except Exception as exc:
        print(f"No {mod_name} app found")
        raise typer.Abort() from exc


@app.command()
def task_worker(burst: bool = False) -> None:  # pragma: no cover

    if settings.ACTIVATE_TASKS_METRICS:
        # -------- this is the prometheus monkey patch ------- #
        values.ValueClass = values.MultiProcessValue(os.getppid)
        # ---------------------------------------------------- #
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        logger.info("Starting prometheus metrics server...")
        start_prometheus_server(settings.PROMETHEUS_HTTP_SERVER_PORT, registry=registry)

    worker = Worker(
        queues=settings.TASK_QUEUES,
        connection=redis_raw,
        log_job_description=True,
        exception_handlers=[job_meta_handler],
    )
    logger.info("Starting task worker...")
    worker.work(burst=burst, with_scheduler=True)


@app.callback()
def callback() -> None:  # pragma: no cover
    """
    cosmos command line interface
    """


if __name__ == "__main__":
    app()
