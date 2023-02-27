import importlib
import logging
import os

import typer
import uvicorn

from prometheus_client import CollectorRegistry
from prometheus_client import start_http_server as start_prometheus_server
from prometheus_client import values
from prometheus_client.multiprocess import MultiProcessCollector
from retry_tasks_lib.reporting import report_anomalous_tasks, report_queue_lengths, report_tasks_summary
from retry_tasks_lib.utils.error_handler import job_meta_handler
from rq import Worker

from cosmos.accounts.config import account_settings
from cosmos.core.config import redis_raw
from cosmos.core.prometheus import job_queue_summary, task_statuses, tasks_summary
from cosmos.core.scheduled_tasks.balances import reset_balances
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler as scheduler
from cosmos.db.session import SyncSessionMaker
from cosmos.rewards.config import reward_settings
from cosmos.rewards.imports.file_agent import RewardImportAgent, RewardUpdatesAgent
from cosmos.rewards.scheduled_tasks.pending_rewards import process_pending_rewards

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def api(
    mod_name: str,
    port: int = typer.Option(..., "--port", "-p", help="Port to bind to"),
    dev: bool = typer.Option(False, "--reload", "-R", help="Reload (dev mode)"),
) -> None:  # pragma: no cover
    mod = f"cosmos.{mod_name}.api.app"
    try:
        importlib.import_module(mod)
        uvicorn.run(f"{mod}:app", port=port, reload=dev)
    except Exception as exc:
        logger.exception(f"Could not start {mod_name} service", exc_info=exc)
        raise typer.Abort() from exc


@app.command()
def task_worker(burst: bool = False) -> None:  # pragma: no cover

    if reward_settings.core.ACTIVATE_TASKS_METRICS:
        # -------- this is the prometheus monkey patch ------- #
        values.ValueClass = values.MultiProcessValue(os.getppid)
        # ---------------------------------------------------- #
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        logger.info("Starting prometheus metrics server...")
        start_prometheus_server(reward_settings.core.PROMETHEUS_HTTP_SERVER_PORT, registry=registry)

    worker = Worker(
        queues=reward_settings.core.TASK_QUEUES,
        connection=redis_raw,
        log_job_description=True,
        exception_handlers=[job_meta_handler],
    )
    logger.info("Starting task worker...")
    worker.work(burst=burst, with_scheduler=True)


@app.command()
def cron_scheduler(  # noqa: PLR0913
    imports: bool = True,
    updates: bool = True,
    pending_rewards: bool = True,
    balances: bool = True,
    report_tasks: bool = True,
    report_rq_queues: bool = True,
) -> None:  # pragma: no cover

    logger.info("Initialising scheduler...")
    if imports:
        scheduler.add_job(
            RewardImportAgent().do_import,
            schedule_fn=lambda: reward_settings.BLOB_IMPORT_SCHEDULE,
            coalesce_jobs=True,
        )

    if updates:
        scheduler.add_job(
            RewardUpdatesAgent().do_import,
            schedule_fn=lambda: reward_settings.BLOB_IMPORT_SCHEDULE,
            coalesce_jobs=True,
        )

    if balances:
        scheduler.add_job(
            reset_balances,
            schedule_fn=lambda: account_settings.RESET_BALANCES_SCHEDULE,
            coalesce_jobs=True,
        )

    if pending_rewards:
        scheduler.add_job(
            process_pending_rewards,
            schedule_fn=lambda: reward_settings.PENDING_REWARDS_SCHEDULE,
            coalesce_jobs=True,
        )

    if report_tasks:
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        logger.info("Starting prometheus metrics server...")
        start_prometheus_server(reward_settings.core.PROMETHEUS_HTTP_SERVER_PORT, registry=registry)

        scheduler.add_job(
            report_anomalous_tasks,
            kwargs={
                "session_maker": SyncSessionMaker,
                "project_name": reward_settings.core.PROJECT_NAME,
                "gauge": task_statuses,
            },
            schedule_fn=lambda: reward_settings.core.REPORT_ANOMALOUS_TASKS_SCHEDULE,
            coalesce_jobs=True,
        )
        scheduler.add_job(
            report_tasks_summary,
            kwargs={
                "session_maker": SyncSessionMaker,
                "project_name": reward_settings.core.PROJECT_NAME,
                "gauge": tasks_summary,
            },
            schedule_fn=lambda: reward_settings.core.REPORT_TASKS_SUMMARY_SCHEDULE,
            coalesce_jobs=True,
        )

    if report_rq_queues:
        scheduler.add_job(
            report_queue_lengths,
            kwargs={
                "redis": redis_raw,
                "project_name": reward_settings.core.PROJECT_NAME,
                "queue_names": reward_settings.core.TASK_QUEUES,
                "gauge": job_queue_summary,
            },
            schedule_fn=lambda: reward_settings.core.REPORT_JOB_QUEUE_LENGTH_SCHEDULE,
            coalesce_jobs=True,
        )

    logger.info(f"Starting scheduler {cron_scheduler}...")
    scheduler.run()


@app.callback()
def callback() -> None:  # pragma: no cover
    """
    cosmos command line interface
    """


if __name__ == "__main__":
    app()
