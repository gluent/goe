# Standard Library
import logging
from typing import Final, Optional

# Gluent
from gluentlib.listener import services, utils
from gluentlib.listener.config import settings
from gluentlib.listener.schemas.base import deserialize_object, serialize_object
from gluentlib.listener.services import periodic_tasks
from gluentlib_contrib.worker import CronJob, Job, Queue, Status, Worker
from gluentlib_contrib.worker.utils import seconds

logger = logging.getLogger(__name__)

WORKER_ID: Final = f"listener:worker:{services.system.generate_listener_group_id()}"
FUNCTION_ALLOWLIST: Final = (
    periodic_tasks.publish_command_executions,
    periodic_tasks.publish_schemas,
    periodic_tasks.publish_heartbeat,
)
STARTUP_CRON_JOBS: Final = (
    CronJob(
        function=periodic_tasks.publish_command_executions,
        function_kwargs={},
        key="cron:publish-command-executions",
        cron="*/2 * * * *",
        timeout=500,
    ),
    CronJob(
        function=periodic_tasks.publish_schemas,
        function_kwargs={},
        key="cron:publish-schemas",
        cron="0 * * * *",
        timeout=3600,
    ),
)


async def startup(context):
    """Worker Startup"""
    utils.cache.get_client()
    logger.debug("Background worker process started successfully")


async def shutdown(context):
    """Worker Shutdown"""
    await cleanup_queue()
    logger.debug("Background worker node shutdown complete")


async def cleanup_queue():
    worker_id = f"listener:worker:{services.system.generate_listener_group_id()}"
    close_connection_at_completion = False
    if not utils.cache.redis_client:
        utils.cache.get_client()
        close_connection_at_completion = True
    total_keys_deleted: int = 0
    keys = await utils.cache.delete_keys(f"gluent:{worker_id}:incomplete*")
    total_keys_deleted += keys
    keys = await utils.cache.delete_keys(f"gluent:{worker_id}:job:*")
    total_keys_deleted += keys
    keys = await utils.cache.delete_keys(f"gluent:{worker_id}:schedule")
    total_keys_deleted += keys
    # keys = await utils.cache.delete_keys(f"gluent:{worker_id}:stats*")
    # total_keys_deleted += keys
    logger.info("listener cache cleanup completed successfully.")
    if close_connection_at_completion:
        await utils.cache.close_client()


async def before_process(context):
    """Worker Before Processing"""
    job: Optional[Job] = context.get("job", None)
    if job:
        logger.info(f"starting job {job.job_id}")
    context["listener_group_id"] = services.system.generate_listener_group_id()
    context["endpoint_id"] = services.system.generate_listener_endpoint_id()


async def after_process(context):
    """Worker After Processing"""
    job: Optional[Job] = context.get("job", None)
    if job:
        if job.status == Status.FAILED:
            logger.error(
                f"job {job.job_id} FAILED after {job.duration('total')} ms. Reason: [bold]{job.error.error}",
            )
            # logger.debug(f"...job {job.job_id} Error: {job.error.traceback}")
        elif job.status == Status.COMPLETE:
            logger.info(
                f"job {job.job_id} COMPLETED after {seconds(job.duration('total'))} seconds.",
            )
        elif job.status == Status.ABORTED:
            logger.warning(
                f"job {job.job_id} was ABORTED after {seconds(job.duration('total'))} seconds",
            )
        else:
            logger.info(
                f"job {job.job_id} finished with a status of {job.status}"
                f" after {seconds(job.duration('total'))} seconds.",
            )


def get_background_worker() -> Optional[Worker]:
    """This setting will only return a valid worker if the REDIS_HOST variable is set"""

    if settings.cache_enabled:
        redis_client = utils.cache.get_client()
        background_tasks = Queue(
            redis_client,
            name=f"listener:worker:{services.system.generate_listener_group_id()}",
            dump=serialize_object,
            load=deserialize_object,
            max_concurrent_ops=10,
        )
        # background_tasks = Queue.from_url(
        #     url=settings.redis_url,
        #     name=f"listener:worker:{services.system.generate_listener_group_id()}",
        #     dump=serialize_object,
        #     load=deserialize_object,
        #     max_concurrent_ops=4,
        # )
        return Worker(
            queue=background_tasks,
            functions=list(FUNCTION_ALLOWLIST),
            cron_jobs=list(STARTUP_CRON_JOBS),
            concurrency=5,
            startup=startup,
            shutdown=shutdown,
            before_process=before_process,
            after_process=after_process,
        )


background_worker = get_background_worker()
