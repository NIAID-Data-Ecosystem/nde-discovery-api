import importlib
import logging
import os
import secrets
import time
from functools import partial
from threading import Thread

from backup import daily_backup_routine
from biothings.web.launcher import main
from filelock import FileLock, Timeout
from handlers import WebAppHandler
from tornado.ioloop import IOLoop
from tornado.options import options
from tornado.web import StaticFileHandler

SETTINGS = {
    "default_handler_class": WebAppHandler,
    "static_path": "dist/static",
    "template_path": os.path.dirname(__file__),
}
ROUTES = [
    (r" ^/$", StaticFileHandler, {"path": "dist/static"}),
]


_CRON_JOBS = []
DAILY_BACKUP_CRON = "0 0 * * *"
DAILY_BACKUP_LOCK_FILE = ".es-backup.lock"


def _load_config_module():
    module_name = getattr(options, "conf", None) or "config"
    return importlib.import_module(module_name)


def _run_backup_with_lock(config):
    logger = logging.getLogger("daily_backup")
    jitter_ms = secrets.randbelow(401) + 100
    time.sleep(jitter_ms / 1000)

    try:
        with FileLock(DAILY_BACKUP_LOCK_FILE, timeout=0):
            logger.info("Daily backup lock acquired; starting Elasticsearch backup")
            daily_backup_routine(config)
    except Timeout:
        logger.info("Daily backup lock is held by another process; skipping this run")
    except Exception:
        logger.error("Daily backup scheduler failed", exc_info=True)


def _run_backup_thread(config):
    thread = Thread(target=_run_backup_with_lock, args=(config,), daemon=True)
    thread.start()


def _schedule_daily_backup(config):
    from aiocron import crontab

    loop = IOLoop.instance().asyncio_loop
    _CRON_JOBS.append(
        crontab(
            DAILY_BACKUP_CRON,
            func=partial(_run_backup_thread, config),
            start=True,
            loop=loop,
        )
    )
    logging.getLogger("daily_backup").info(
        "Daily Elasticsearch backup scheduled with cron %s",
        DAILY_BACKUP_CRON,
    )


if __name__ == '__main__':
    options.parse_command_line()
    _schedule_daily_backup(_load_config_module())
    main(ROUTES, SETTINGS)
