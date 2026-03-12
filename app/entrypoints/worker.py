import logging
import logging.handlers
import os
import sys

from flask import Flask

from app.application.services.worker_poller import worker_poller
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ActiveConfig, ConfigProfile, Job, SchemaMapping
from app.shared.settings import Settings

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s (%(threadName)s): %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _setup_logging() -> None:
    """Configure root logger for worker process.

    Env vars:
      LOG_LEVEL   — logging level, e.g. DEBUG / INFO / WARNING (default: INFO)
      LOG_FILE    — path to a rotating log file; stdout only if not set
      LOG_MAX_BYTES  — max size per log file in bytes (default: 10 MB)
      LOG_BACKUP_COUNT — number of rotated files to keep (default: 5)
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    handlers: list[logging.Handler] = []

    # stdout handler — always present
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    # optional rotating file handler
    log_file = os.getenv("LOG_FILE")
    if log_file:
        max_bytes = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    root = logging.getLogger()
    root.setLevel(level)
    # Remove any handlers added before (e.g. by Flask/Werkzeug at import time)
    root.handlers.clear()
    for h in handlers:
        root.addHandler(h)

    # Quiet down noisy third-party loggers unless DEBUG is requested
    if level > logging.DEBUG:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
        logging.getLogger("alembic").setLevel(logging.WARNING)
        logging.getLogger("werkzeug").setLevel(logging.WARNING)


def create_worker_app() -> Flask:
    _setup_logging()

    app = Flask(__name__)
    settings = Settings.from_env()
    app.config.update(settings.to_flask_config())

    db.init_app(app)

    @app.get("/health")
    def health() -> dict:
        return {
            "status": "ok",
            "work_type": "worker",
            "worker_id": worker_poller._worker_id,
        }

    with app.app_context():
        _ = (ConfigProfile, ActiveConfig, Job, SchemaMapping)

    worker_poller.start(app)
    logger.info(
        "Worker app ready (worker_id=%s, db=%s)",
        worker_poller._worker_id,
        settings.database_url.split("@")[-1],  # host/db only, no credentials
    )
    return app
