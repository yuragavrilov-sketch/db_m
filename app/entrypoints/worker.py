import logging

from alembic import command
from alembic.config import Config
from flask import Flask

from app.application.services.worker_poller import worker_poller
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ActiveConfig, ConfigProfile, Job, SchemaMapping
from app.shared.settings import Settings

logger = logging.getLogger(__name__)


def create_worker_app() -> Flask:
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

    def _alembic_cfg() -> Config:
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", app.config["SQLALCHEMY_DATABASE_URI"])
        return cfg

    with app.app_context():
        _ = (ConfigProfile, ActiveConfig, Job, SchemaMapping)
        command.upgrade(_alembic_cfg(), "head")

    worker_poller.start(app)
    logger.info("Worker app ready")
    return app
