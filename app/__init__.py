import os

from app.entrypoints.web import create_web_app
from app.entrypoints.worker import create_worker_app


def create_app(work_type: str | None = None):
    resolved = (work_type or os.getenv("WORK_TYPE", "web")).lower()
    if resolved == "web":
        return create_web_app()
    if resolved == "worker":
        return create_worker_app()
    raise RuntimeError(f"Unsupported WORK_TYPE '{resolved}'. Supported values: 'web', 'worker'.")
