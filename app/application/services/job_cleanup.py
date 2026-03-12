from __future__ import annotations

import logging
import threading
import time

from flask import Flask

logger = logging.getLogger(__name__)


class JobCleanup:
    def __init__(self) -> None:
        self._started = False
        self._lock = threading.Lock()

    def start(self, app: Flask, interval_seconds: int = 60) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(
            target=self._run,
            args=(app, interval_seconds),
            daemon=True,
            name="db-m-job-cleanup",
        ).start()

    def _run(self, app: Flask, interval_seconds: int) -> None:
        from app.domain.enums import JobStatus
        from app.infrastructure.db.extensions import db
        from app.infrastructure.db.models import Job, SchemaMapping

        with app.app_context():
            while True:
                time.sleep(interval_seconds)
                try:
                    success_ids = [
                        row.id
                        for row in db.session.query(Job.id)
                        .filter(Job.status == JobStatus.SUCCESS)
                        .all()
                    ]
                    if not success_ids:
                        continue

                    # Обнуляем FK в schema_mappings перед удалением
                    db.session.query(SchemaMapping).filter(
                        SchemaMapping.last_job_id.in_(success_ids)
                    ).update({"last_job_id": None}, synchronize_session=False)

                    deleted = (
                        db.session.query(Job)
                        .filter(Job.id.in_(success_ids))
                        .delete(synchronize_session=False)
                    )
                    db.session.commit()
                    if deleted:
                        logger.info("Job cleanup: deleted %d successful job(s)", deleted)
                except Exception:
                    db.session.rollback()
                    logger.exception("Job cleanup failed")


job_cleanup = JobCleanup()
