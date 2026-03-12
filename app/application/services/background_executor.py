from __future__ import annotations

import queue
import threading
from datetime import datetime, timezone
from uuid import UUID

from flask import Flask

from app.application.services.connection_test_service import connection_test_service
from app.application.services.job_service import job_service
from app.application.services.schema_compare_service import schema_compare_service
from app.domain.enums import JobStatus, JobType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import Job


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class BackgroundExecutor:
    def __init__(self) -> None:
        self._queue: queue.Queue[UUID] = queue.Queue()
        self._started = False
        self._lock = threading.Lock()

    def start(self, app: Flask) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(target=self._run, args=(app,), daemon=True, name="db-m-bg-worker").start()

    def enqueue(self, job_id: UUID) -> None:
        self._queue.put(job_id)

    def _run(self, app: Flask) -> None:
        with app.app_context():
            while True:
                job_id = self._queue.get()
                try:
                    self._handle_job(job_id)
                except Exception as exc:  # noqa: BLE001
                    db.session.rollback()
                    job = db.session.get(Job, job_id)
                    if job:
                        job.status = JobStatus.FAILED
                        job.error_text = str(exc)
                        job.finished_at = _utcnow()
                        log = list(job.progress_log or [])
                        log.append(
                            {
                                "ts": _utcnow().isoformat(),
                                "level": "error",
                                "code": "unexpected_error",
                                "message": str(exc),
                                "meta": {},
                            }
                        )
                        job.progress_log = log
                        db.session.commit()
                        job_service.publish_job_updated(
                            job,
                            changed_fields=["status", "error_text", "finished_at", "progress_log", "result"],
                        )
                finally:
                    self._queue.task_done()

    def _handle_job(self, job_id: UUID) -> None:
        job = db.session.get(Job, job_id)
        if not job:
            return
        if job.job_type == JobType.CONNECTION_TEST:
            self._handle_connection_test(job)
        elif job.job_type == JobType.SCHEMA_COMPARE_TABLE:
            self._handle_schema_compare_table(job)
        elif job.job_type == JobType.SCHEMA_COMPARE_ALL:
            self._handle_schema_compare_all(job)

    def _mark_running(self, job: Job, message: str) -> None:
        job.status = JobStatus.RUNNING
        job.started_at = _utcnow()
        job.progress_log = list(job.progress_log or []) + [
            {"ts": _utcnow().isoformat(), "level": "info", "code": "job_started", "message": message, "meta": {}}
        ]
        db.session.commit()
        job_service.publish_job_updated(job, changed_fields=["status", "started_at", "progress_log"])

    def _finish(self, job: Job, ok: bool, message: str, meta: dict | None = None) -> None:
        job.finished_at = _utcnow()
        job.status = JobStatus.SUCCESS if ok else JobStatus.FAILED
        job.progress_log = list(job.progress_log or []) + [
            {
                "ts": _utcnow().isoformat(),
                "level": "info" if ok else "error",
                "code": "job_finished",
                "message": message,
                "meta": meta or {},
            }
        ]
        db.session.commit()
        job_service.publish_job_updated(
            job, changed_fields=["status", "finished_at", "result", "error_text", "progress_log"]
        )

    def _handle_connection_test(self, job: Job) -> None:
        self._mark_running(job, "Connection test started")
        ok, result, error_text = connection_test_service.run_test(job.payload)
        job = db.session.get(Job, job.id)
        if not job:
            return
        job.result = result
        job.error_text = error_text
        self._finish(job, ok=ok, message="Connection test finished", meta={"ok": ok})

    def _handle_schema_compare_table(self, job: Job) -> None:
        self._mark_running(job, "Schema compare table started")
        mapping_id_raw = (job.payload or {}).get("mapping_id")
        mapping_id = UUID(mapping_id_raw) if mapping_id_raw else None
        if not mapping_id:
            job.error_text = "mapping_id is required"
            job.result = {"error": "missing mapping_id"}
            self._finish(job, ok=False, message="Schema compare table failed", meta={"ok": False})
            return

        ok, result, error_text, compare_status = schema_compare_service.compare_table_now(mapping_id)
        job = db.session.get(Job, job.id)
        if not job:
            return
        job.result = result
        job.error_text = error_text
        schema_compare_service.apply_compare_result(
            mapping_id=mapping_id,
            status=compare_status,
            diff_summary=result.get("diff") if isinstance(result, dict) else None,
            job_id=job.id,
        )
        self._finish(job, ok=ok, message="Schema compare table finished", meta={"ok": ok, "status": compare_status})

    def _handle_schema_compare_all(self, job: Job) -> None:
        self._mark_running(job, "Schema compare all started")
        listing = schema_compare_service.list_mappings(status_filter="all")
        mapping_ids = [UUID(item["id"]) for item in listing.get("items", [])]

        enqueued: list[str] = []
        for mapping_id in mapping_ids:
            table_job = schema_compare_service.enqueue_compare_table(mapping_id, requested_by=job.requested_by)
            if not table_job:
                continue
            enqueued.append(str(table_job.id))
            self.enqueue(table_job.id)

        job = db.session.get(Job, job.id)
        if not job:
            return
        job.result = {"enqueued_table_jobs": enqueued, "count": len(enqueued)}
        self._finish(job, ok=True, message="Schema compare all finished", meta={"enqueued": len(enqueued)})


background_executor = BackgroundExecutor()
