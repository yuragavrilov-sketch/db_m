from __future__ import annotations

import logging
import os
import socket
import threading
from datetime import datetime, timedelta, timezone
from uuid import UUID

from flask import Flask
from sqlalchemy import select

from app.application.services.job_service import job_service
from app.application.services.schema_compare_service import schema_compare_service
from app.domain.enums import JobStatus, JobType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import Job

logger = logging.getLogger(__name__)

_COMPARE_JOB_TYPES = (JobType.SCHEMA_COMPARE_TABLE, JobType.SCHEMA_COMPARE_ALL)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class WorkerPoller:
    """Polls the database for queued table-comparison jobs and processes them.

    Supports running multiple worker instances concurrently — jobs are claimed
    atomically via SELECT FOR UPDATE SKIP LOCKED so each job is handled by
    exactly one worker.
    """

    def __init__(
        self,
        poll_interval: float | None = None,
        stale_lock_seconds: int | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._poll_interval = poll_interval or float(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "5"))
        self._stale_lock_seconds = stale_lock_seconds or int(os.getenv("WORKER_STALE_LOCK_SECONDS", "300"))
        self._worker_id = worker_id or os.getenv("WORKER_ID") or socket.gethostname()
        self._stop_event = threading.Event()
        self._started = False
        self._lock = threading.Lock()

    def start(self, app: Flask) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(
            target=self._run,
            args=(app,),
            daemon=True,
            name="db-m-worker-poller",
        ).start()
        logger.info(
            "WorkerPoller started (worker_id=%s, poll_interval=%.1fs, stale_lock=%ds)",
            self._worker_id,
            self._poll_interval,
            self._stale_lock_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    def _run(self, app: Flask) -> None:
        with app.app_context():
            while not self._stop_event.is_set():
                try:
                    self._release_stale_locks()
                    self._poll_and_process()
                except Exception:
                    logger.exception("Unexpected error in worker poller loop")
                    db.session.rollback()
                self._stop_event.wait(self._poll_interval)

    def _poll_and_process(self) -> None:
        job = self._claim_next_job()
        if job is not None:
            self._process_job(job)

    # ------------------------------------------------------------------
    # Job claiming
    # ------------------------------------------------------------------

    def _claim_next_job(self) -> Job | None:
        """Atomically claim one QUEUED comparison job, return None if none available."""
        stmt = (
            select(Job)
            .where(Job.status == JobStatus.QUEUED)
            .where(Job.job_type.in_(_COMPARE_JOB_TYPES))
            .where(Job.locked_by.is_(None))
            .order_by(Job.priority, Job.requested_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = db.session.execute(stmt).scalar_one_or_none()
        if job is None:
            return None
        job.locked_by = self._worker_id
        job.locked_at = _utcnow()
        db.session.commit()
        logger.debug("Claimed job %s (type=%s)", job.id, job.job_type.value)
        return job

    def _release_stale_locks(self) -> None:
        """Re-queue jobs whose lock is older than stale_lock_seconds (worker likely crashed)."""
        cutoff = _utcnow() - timedelta(seconds=self._stale_lock_seconds)
        stale = Job.query.filter(
            Job.status == JobStatus.RUNNING,
            Job.locked_by.isnot(None),
            Job.locked_at < cutoff,
        ).all()
        for job in stale:
            logger.warning("Releasing stale lock on job %s (locked_by=%s)", job.id, job.locked_by)
            job.status = JobStatus.QUEUED
            job.locked_by = None
            job.locked_at = None
            log = list(job.progress_log or [])
            log.append(
                {
                    "ts": _utcnow().isoformat(),
                    "level": "warning",
                    "code": "stale_lock_released",
                    "message": "Stale lock released, job re-queued",
                    "meta": {},
                }
            )
            job.progress_log = log
        if stale:
            db.session.commit()

    # ------------------------------------------------------------------
    # Job dispatch
    # ------------------------------------------------------------------

    def _process_job(self, job: Job) -> None:
        try:
            if job.job_type == JobType.SCHEMA_COMPARE_TABLE:
                self._handle_compare_table(job)
            elif job.job_type == JobType.SCHEMA_COMPARE_ALL:
                self._handle_compare_all(job)
        except Exception as exc:
            db.session.rollback()
            job = db.session.get(Job, job.id)
            if job:
                job.status = JobStatus.FAILED
                job.error_text = str(exc)
                job.finished_at = _utcnow()
                job.locked_by = None
                job.locked_at = None
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
                    changed_fields=["status", "error_text", "finished_at", "progress_log"],
                )
            logger.exception("Job %s failed with unexpected error", job.id if job else "unknown")

    # ------------------------------------------------------------------
    # Status helpers
    # ------------------------------------------------------------------

    def _mark_running(self, job: Job, message: str) -> None:
        job.status = JobStatus.RUNNING
        job.started_at = _utcnow()
        job.progress_log = list(job.progress_log or []) + [
            {
                "ts": _utcnow().isoformat(),
                "level": "info",
                "code": "job_started",
                "message": message,
                "meta": {"worker_id": self._worker_id},
            }
        ]
        db.session.commit()
        job_service.publish_job_updated(job, changed_fields=["status", "started_at", "progress_log"])

    def _finish(self, job: Job, ok: bool, message: str, meta: dict | None = None) -> None:
        job.finished_at = _utcnow()
        job.status = JobStatus.SUCCESS if ok else JobStatus.FAILED
        job.locked_by = None
        job.locked_at = None
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
            job,
            changed_fields=["status", "finished_at", "result", "error_text", "progress_log"],
        )

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_compare_table(self, job: Job) -> None:
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
        self._finish(
            job,
            ok=ok,
            message="Schema compare table finished",
            meta={"ok": ok, "status": compare_status},
        )

    def _handle_compare_all(self, job: Job) -> None:
        self._mark_running(job, "Schema compare all started")

        listing = schema_compare_service.list_mappings(status_filter="all")
        mapping_ids = [UUID(item["id"]) for item in listing.get("items", [])]

        enqueued: list[str] = []
        for mapping_id in mapping_ids:
            table_job = schema_compare_service.enqueue_compare_table(mapping_id, requested_by=job.requested_by)
            if not table_job:
                continue
            enqueued.append(str(table_job.id))
            # Sub-jobs are persisted in DB; the poller will pick them up in subsequent cycles.

        job = db.session.get(Job, job.id)
        if not job:
            return
        job.result = {"enqueued_table_jobs": enqueued, "count": len(enqueued)}
        self._finish(
            job,
            ok=True,
            message="Schema compare all finished",
            meta={"enqueued": len(enqueued)},
        )


worker_poller = WorkerPoller()
