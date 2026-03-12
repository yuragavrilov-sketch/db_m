from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import or_

from app.application.services.sse_broker import sse_broker
from app.domain.enums import JobStatus, JobType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import Job


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class JobService:
    def enqueue_connection_test(
        self, config_profile_id: UUID, payload: dict[str, Any], requested_by: str = "web-ui"
    ) -> Job:
        job = Job(
            job_type=JobType.CONNECTION_TEST,
            status=JobStatus.QUEUED,
            payload={"config_profile_id": str(config_profile_id), **payload},
            requested_by=requested_by,
            progress_log=[
                {
                    "ts": _utcnow().isoformat(),
                    "level": "info",
                    "code": "queued",
                    "message": "Connection test queued",
                    "meta": {"config_profile_id": str(config_profile_id)},
                }
            ],
        )
        db.session.add(job)
        db.session.commit()
        self.publish_job_updated(job, changed_fields=["status", "payload", "progress_log", "requested_at"])
        return job

    def list_jobs(
        self,
        status: str | None = None,
        job_type: str | None = None,
        q: str | None = None,
        created_from: str | None = None,
        created_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        query = Job.query
        if status:
            query = query.filter(db.cast(Job.status, db.Text) == JobStatus(status).value)
        if job_type:
            query = query.filter(db.cast(Job.job_type, db.Text) == JobType(job_type).value)

        parsed_from = self._parse_iso_dt(created_from)
        if parsed_from:
            query = query.filter(Job.requested_at >= parsed_from)
        parsed_to = self._parse_iso_dt(created_to)
        if parsed_to:
            query = query.filter(Job.requested_at <= parsed_to)

        if q:
            pattern = f"%{q.strip()}%"
            query = query.filter(
                or_(
                    Job.requested_by.ilike(pattern),
                    Job.error_text.ilike(pattern),
                )
            )

        total = query.count()
        jobs = query.order_by(Job.requested_at.desc()).offset(offset).limit(limit).all()
        return {
            "items": [self.to_dto(job) for job in jobs],
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total,
                "has_more": offset + len(jobs) < total,
            },
            "filters": {
                "status": status,
                "type": job_type,
                "q": q,
                "created_from": created_from,
                "created_to": created_to,
            },
        }

    def get_job(self, job_id: UUID) -> dict[str, Any] | None:
        job = db.session.get(Job, job_id)
        return self.to_dto(job) if job else None

    @staticmethod
    def to_dto(job: Job) -> dict[str, Any]:
        updated_at = job.finished_at or job.started_at or job.requested_at
        return {
            "id": str(job.id),
            "type": job.job_type.value,
            "job_type": job.job_type.value,
            "status": job.status.value,
            "created_at": job.requested_at.isoformat() if job.requested_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
            "priority": job.priority,
            "payload": job.payload,
            "result": job.result,
            "error_text": job.error_text,
            "progress_log": job.progress_log,
            "requested_by": job.requested_by,
            "requested_at": job.requested_at.isoformat() if job.requested_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        }

    @staticmethod
    def _parse_iso_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None

    @staticmethod
    def publish_job_updated(job: Job, changed_fields: list[str] | None = None) -> None:
        sse_broker.publish(
            "job_updated",
            {
                "entity_id": str(job.id),
                "status": job.status.value,
                "job_type": job.job_type.value,
                "changed_fields": changed_fields or ["status", "progress_log"],
            },
        )


job_service = JobService()
