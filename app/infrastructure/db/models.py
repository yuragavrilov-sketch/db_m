from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.types import JSON

from app.domain.enums import ConfigType, JobStatus, JobType
from app.infrastructure.db.extensions import db


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _json():
    return JSONB().with_variant(JSON(), "sqlite")


def _enum_values(enum_cls):
    return [item.value for item in enum_cls]


class ConfigProfile(db.Model):
    __tablename__ = "config_profiles"

    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    config_type = db.Column(
        db.Enum(ConfigType, name="config_type", values_callable=_enum_values, validate_strings=True),
        nullable=False,
    )
    name = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text, nullable=True)
    settings_encrypted = db.Column(_json(), nullable=False, default=dict)
    is_enabled = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        UniqueConstraint("config_type", "name", name="uq_config_profiles_type_name"),
        Index("ix_config_profiles_type_enabled", "config_type", "is_enabled"),
    )


class ActiveConfig(db.Model):
    __tablename__ = "active_configs"

    config_type = db.Column(
        db.Enum(ConfigType, name="config_type", values_callable=_enum_values, validate_strings=True),
        primary_key=True,
    )
    profile_id = db.Column(UUID(as_uuid=True), db.ForeignKey("config_profiles.id"), nullable=False)
    selected_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    selected_by = db.Column(db.Text, nullable=False, default="system")


class Job(db.Model):
    __tablename__ = "jobs"

    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type = db.Column(
        db.Enum(JobType, name="job_type", values_callable=_enum_values, validate_strings=True),
        nullable=False,
    )
    status = db.Column(
        db.Enum(JobStatus, name="job_status", values_callable=_enum_values, validate_strings=True),
        nullable=False,
        default=JobStatus.QUEUED,
    )
    priority = db.Column(db.Integer, nullable=False, default=100)
    payload = db.Column(_json(), nullable=False, default=dict)
    result = db.Column(_json(), nullable=True)
    error_text = db.Column(db.Text, nullable=True)
    progress_log = db.Column(_json(), nullable=False, default=list)
    requested_by = db.Column(db.Text, nullable=False, default="web")
    requested_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    started_at = db.Column(db.DateTime(timezone=True), nullable=True)
    finished_at = db.Column(db.DateTime(timezone=True), nullable=True)
    locked_by = db.Column(db.Text, nullable=True)
    locked_at = db.Column(db.DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_jobs_status_priority_requested", "status", "priority", "requested_at"),
        Index("ix_jobs_type_requested", "job_type", db.text("requested_at desc")),
        Index("ix_jobs_payload_gin", "payload", postgresql_using="gin"),
    )


class SchemaMapping(db.Model):
    __tablename__ = "schema_mappings"

    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_schema = db.Column(db.Text, nullable=False, default="public")
    source_table = db.Column(db.Text, nullable=False)
    target_schema = db.Column(db.Text, nullable=True)
    target_table = db.Column(db.Text, nullable=True)
    compare_status = db.Column(db.Text, nullable=False, default="unknown")
    diff_summary = db.Column(_json(), nullable=True)
    last_compared_at = db.Column(db.DateTime(timezone=True), nullable=True)
    last_job_id = db.Column(UUID(as_uuid=True), db.ForeignKey("jobs.id"), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        UniqueConstraint("source_schema", "source_table", name="uq_schema_mappings_source_table"),
        Index("ix_schema_mappings_compare_status", "compare_status"),
    )
