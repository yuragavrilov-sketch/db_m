"""initial schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-03-12 00:00:00

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None

config_type = postgresql.ENUM(
    "source_db", "target_db", "kafka", "kafka_connect",
    name="config_type", create_type=False,
)
job_type = postgresql.ENUM(
    "connection_test", "schema_compare_table", "schema_compare_all",
    name="job_type", create_type=False,
)
job_status = postgresql.ENUM(
    "queued", "running", "success", "failed", "cancelled",
    name="job_status", create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    config_type.create(bind, checkfirst=True)
    job_type.create(bind, checkfirst=True)
    job_status.create(bind, checkfirst=True)

    op.create_table(
        "config_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("config_type", config_type, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("settings_encrypted", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("config_type", "name", name="uq_config_profiles_type_name"),
    )
    op.create_index("ix_config_profiles_type_enabled", "config_profiles", ["config_type", "is_enabled"])

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", job_type, nullable=False),
        sa.Column("status", job_status, nullable=False, server_default=sa.text("'queued'::job_status")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("progress_log", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("requested_by", sa.Text(), nullable=False, server_default=sa.text("'web'")),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_jobs_status_priority_requested", "jobs", ["status", "priority", "requested_at"])
    op.create_index("ix_jobs_type_requested", "jobs", ["job_type", sa.text("requested_at desc")])
    op.create_index("ix_jobs_payload_gin", "jobs", ["payload"], postgresql_using="gin")

    op.create_table(
        "active_configs",
        sa.Column("config_type", config_type, nullable=False),
        sa.Column("profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("selected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("selected_by", sa.Text(), nullable=False, server_default=sa.text("'system'")),
        sa.ForeignKeyConstraint(["profile_id"], ["config_profiles.id"]),
        sa.PrimaryKeyConstraint("config_type"),
    )

    op.create_table(
        "schema_mappings",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_schema", sa.Text(), nullable=False, server_default=sa.text("'public'")),
        sa.Column("source_table", sa.Text(), nullable=False),
        sa.Column("target_schema", sa.Text(), nullable=True),
        sa.Column("target_table", sa.Text(), nullable=True),
        sa.Column("compare_status", sa.Text(), nullable=False, server_default=sa.text("'unknown'")),
        sa.Column("diff_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_compared_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["last_job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_schema", "source_table", name="uq_schema_mappings_source_table"),
    )
    op.create_index("ix_schema_mappings_compare_status", "schema_mappings", ["compare_status"])


def downgrade() -> None:
    op.drop_index("ix_schema_mappings_compare_status", table_name="schema_mappings")
    op.drop_table("schema_mappings")
    op.drop_table("active_configs")
    op.drop_index("ix_jobs_payload_gin", table_name="jobs")
    op.drop_index("ix_jobs_type_requested", table_name="jobs")
    op.drop_index("ix_jobs_status_priority_requested", table_name="jobs")
    op.drop_table("jobs")
    op.drop_index("ix_config_profiles_type_enabled", table_name="config_profiles")
    op.drop_table("config_profiles")

    bind = op.get_bind()
    job_status.drop(bind, checkfirst=True)
    job_type.drop(bind, checkfirst=True)
    config_type.drop(bind, checkfirst=True)
