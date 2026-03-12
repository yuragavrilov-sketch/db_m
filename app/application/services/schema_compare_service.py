from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import create_engine, inspect

_ORACLE_SYSTEM_SCHEMAS = frozenset({
    "SYS", "SYSTEM", "OUTLN", "DBSNMP", "APPQOSSYS", "DBSFWUSER",
    "GGSYS", "ANONYMOUS", "CTXSYS", "DVSYS", "DVF", "GSMADMIN_INTERNAL",
    "MDSYS", "OLAPSYS", "REMOTE_SCHEDULER_AGENT", "XDB", "XS$NULL",
    "LBACSYS", "ORDSYS", "ORDPLUGINS", "ORDDATA", "SI_INFORMTN_SCHEMA", "WMSYS",
})
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.application.services.job_service import job_service
from app.application.services.sse_broker import sse_broker
from app.domain.enums import ConfigType, JobStatus, JobType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ActiveConfig, ConfigProfile, Job, SchemaMapping


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


_VALID_FILTERS = {"all", "same", "different", "missing", "unknown", "diff"}


class SchemaCompareService:
    def list_mappings(self, status_filter: str = "all") -> dict[str, Any]:
        normalized = self._normalize_filter(status_filter)
        self._sync_mappings_from_active_configs()

        query = SchemaMapping.query
        if normalized != "all":
            query = query.filter_by(compare_status=normalized)

        rows = query.order_by(SchemaMapping.source_schema, SchemaMapping.source_table).all()

        summary = {
            "all": SchemaMapping.query.count(),
            "same": SchemaMapping.query.filter_by(compare_status="same").count(),
            "different": SchemaMapping.query.filter_by(compare_status="different").count(),
            "missing": SchemaMapping.query.filter_by(compare_status="missing").count(),
            "unknown": SchemaMapping.query.filter_by(compare_status="unknown").count(),
        }

        return {"items": [self._to_dto(row) for row in rows], "summary": summary, "filter": normalized}

    def get_mapping_details(self, mapping_id: UUID) -> dict[str, Any] | None:
        row = db.session.get(SchemaMapping, mapping_id)
        if not row:
            return None
        return self._to_detail_dto(row)

    def enqueue_compare_table(self, mapping_id: UUID, requested_by: str = "web-ui") -> Job | None:
        mapping = db.session.get(SchemaMapping, mapping_id)
        if not mapping:
            return None

        job = Job(
            job_type=JobType.SCHEMA_COMPARE_TABLE,
            status=JobStatus.QUEUED,
            payload={
                "mapping_id": str(mapping.id),
                "source_schema": mapping.source_schema,
                "source_table": mapping.source_table,
                "target_schema": mapping.target_schema,
                "target_table": mapping.target_table,
            },
            requested_by=requested_by,
            progress_log=[
                {
                    "ts": _utcnow().isoformat(),
                    "level": "info",
                    "code": "queued",
                    "message": "Schema compare table queued",
                    "meta": {"mapping_id": str(mapping.id)},
                }
            ],
        )
        db.session.add(job)
        mapping.last_job_id = job.id
        mapping.compare_status = mapping.compare_status or "unknown"
        db.session.commit()

        job_service.publish_job_updated(job, changed_fields=["status", "payload", "progress_log", "requested_at"])
        sse_broker.publish(
            "schema_compare_updated",
            {"entity_id": str(mapping.id), "status": mapping.compare_status, "changed_fields": ["last_job_id"]},
        )
        return job

    def enqueue_compare_all(self, requested_by: str = "web-ui") -> tuple[Job, list[UUID]]:
        self._sync_mappings_from_active_configs()
        mappings = SchemaMapping.query.order_by(SchemaMapping.source_schema, SchemaMapping.source_table).all()

        all_job = Job(
            job_type=JobType.SCHEMA_COMPARE_ALL,
            status=JobStatus.QUEUED,
            payload={"scope": "current_list", "mappings_count": len(mappings)},
            requested_by=requested_by,
            progress_log=[
                {
                    "ts": _utcnow().isoformat(),
                    "level": "info",
                    "code": "queued",
                    "message": "Schema compare all queued",
                    "meta": {"mappings_count": len(mappings)},
                }
            ],
        )
        db.session.add(all_job)
        db.session.commit()

        job_service.publish_job_updated(all_job, changed_fields=["status", "payload", "progress_log", "requested_at"])
        return all_job, [m.id for m in mappings]

    def compare_table_now(self, mapping_id: UUID) -> tuple[bool, dict[str, Any], str | None, str]:
        mapping = db.session.get(SchemaMapping, mapping_id)
        if not mapping:
            return False, {}, "Schema mapping not found", "unknown"

        if not mapping.target_table or not mapping.target_schema:
            return False, {"reason": "target_not_mapped"}, "Target table is not mapped", "missing"

        source_url, _ = self._get_active_db_settings(ConfigType.SOURCE_DB)
        target_url, _ = self._get_active_db_settings(ConfigType.TARGET_DB)
        if not source_url or not target_url:
            return False, {"reason": "active_configs_missing"}, "Active source/target configs are required", "unknown"

        try:
            source_cols = self._load_columns(source_url, mapping.source_schema, mapping.source_table)
            target_cols = self._load_columns(target_url, mapping.target_schema, mapping.target_table)
        except SQLAlchemyError as exc:
            return False, {}, f"DB compare error: {exc}", "unknown"

        src_names = [c["name"] for c in source_cols]
        tgt_names = [c["name"] for c in target_cols]
        missing_in_target = [n for n in src_names if n not in tgt_names]
        extra_in_target = [n for n in tgt_names if n not in src_names]
        same = not missing_in_target and not extra_in_target

        result = {
            "source": {"schema": mapping.source_schema, "table": mapping.source_table, "columns": src_names},
            "target": {"schema": mapping.target_schema, "table": mapping.target_table, "columns": tgt_names},
            "diff": {
                "missing_in_target": missing_in_target,
                "extra_in_target": extra_in_target,
                "same": same,
            },
        }
        return same, result, None, "same" if same else "different"

    def apply_compare_result(
        self,
        mapping_id: UUID,
        status: str,
        diff_summary: dict[str, Any] | None,
        job_id: UUID | None,
    ) -> None:
        mapping = db.session.get(SchemaMapping, mapping_id)
        if not mapping:
            return
        mapping.compare_status = status
        mapping.diff_summary = diff_summary
        mapping.last_compared_at = _utcnow()
        if job_id:
            mapping.last_job_id = job_id
        db.session.commit()

        sse_broker.publish(
            "schema_compare_updated",
            {
                "entity_id": str(mapping.id),
                "status": mapping.compare_status,
                "changed_fields": ["compare_status", "diff_summary", "last_compared_at", "last_job_id"],
            },
        )

    def _sync_mappings_from_active_configs(self) -> None:
        source_url, source_schema = self._get_active_db_settings(ConfigType.SOURCE_DB)
        target_url, target_schema = self._get_active_db_settings(ConfigType.TARGET_DB)
        if not source_url:
            return

        try:
            source_tables = self._list_tables(source_url, source_schema)
            target_tables = self._list_tables(target_url, target_schema) if target_url else []
        except SQLAlchemyError:
            return

        target_by_name = {t["table"]: t for t in target_tables}
        changed = False

        for src in source_tables:
            mapping = SchemaMapping.query.filter_by(
                source_schema=src["schema"], source_table=src["table"]
            ).first()
            target_hit = target_by_name.get(src["table"])
            target_schema = target_hit["schema"] if target_hit else None
            target_table = target_hit["table"] if target_hit else None
            next_status = "missing" if not target_table else (mapping.compare_status if mapping else "unknown")

            if not mapping:
                db.session.add(
                    SchemaMapping(
                        source_schema=src["schema"],
                        source_table=src["table"],
                        target_schema=target_schema,
                        target_table=target_table,
                        compare_status=next_status,
                    )
                )
                changed = True
                continue

            if mapping.target_schema != target_schema or mapping.target_table != target_table:
                mapping.target_schema = target_schema
                mapping.target_table = target_table
                if not target_table:
                    mapping.compare_status = "missing"
                elif mapping.compare_status == "missing":
                    mapping.compare_status = "unknown"
                changed = True

        if changed:
            db.session.commit()

    @staticmethod
    def _normalize_filter(status_filter: str) -> str:
        raw = (status_filter or "all").strip().lower()
        if raw not in _VALID_FILTERS:
            raise ValueError("invalid filter")
        return "different" if raw == "diff" else raw

    @staticmethod
    def _get_active_db_settings(config_type: ConfigType) -> tuple[str | None, str | None]:
        active = ActiveConfig.query.filter_by(config_type=config_type).first()
        if not active:
            return None, None
        profile = db.session.get(ConfigProfile, active.profile_id)
        if not profile:
            return None, None
        s = profile.settings_encrypted or {}
        return s.get("database_url"), s.get("schema") or None

    @staticmethod
    def _list_tables(database_url: str, schema: str | None = None) -> list[dict[str, str]]:
        engine = create_engine(database_url)
        is_oracle = database_url.lower().startswith("oracle")
        try:
            insp = inspect(engine)
            result: list[dict[str, str]] = []
            schemas = [schema] if schema else insp.get_schema_names()
            for s in schemas:
                if s in {"pg_catalog", "information_schema"}:
                    continue
                if is_oracle and s.upper() in _ORACLE_SYSTEM_SCHEMAS:
                    continue
                for table in insp.get_table_names(schema=s):
                    result.append({"schema": s, "table": table})
            return result
        finally:
            engine.dispose()

    @staticmethod
    def _load_columns(database_url: str, schema: str, table: str) -> list[dict[str, Any]]:
        engine = create_engine(database_url)
        try:
            return inspect(engine).get_columns(table_name=table, schema=schema)
        finally:
            engine.dispose()

    @staticmethod
    def _to_dto(row: SchemaMapping) -> dict[str, Any]:
        return {
            "id": str(row.id),
            "source_schema": row.source_schema,
            "source_table": row.source_table,
            "target_schema": row.target_schema,
            "target_table": row.target_table,
            "compare_status": row.compare_status,
            "last_compared_at": row.last_compared_at.isoformat() if row.last_compared_at else None,
            "last_job_id": str(row.last_job_id) if row.last_job_id else None,
        }

    def _to_detail_dto(self, row: SchemaMapping) -> dict[str, Any]:
        base = self._to_dto(row)
        base["diff_summary"] = row.diff_summary or {}
        if row.last_job_id:
            job = db.session.get(Job, row.last_job_id)
            base["last_job"] = (
                {
                    "id": str(job.id),
                    "status": job.status.value,
                    "job_type": job.job_type.value,
                    "requested_at": job.requested_at.isoformat() if job.requested_at else None,
                    "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                }
                if job
                else None
            )
        else:
            base["last_job"] = None
        return base


schema_compare_service = SchemaCompareService()
