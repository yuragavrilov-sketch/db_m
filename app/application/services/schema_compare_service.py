from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import create_engine, inspect, text

_ORACLE_SYSTEM_SCHEMAS = frozenset({
    "SYS", "SYSTEM", "OUTLN", "DBSNMP", "APPQOSSYS", "DBSFWUSER",
    "GGSYS", "ANONYMOUS", "CTXSYS", "DVSYS", "DVF", "GSMADMIN_INTERNAL",
    "MDSYS", "OLAPSYS", "REMOTE_SCHEDULER_AGENT", "XDB", "XS$NULL",
    "LBACSYS", "ORDSYS", "ORDPLUGINS", "ORDDATA", "SI_INFORMTN_SCHEMA", "WMSYS",
})
from sqlalchemy.exc import SQLAlchemyError

from app.application.services.job_service import job_service
from app.application.services.sse_broker import sse_broker
from app.domain.enums import ConfigType, JobStatus, JobType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ConfigProfile, Job, SchemaMapping


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
            source_meta = self._load_table_metadata(source_url, mapping.source_schema, mapping.source_table)
            target_meta = self._load_table_metadata(target_url, mapping.target_schema, mapping.target_table)
        except SQLAlchemyError as exc:
            return False, {}, f"DB compare error: {exc}", "unknown"

        cols_diff = self._compare_columns(source_meta["columns"], target_meta["columns"])
        idx_diff = self._compare_indexes(source_meta["indexes"], target_meta["indexes"])
        pk_diff = self._compare_pk(source_meta["pk"], target_meta["pk"])
        fk_diff = self._compare_fks(source_meta["foreign_keys"], target_meta["foreign_keys"])
        uq_diff = self._compare_named_items(
            source_meta["unique_constraints"],
            target_meta["unique_constraints"],
            key_fn=lambda x: x.get("name") or "",
            eq_fn=lambda s, t: sorted(s.get("column_names", [])) == sorted(t.get("column_names", [])),
            detail_fn=lambda x: {"columns": x.get("column_names", [])},
        )
        chk_diff = self._compare_named_items(
            source_meta["check_constraints"],
            target_meta["check_constraints"],
            key_fn=lambda x: x.get("name") or "",
            eq_fn=lambda s, t: s.get("sqltext", "") == t.get("sqltext", ""),
            detail_fn=lambda x: {"sqltext": x.get("sqltext", "")},
        )
        trig_diff = self._compare_triggers(source_meta["triggers"], target_meta["triggers"])
        src_trig_err = source_meta.get("trigger_error")
        tgt_trig_err = target_meta.get("trigger_error")
        if src_trig_err or tgt_trig_err:
            trig_diff["load_error"] = {
                "source": src_trig_err,
                "target": tgt_trig_err,
            }

        same = all([
            cols_diff["same"],
            idx_diff["same"],
            pk_diff["same"],
            fk_diff["same"],
            uq_diff["same"],
            chk_diff["same"],
            trig_diff["same"],
        ])

        result = {
            "source": {"schema": mapping.source_schema, "table": mapping.source_table},
            "target": {"schema": mapping.target_schema, "table": mapping.target_table},
            "diff": {
                "same": same,
                "columns": cols_diff,
                "indexes": idx_diff,
                "primary_key": pk_diff,
                "foreign_keys": fk_diff,
                "unique_constraints": uq_diff,
                "check_constraints": chk_diff,
                "triggers": trig_diff,
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

    # ── Comparison helpers ────────────────────────────────────────────────────

    @staticmethod
    def _compare_columns(source_cols: list, target_cols: list) -> dict[str, Any]:
        src_by_name = {c["name"]: c for c in source_cols}
        tgt_by_name = {c["name"]: c for c in target_cols}
        all_names = list(dict.fromkeys(
            [c["name"] for c in source_cols] + [c["name"] for c in target_cols]
        ))
        items = []
        same = True
        for name in all_names:
            src = src_by_name.get(name)
            tgt = tgt_by_name.get(name)
            if src and tgt:
                src_type = str(src.get("type", ""))
                tgt_type = str(tgt.get("type", ""))
                src_null = src.get("nullable", True)
                tgt_null = tgt.get("nullable", True)
                src_def = str(src["default"]) if src.get("default") is not None else None
                tgt_def = str(tgt["default"]) if tgt.get("default") is not None else None
                diff_fields = []
                if src_type != tgt_type:
                    diff_fields.append("type")
                if src_null != tgt_null:
                    diff_fields.append("nullable")
                if src_def != tgt_def:
                    diff_fields.append("default")
                is_eq = not diff_fields
                if not is_eq:
                    same = False
                items.append({
                    "name": name,
                    "source": {"type": src_type, "nullable": src_null, "default": src_def},
                    "target": {"type": tgt_type, "nullable": tgt_null, "default": tgt_def},
                    "status": "same" if is_eq else "different",
                    "diff_fields": diff_fields,
                })
            elif src:
                same = False
                items.append({
                    "name": name,
                    "source": {"type": str(src.get("type", "")), "nullable": src.get("nullable", True),
                               "default": str(src["default"]) if src.get("default") is not None else None},
                    "target": None,
                    "status": "missing_in_target",
                    "diff_fields": [],
                })
            else:
                same = False
                items.append({
                    "name": name,
                    "source": None,
                    "target": {"type": str(tgt.get("type", "")), "nullable": tgt.get("nullable", True),
                               "default": str(tgt["default"]) if tgt.get("default") is not None else None},
                    "status": "extra_in_target",
                    "diff_fields": [],
                })
        return {"items": items, "same": same}

    @staticmethod
    def _compare_indexes(source_idx: list, target_idx: list) -> dict[str, Any]:
        def detail(i: dict) -> dict:
            return {"columns": i.get("column_names", []), "unique": bool(i.get("unique", False))}

        def eq(s: dict, t: dict) -> bool:
            return (
                sorted(s.get("column_names") or []) == sorted(t.get("column_names") or [])
                and bool(s.get("unique")) == bool(t.get("unique"))
            )

        src_by_name = {i["name"]: i for i in source_idx if i.get("name")}
        tgt_by_name = {i["name"]: i for i in target_idx if i.get("name")}
        all_names = sorted(set(src_by_name) | set(tgt_by_name))
        items = []
        same = True
        for name in all_names:
            src = src_by_name.get(name)
            tgt = tgt_by_name.get(name)
            if src and tgt:
                is_eq = eq(src, tgt)
                if not is_eq:
                    same = False
                items.append({"name": name, "source": detail(src), "target": detail(tgt),
                               "status": "same" if is_eq else "different"})
            elif src:
                same = False
                items.append({"name": name, "source": detail(src), "target": None, "status": "missing_in_target"})
            else:
                same = False
                items.append({"name": name, "source": None, "target": detail(tgt), "status": "extra_in_target"})
        return {"items": items, "same": same}

    @staticmethod
    def _compare_pk(source_pk: dict, target_pk: dict) -> dict[str, Any]:
        src_cols = sorted(source_pk.get("constrained_columns") or [])
        tgt_cols = sorted(target_pk.get("constrained_columns") or [])
        same = src_cols == tgt_cols
        return {
            "same": same,
            "source": {"name": source_pk.get("name"), "columns": src_cols},
            "target": {"name": target_pk.get("name"), "columns": tgt_cols},
        }

    @staticmethod
    def _compare_fks(source_fks: list, target_fks: list) -> dict[str, Any]:
        def detail(fk: dict) -> dict:
            return {
                "columns": fk.get("constrained_columns", []),
                "referred_schema": fk.get("referred_schema"),
                "referred_table": fk.get("referred_table"),
                "referred_columns": fk.get("referred_columns", []),
            }

        def eq(s: dict, t: dict) -> bool:
            return (
                sorted(s.get("constrained_columns") or []) == sorted(t.get("constrained_columns") or [])
                and s.get("referred_table") == t.get("referred_table")
                and sorted(s.get("referred_columns") or []) == sorted(t.get("referred_columns") or [])
            )

        src_by_name = {fk.get("name") or "": fk for fk in source_fks}
        tgt_by_name = {fk.get("name") or "": fk for fk in target_fks}
        all_names = sorted(set(src_by_name) | set(tgt_by_name))
        items = []
        same = True
        for name in all_names:
            src = src_by_name.get(name)
            tgt = tgt_by_name.get(name)
            if src and tgt:
                is_eq = eq(src, tgt)
                if not is_eq:
                    same = False
                items.append({"name": name, "source": detail(src), "target": detail(tgt),
                               "status": "same" if is_eq else "different"})
            elif src:
                same = False
                items.append({"name": name, "source": detail(src), "target": None, "status": "missing_in_target"})
            else:
                same = False
                items.append({"name": name, "source": None, "target": detail(tgt), "status": "extra_in_target"})
        return {"items": items, "same": same}

    @staticmethod
    def _compare_named_items(
        source_items: list,
        target_items: list,
        key_fn: Any,
        eq_fn: Any,
        detail_fn: Any,
    ) -> dict[str, Any]:
        src_by_key = {key_fn(x): x for x in source_items if key_fn(x)}
        tgt_by_key = {key_fn(x): x for x in target_items if key_fn(x)}
        all_keys = sorted(set(src_by_key) | set(tgt_by_key))
        items = []
        same = True
        for key in all_keys:
            src = src_by_key.get(key)
            tgt = tgt_by_key.get(key)
            if src and tgt:
                is_eq = eq_fn(src, tgt)
                if not is_eq:
                    same = False
                items.append({"name": key, "source": detail_fn(src), "target": detail_fn(tgt),
                               "status": "same" if is_eq else "different"})
            elif src:
                same = False
                items.append({"name": key, "source": detail_fn(src), "target": None, "status": "missing_in_target"})
            else:
                same = False
                items.append({"name": key, "source": None, "target": detail_fn(tgt), "status": "extra_in_target"})
        return {"items": items, "same": same}

    @staticmethod
    def _compare_triggers(source_trigs: list, target_trigs: list) -> dict[str, Any]:
        """Compare triggers field-by-field: presence, event, timing, enabled, body."""
        def _detail(t: dict) -> dict:
            return {
                "event":   t.get("event"),
                "timing":  t.get("timing"),
                "enabled": t.get("enabled"),
                "body":    t.get("body"),
            }

        src_by_name = {t["name"]: t for t in source_trigs if t.get("name")}
        tgt_by_name = {t["name"]: t for t in target_trigs if t.get("name")}
        all_names = sorted(set(src_by_name) | set(tgt_by_name))
        items: list[dict[str, Any]] = []
        same = True

        for name in all_names:
            src = src_by_name.get(name)
            tgt = tgt_by_name.get(name)
            if src and tgt:
                diff_fields: list[str] = []
                if src.get("event")   != tgt.get("event"):
                    diff_fields.append("event")
                if src.get("timing")  != tgt.get("timing"):
                    diff_fields.append("timing")
                if src.get("enabled") != tgt.get("enabled"):
                    diff_fields.append("enabled")
                src_body = (src.get("body") or "").strip()
                tgt_body = (tgt.get("body") or "").strip()
                if src_body != tgt_body:
                    diff_fields.append("body")
                if diff_fields:
                    same = False
                items.append({
                    "name": name,
                    "source": _detail(src),
                    "target": _detail(tgt),
                    "status": "same" if not diff_fields else "different",
                    "diff_fields": diff_fields,
                })
            elif src:
                same = False
                items.append({"name": name, "source": _detail(src), "target": None,
                               "status": "missing_in_target", "diff_fields": []})
            else:
                same = False
                items.append({"name": name, "source": None, "target": _detail(tgt),
                               "status": "extra_in_target", "diff_fields": []})

        return {"items": items, "same": same}

    # ── DB introspection ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_filter(status_filter: str) -> str:
        raw = (status_filter or "all").strip().lower()
        if raw not in _VALID_FILTERS:
            raise ValueError("invalid filter")
        return "different" if raw == "diff" else raw

    @staticmethod
    def _get_active_db_settings(config_type: ConfigType) -> tuple[str | None, str | None]:
        profile = ConfigProfile.query.filter_by(config_type=config_type, is_enabled=True).first()
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
    def _load_table_metadata(database_url: str, schema: str, table: str) -> dict[str, Any]:
        engine = create_engine(database_url)
        is_oracle = engine.dialect.name == "oracle"
        try:
            insp = inspect(engine)
            columns = insp.get_columns(table_name=table, schema=schema)
            indexes = insp.get_indexes(table_name=table, schema=schema)
            pk = insp.get_pk_constraint(table_name=table, schema=schema) or {}
            fks = insp.get_foreign_keys(table_name=table, schema=schema)
            try:
                unique_constraints = insp.get_unique_constraints(table_name=table, schema=schema)
            except Exception:
                unique_constraints = []
            try:
                check_constraints = insp.get_check_constraints(table_name=table, schema=schema)
            except Exception:
                check_constraints = []

            try:
                triggers, trigger_error = SchemaCompareService._load_triggers(engine, schema, table, is_oracle)
            except Exception as exc:
                triggers, trigger_error = [], str(exc)

            return {
                "columns": columns,
                "indexes": indexes,
                "pk": pk,
                "foreign_keys": fks,
                "unique_constraints": unique_constraints,
                "check_constraints": check_constraints,
                "triggers": triggers,
                "trigger_error": trigger_error,
            }
        finally:
            engine.dispose()

    @staticmethod
    def _load_triggers(engine: Any, schema: str, table: str, is_oracle: bool) -> tuple[list[dict[str, Any]], str | None]:
        if is_oracle:
            sql = text("""
                SELECT trigger_name  AS name,
                       triggering_event AS event,
                       trigger_type  AS timing,
                       status        AS enabled,
                       trigger_body  AS body
                FROM all_triggers
                WHERE owner = :schema AND table_name = :table
            """)
            params = {"schema": schema.upper(), "table": table.upper()}
        else:
            # pg_trigger is more reliable than information_schema.triggers:
            # information_schema.triggers only shows triggers on tables owned by current user.
            sql = text("""
                SELECT t.tgname AS name,
                       array_to_string(
                           ARRAY[
                               CASE WHEN (t.tgtype::integer & 4)  > 0 THEN 'INSERT'   END,
                               CASE WHEN (t.tgtype::integer & 8)  > 0 THEN 'DELETE'   END,
                               CASE WHEN (t.tgtype::integer & 16) > 0 THEN 'UPDATE'   END,
                               CASE WHEN (t.tgtype::integer & 32) > 0 THEN 'TRUNCATE' END
                           ]::text[],
                           ', '
                       ) AS event,
                       CASE
                           WHEN (t.tgtype::integer & 64) > 0 THEN 'INSTEAD OF'
                           WHEN (t.tgtype::integer & 2)  > 0 THEN 'BEFORE'
                           ELSE 'AFTER'
                       END AS timing,
                       CASE WHEN t.tgenabled = 'D' THEN 'DISABLED' ELSE 'ENABLED' END AS enabled,
                       p.prosrc AS body
                FROM pg_trigger t
                JOIN pg_class c     ON c.oid = t.tgrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_proc p      ON p.oid = t.tgfoid
                WHERE n.nspname = :schema
                  AND c.relname  = :table
                  AND NOT t.tgisinternal
                ORDER BY t.tgname
            """)
            params = {"schema": schema, "table": table}

        with engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
            return [dict(r) for r in rows], None

    @staticmethod
    def _load_columns(database_url: str, schema: str, table: str) -> list[dict[str, Any]]:
        engine = create_engine(database_url)
        try:
            return inspect(engine).get_columns(table_name=table, schema=schema)
        finally:
            engine.dispose()

    @staticmethod
    def _diff_categories(diff_summary: dict | None) -> list[str]:
        """Return short keys for diff categories that are NOT same."""
        if not diff_summary:
            return []
        diff = diff_summary.get("diff", {})
        short = {
            "columns": "cols", "indexes": "idx", "primary_key": "pk",
            "foreign_keys": "fk", "unique_constraints": "uq",
            "check_constraints": "ck", "triggers": "trg",
        }
        return [v for k, v in short.items() if not (diff.get(k) or {}).get("same", True)]

    @staticmethod
    def _to_dto(row: SchemaMapping) -> dict[str, Any]:
        return {
            "id": str(row.id),
            "source_schema": row.source_schema,
            "source_table": row.source_table,
            "target_schema": row.target_schema,
            "target_table": row.target_table,
            "compare_status": row.compare_status,
            "diff_categories": SchemaCompareService._diff_categories(row.diff_summary),
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
                    "result": job.result,
                    "error_text": job.error_text,
                }
                if job
                else None
            )
        else:
            base["last_job"] = None
        return base


schema_compare_service = SchemaCompareService()
