from __future__ import annotations

from uuid import UUID

from flask import Blueprint, jsonify

from app.application.services.background_executor import background_executor
from app.application.services.schema_compare_service import schema_compare_service
from flask import request


api_schema_bp = Blueprint("api_schema", __name__, url_prefix="/api/schema")


@api_schema_bp.get("/mappings")
def list_mappings():
    status_filter = request.args.get("filter", "all")
    try:
        return jsonify(schema_compare_service.list_mappings(status_filter=status_filter))
    except ValueError:
        return jsonify({"error": "invalid filter"}), 400


@api_schema_bp.get("/mappings/<uuid:mapping_id>")
def mapping_details(mapping_id: UUID):
    details = schema_compare_service.get_mapping_details(mapping_id)
    if not details:
        return jsonify({"error": "not found"}), 404
    return jsonify(details)


@api_schema_bp.post("/mappings/<uuid:mapping_id>/compare")
def compare_one(mapping_id: UUID):
    job = schema_compare_service.enqueue_compare_table(mapping_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    background_executor.enqueue(job.id)
    return jsonify({"job_id": str(job.id), "status": "queued"}), 202


@api_schema_bp.post("/compare-all")
def compare_all():
    job, _ = schema_compare_service.enqueue_compare_all()
    background_executor.enqueue(job.id)
    return jsonify({"job_id": str(job.id), "status": "queued"}), 202
