from __future__ import annotations

from uuid import UUID

from flask import Blueprint, jsonify, request

from app.application.services.job_service import job_service


api_jobs_bp = Blueprint("api_jobs", __name__, url_prefix="/api/jobs")


@api_jobs_bp.get("")
def list_jobs():
    status = request.args.get("status")
    job_type = request.args.get("type")
    q = request.args.get("q")
    created_from = request.args.get("created_from")
    created_to = request.args.get("created_to")

    try:
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "invalid pagination params"}), 400

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    try:
        return jsonify(
            job_service.list_jobs(
                status=status,
                job_type=job_type,
                q=q,
                created_from=created_from,
                created_to=created_to,
                limit=limit,
                offset=offset,
            )
        )
    except ValueError:
        return jsonify({"error": "invalid status or type"}), 400


@api_jobs_bp.get("/<uuid:job_id>")
def get_job(job_id: UUID):
    data = job_service.get_job(job_id)
    if not data:
        return jsonify({"error": "not found"}), 404
    return jsonify(data)
