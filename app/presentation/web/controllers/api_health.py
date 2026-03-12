from __future__ import annotations

from flask import Blueprint, jsonify

from app.application.services.health_check_service import health_check_service

api_health_bp = Blueprint("api_health", __name__, url_prefix="/api")


@api_health_bp.get("/health-status")
def get_health_status():
    return jsonify(health_check_service.get_all())
