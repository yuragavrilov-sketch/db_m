from __future__ import annotations

from uuid import UUID

from flask import Blueprint, jsonify, request

from app.application.services.background_executor import background_executor
from app.application.services.config_service import (
    ActiveConfigDeleteError,
    DuplicateConfigNameError,
    InvalidSettingsError,
    config_service,
)
from app.application.services.job_service import job_service
from app.domain.enums import ConfigType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ConfigProfile


api_config_bp = Blueprint("api_config", __name__, url_prefix="/api/configs")


@api_config_bp.get("")
def list_configs():
    raw_type = request.args.get("type")
    try:
        config_type = ConfigType(raw_type) if raw_type else None
    except ValueError:
        valid = [ct.value for ct in ConfigType]
        return jsonify({"error": f"unknown config type '{raw_type}', valid values: {valid}"}), 400
    return jsonify(config_service.list_configs(config_type))


@api_config_bp.post("")
def create_config():
    data = request.get_json(silent=True) or {}
    missing = [f for f in ("config_type", "name") if not data.get(f)]
    if missing:
        return jsonify({"error": f"missing required fields: {missing}"}), 400
    try:
        config_type = ConfigType(data["config_type"])
    except ValueError:
        valid = [ct.value for ct in ConfigType]
        return jsonify({"error": f"unknown config_type '{data['config_type']}', valid values: {valid}"}), 400
    try:
        created = config_service.create_config(
            config_type=config_type,
            name=data["name"],
            description=data.get("description"),
            settings=data.get("settings", {}),
        )
        return jsonify(created), 201
    except InvalidSettingsError as exc:
        return jsonify({"error": f"missing required settings for {config_type.value}: {exc.missing}"}), 422
    except DuplicateConfigNameError:
        return jsonify({"error": f"profile name '{data['name']}' already exists for type '{config_type.value}'"}), 409


@api_config_bp.put("/<uuid:profile_id>")
def update_config(profile_id: UUID):
    data = request.get_json(silent=True) or {}
    if not data.get("name"):
        return jsonify({"error": "missing required field: name"}), 400
    try:
        updated = config_service.update_config(
            profile_id=profile_id,
            name=data["name"],
            description=data.get("description"),
            settings=data.get("settings", {}),
            is_enabled=bool(data.get("is_enabled", True)),
        )
    except InvalidSettingsError as exc:
        return jsonify({"error": f"missing required settings: {exc.missing}"}), 422
    except DuplicateConfigNameError:
        return jsonify({"error": f"profile name '{data['name']}' already exists for this type"}), 409

    if not updated:
        return jsonify({"error": "not found"}), 404
    return jsonify(updated)


@api_config_bp.delete("/<uuid:profile_id>")
def delete_config(profile_id: UUID):
    try:
        deleted = config_service.delete_config(profile_id)
    except ActiveConfigDeleteError:
        return jsonify({"error": "cannot delete active config"}), 409

    if not deleted:
        return jsonify({"error": "not found"}), 404
    return "", 204


@api_config_bp.post("/<uuid:profile_id>/activate")
def activate_config(profile_id: UUID):
    result = config_service.activate_config(profile_id)
    if not result:
        return jsonify({"error": "not found"}), 404
    return jsonify(result)


@api_config_bp.post("/<uuid:profile_id>/test-connection")
def test_connection(profile_id: UUID):
    profile = db.session.get(ConfigProfile, profile_id)
    if not profile:
        return jsonify({"error": "not found"}), 404

    job = job_service.enqueue_connection_test(
        config_profile_id=profile.id,
        payload={
            "config_type": profile.config_type.value,
            "settings": profile.settings_encrypted,
            "name": profile.name,
        },
    )
    background_executor.enqueue(job.id)
    return jsonify({"job_id": str(job.id), "status": "queued"}), 202
