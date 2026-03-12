from __future__ import annotations

from uuid import UUID

from flask import Blueprint, jsonify, request

from app.application.services.config_service import (
    DuplicateConfigNameError,
    InvalidSettingsError,
    config_service,
)
from app.domain.enums import ConfigType


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
    deleted = config_service.delete_config(profile_id)
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return "", 204
