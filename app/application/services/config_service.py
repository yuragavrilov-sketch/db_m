from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from app.domain.enums import ConfigType
from app.infrastructure.db.extensions import db
from app.infrastructure.db.models import ConfigProfile

# Обязательные поля settings для каждого типа конфигурации
_REQUIRED_SETTINGS: dict[ConfigType, list[str]] = {
    ConfigType.SOURCE_DB: ["database_url"],
    ConfigType.TARGET_DB: ["database_url"],
    ConfigType.KAFKA: ["bootstrap_servers"],
    ConfigType.KAFKA_CONNECT: ["connect_url"],
}


class DuplicateConfigNameError(Exception):
    pass


class InvalidSettingsError(Exception):
    """Raised when required settings fields are missing for the given config_type."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__(f"Missing required settings: {', '.join(missing)}")


class ConfigService:
    @staticmethod
    def _validate_settings(config_type: ConfigType, settings: dict[str, Any]) -> None:
        required = _REQUIRED_SETTINGS.get(config_type, [])
        missing = [f for f in required if not settings.get(f)]
        if missing:
            raise InvalidSettingsError(missing)

    def list_configs(self, config_type: ConfigType | None = None) -> list[dict[str, Any]]:
        query = ConfigProfile.query
        if config_type:
            query = query.filter_by(config_type=config_type)
        profiles = query.order_by(ConfigProfile.config_type, ConfigProfile.name).all()
        return [self._to_dto(p) for p in profiles]

    def create_config(
        self,
        config_type: ConfigType,
        name: str,
        description: str | None,
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        self._validate_settings(config_type, settings)
        profile = ConfigProfile(
            config_type=config_type,
            name=name,
            description=description,
            settings_encrypted=settings,
        )
        db.session.add(profile)
        try:
            db.session.commit()
        except IntegrityError as exc:
            db.session.rollback()
            raise DuplicateConfigNameError from exc
        return self._to_dto(profile)

    def update_config(
        self,
        profile_id: UUID,
        name: str,
        description: str | None,
        settings: dict[str, Any],
        is_enabled: bool = True,
    ) -> dict[str, Any] | None:
        profile = db.session.get(ConfigProfile, profile_id)
        if not profile:
            return None

        self._validate_settings(profile.config_type, settings)
        profile.name = name
        profile.description = description
        profile.settings_encrypted = settings
        profile.is_enabled = is_enabled

        try:
            db.session.commit()
        except IntegrityError as exc:
            db.session.rollback()
            raise DuplicateConfigNameError from exc

        return self._to_dto(profile)

    def delete_config(self, profile_id: UUID) -> bool:
        profile = db.session.get(ConfigProfile, profile_id)
        if not profile:
            return False
        db.session.delete(profile)
        db.session.commit()
        return True

    @staticmethod
    def _to_dto(profile: ConfigProfile) -> dict[str, Any]:
        return {
            "id": str(profile.id),
            "config_type": profile.config_type.value,
            "name": profile.name,
            "description": profile.description,
            "settings": profile.settings_encrypted or {},
            "is_enabled": profile.is_enabled,
            "created_at": profile.created_at.isoformat() if profile.created_at else None,
            "updated_at": profile.updated_at.isoformat() if profile.updated_at else None,
        }


config_service = ConfigService()
