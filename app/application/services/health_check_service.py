from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.application.services.connection_test_service import ConnectionTestService
from app.domain.enums import ConfigType
from app.infrastructure.db.models import ConfigProfile

_COMPONENTS: dict[ConfigType, str] = {
    ConfigType.SOURCE_DB: "source_db",
    ConfigType.TARGET_DB: "target_db",
    ConfigType.KAFKA: "kafka",
    ConfigType.KAFKA_CONNECT: "kafka_connect",
}

_INITIAL: dict[str, Any] = {"status": "unknown", "message": None, "checked_at": None}


class HealthCheckService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._status: dict[str, dict[str, Any]] = {
            key: dict(_INITIAL) for key in _COMPONENTS.values()
        }
        self._tester = ConnectionTestService()

    def get_all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._status.items()}

    def check_all(self) -> None:
        from app.application.services.sse_broker import sse_broker

        for config_type, key in _COMPONENTS.items():
            self._check_one(config_type, key)

        sse_broker.publish("health_status_updated", self.get_all())

    def _check_one(self, config_type: ConfigType, key: str) -> None:
        profile = ConfigProfile.query.filter_by(config_type=config_type, is_enabled=True).first()
        if not profile:
            self._set(key, "unknown", "Нет конфигурации")
            return

        ok, result, error = self._tester.run_test({"settings": profile.settings_encrypted or {}})
        message = (result.get("message") if isinstance(result, dict) else str(result)) or ""
        if not ok and error:
            message = error
        self._set(key, "ok" if ok else "error", message)

    def _set(self, key: str, status: str, message: str | None) -> None:
        with self._lock:
            self._status[key] = {
                "status": status,
                "message": message,
                "checked_at": datetime.now(tz=timezone.utc).isoformat(),
            }


health_check_service = HealthCheckService()
