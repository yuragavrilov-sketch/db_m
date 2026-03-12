from __future__ import annotations

from urllib.parse import urlparse

from flask import Blueprint, render_template

from app.application.services.config_service import config_service
from app.domain.enums import ConfigType


pages_bp = Blueprint("pages", __name__)

_CONFIG_TYPE_LABELS: dict[str, str] = {
    ConfigType.SOURCE_DB.value: "База-источник",
    ConfigType.TARGET_DB.value: "База-приёмник",
    ConfigType.KAFKA.value: "Kafka — брокер",
    ConfigType.KAFKA_CONNECT.value: "Kafka Connect",
}


def _conn_summary(settings: dict) -> str:
    """Краткое описание подключения без пароля."""
    if not settings:
        return "—"
    url = settings.get("database_url")
    if url:
        try:
            p = urlparse(url)
            netloc = p.hostname or ""
            if p.port:
                netloc += f":{p.port}"
            path = p.path.lstrip("/")
            user = p.username or ""
            return f"{user}@{netloc}/{path}" if user else f"{netloc}/{path}"
        except Exception:  # noqa: BLE001
            return url
    if settings.get("bootstrap_servers"):
        return settings["bootstrap_servers"]
    if settings.get("connect_url"):
        return settings["connect_url"]
    return "—"


@pages_bp.get("/")
@pages_bp.get("/config")
def config_tab():
    configs = config_service.list_configs()
    grouped: dict[str, list[dict]] = {t.value: [] for t in ConfigType}
    for item in configs:
        item["conn_summary"] = _conn_summary(item["settings"])
        grouped[item["config_type"]].append(item)

    return render_template(
        "tabs/config.html",
        config_types=[t.value for t in ConfigType],
        config_type_labels=_CONFIG_TYPE_LABELS,
        grouped_configs=grouped,
    )


@pages_bp.get("/schema")
def schema_tab():
    return render_template("tabs/schema_compare.html")


@pages_bp.get("/jobs")
def jobs_tab():
    return render_template("tabs/jobs.html")
