from __future__ import annotations

import socket
import urllib.error
import urllib.request
from typing import Any

from sqlalchemy import create_engine, text


class ConnectionTestService:
    def run_test(self, payload: dict[str, Any]) -> tuple[bool, dict[str, Any], str | None]:
        settings = payload.get("settings", {})

        database_url = settings.get("database_url")
        if database_url:
            try:
                engine = create_engine(database_url)
                is_oracle = database_url.lower().startswith("oracle")
                probe = text("SELECT 1 FROM DUAL") if is_oracle else text("SELECT 1")
                with engine.connect() as conn:
                    conn.execute(probe)
                return True, {"kind": "db", "message": "Database connection successful"}, None
            except Exception as exc:  # noqa: BLE001
                return False, {"kind": "db", "message": "Database connection failed"}, str(exc)

        bootstrap_servers = settings.get("bootstrap_servers")
        if bootstrap_servers:
            return self._test_kafka(bootstrap_servers)

        connect_url = settings.get("connect_url")
        if connect_url:
            return self._test_kafka_connect(connect_url)

        return False, {"kind": "unknown", "message": "Unsupported configuration for test"}, "No supported settings"

    @staticmethod
    def _test_kafka(bootstrap_servers: str) -> tuple[bool, dict[str, Any], str | None]:
        brokers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
        if not brokers:
            return False, {"kind": "kafka", "message": "No brokers specified"}, "Empty bootstrap_servers"

        failed: list[str] = []
        for broker in brokers:
            parts = broker.rsplit(":", 1)
            if len(parts) != 2:
                failed.append(f"{broker}: invalid format, expected host:port")
                continue
            host, port_str = parts
            try:
                port = int(port_str)
            except ValueError:
                failed.append(f"{broker}: invalid port '{port_str}'")
                continue
            try:
                with socket.create_connection((host, port), timeout=5):
                    pass
            except OSError as exc:
                failed.append(f"{broker}: {exc}")

        if failed:
            return (
                False,
                {
                    "kind": "kafka",
                    "message": f"Failed to reach {len(failed)}/{len(brokers)} broker(s)",
                    "brokers_failed": failed,
                },
                "; ".join(failed),
            )
        return (
            True,
            {"kind": "kafka", "message": f"All {len(brokers)} broker(s) reachable"},
            None,
        )

    @staticmethod
    def _test_kafka_connect(connect_url: str) -> tuple[bool, dict[str, Any], str | None]:
        url = connect_url.rstrip("/") + "/"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                return (
                    True,
                    {"kind": "kafka_connect", "message": f"Kafka Connect reachable (HTTP {resp.status})"},
                    None,
                )
        except urllib.error.HTTPError as exc:
            # 4xx/5xx still means the server responded
            return (
                True,
                {"kind": "kafka_connect", "message": f"Kafka Connect responded (HTTP {exc.code})"},
                None,
            )
        except Exception as exc:  # noqa: BLE001
            return False, {"kind": "kafka_connect", "message": "Kafka Connect unreachable"}, str(exc)


connection_test_service = ConnectionTestService()
