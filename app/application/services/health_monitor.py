from __future__ import annotations

import threading
import time

from flask import Flask


class HealthMonitor:
    def __init__(self) -> None:
        self._started = False
        self._lock = threading.Lock()

    def start(self, app: Flask, interval_seconds: int = 60) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(
            target=self._run,
            args=(app, interval_seconds),
            daemon=True,
            name="db-m-health-monitor",
        ).start()

    def _run(self, app: Flask, interval_seconds: int) -> None:
        from app.application.services.health_check_service import health_check_service

        with app.app_context():
            while True:
                try:
                    health_check_service.check_all()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(interval_seconds)


health_monitor = HealthMonitor()
