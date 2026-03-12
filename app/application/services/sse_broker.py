from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator
from uuid import uuid4


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class SseEvent:
    event: str
    data: dict[str, Any]


class SseBroker:
    def __init__(self) -> None:
        self._subs: dict[str, queue.Queue[SseEvent]] = {}
        self._lock = threading.Lock()

    def subscribe(self) -> tuple[str, queue.Queue[SseEvent]]:
        sid = str(uuid4())
        q: queue.Queue[SseEvent] = queue.Queue(maxsize=100)
        with self._lock:
            self._subs[sid] = q
        return sid, q

    def unsubscribe(self, sid: str) -> None:
        with self._lock:
            self._subs.pop(sid, None)

    def publish(self, event: str, data: dict[str, Any]) -> None:
        envelope = SseEvent(event=event, data={**data, "sent_at": _utcnow_iso()})
        with self._lock:
            subs = list(self._subs.values())
        for q in subs:
            try:
                q.put_nowait(envelope)
            except queue.Full:
                continue

    @staticmethod
    def format_event(evt: SseEvent) -> str:
        return f"event: {evt.event}\ndata: {json.dumps(evt.data, ensure_ascii=False)}\n\n"

    def stream(self, sid: str, q: queue.Queue[SseEvent]) -> Iterator[str]:
        try:
            yield ": connected\n\n"
            while True:
                try:
                    evt = q.get(timeout=5)
                    yield self.format_event(evt)
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            self.unsubscribe(sid)


sse_broker = SseBroker()
