from __future__ import annotations

from flask import Blueprint, Response, stream_with_context

from app.application.services.sse_broker import sse_broker


sse_bp = Blueprint("sse", __name__, url_prefix="/api/events")


@sse_bp.get("/stream")
def events_stream():
    sid, q = sse_broker.subscribe()
    return Response(
        stream_with_context(sse_broker.stream(sid, q)),
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
