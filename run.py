import os
import signal
import sys
import threading
import time

from dotenv import load_dotenv

load_dotenv()  # загружает .env если файл присутствует

from app import create_app


def main() -> None:
    import signal
    import time

    # CLI arg takes priority over WORK_TYPE env var: python run.py [web|worker]
    work_type = sys.argv[1] if len(sys.argv) > 1 else os.getenv("WORK_TYPE", "web")
    app = create_app(work_type=work_type)

    if work_type == "worker":
        # Worker doesn't need the Werkzeug dev server — the poller runs in a
        # daemon thread.  We just keep the main thread alive so the process
        # doesn't exit.  A minimal Flask /health endpoint is still registered
        # inside create_worker_app(), but we don't expose it via app.run() to
        # avoid the debug-reloader spawning a second worker process.
        stop = threading.Event()
        signal.signal(signal.SIGINT,  lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        while not stop.is_set():
            time.sleep(1)
    else:
        app.run(
            host=os.getenv("FLASK_RUN_HOST", "127.0.0.1"),
            port=int(os.getenv("FLASK_RUN_PORT", "5000")),
            debug=os.getenv("FLASK_DEBUG", "0") == "1",
        )


if __name__ == "__main__":
    main()
