import os

from dotenv import load_dotenv

load_dotenv()  # загружает .env если файл присутствует

from app import create_app


def main() -> None:
    work_type = os.getenv("WORK_TYPE", "web")
    app = create_app(work_type=work_type)
    app.run(
        host=os.getenv("FLASK_RUN_HOST", "127.0.0.1"),
        port=int(os.getenv("FLASK_RUN_PORT", "5000")),
        debug=os.getenv("FLASK_DEBUG", "0") == "1",
    )


if __name__ == "__main__":
    main()
