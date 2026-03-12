from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    work_type: str
    database_url: str
    secret_key: str
    sqlalchemy_echo: bool

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            work_type=os.getenv("WORK_TYPE", "web"),
            database_url=os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/db_m"),
            secret_key=os.getenv("SECRET_KEY", "dev-secret-key"),
            sqlalchemy_echo=os.getenv("SQLALCHEMY_ECHO", "0") == "1",
        )

    def to_flask_config(self) -> dict:
        return {
            "WORK_TYPE": self.work_type,
            "SECRET_KEY": self.secret_key,
            "SQLALCHEMY_DATABASE_URI": self.database_url,
            "SQLALCHEMY_TRACK_MODIFICATIONS": False,
            "SQLALCHEMY_ECHO": self.sqlalchemy_echo,
        }
