from enum import Enum


class ConfigType(str, Enum):
    SOURCE_DB = "source_db"
    TARGET_DB = "target_db"
    KAFKA = "kafka"
    KAFKA_CONNECT = "kafka_connect"


class JobType(str, Enum):
    CONNECTION_TEST = "connection_test"
    SCHEMA_COMPARE_TABLE = "schema_compare_table"
    SCHEMA_COMPARE_ALL = "schema_compare_all"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
