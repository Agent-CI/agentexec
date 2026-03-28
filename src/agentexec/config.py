from zoneinfo import ZoneInfo

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    table_prefix: str = Field(
        default="agentexec_",
        description="Prefix for database table names",
        validation_alias="AGENTEXEC_TABLE_PREFIX",
    )
    queue_name: str = Field(
        default="agentexec_tasks",
        description="Name of the task queue (Redis list key or Kafka topic base name)",
        validation_alias="AGENTEXEC_QUEUE_NAME",
    )
    num_workers: int = Field(
        default=4,
        description="Number of worker processes to spawn",
        validation_alias="AGENTEXEC_NUM_WORKERS",
    )
    graceful_shutdown_timeout: int = Field(
        default=300,
        description="Maximum seconds to wait for workers to finish on shutdown",
        validation_alias="AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT",
    )

    activity_message_create: str = Field(
        default="Waiting to start.",
        description="Default message when creating a new agent activity",
        validation_alias="AGENTEXEC_ACTIVITY_MESSAGE_CREATE",
    )
    activity_message_started: str = Field(
        default="Task started.",
        description="Default message when an agent activity starts execution",
        validation_alias="AGENTEXEC_ACTIVITY_MESSAGE_STARTED",
    )
    activity_message_complete: str = Field(
        default="Task completed successfully.",
        description="Default message when an agent activity completes successfully",
        validation_alias="AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE",
    )
    activity_message_error: str = Field(
        default="Task failed with error: {error}",
        description="Default message when an agent activity encounters an error",
        validation_alias="AGENTEXEC_ACTIVITY_MESSAGE_ERROR",
    )

    redis_url: str | None = Field(
        default=None,
        description="Redis connection URL",
        validation_alias=AliasChoices("AGENTEXEC_REDIS_URL", "REDIS_URL"),
    )
    redis_pool_size: int = Field(
        default=10,
        description="Redis connection pool size",
        validation_alias=AliasChoices("AGENTEXEC_REDIS_POOL_SIZE", "REDIS_POOL_SIZE"),
    )
    redis_pool_timeout: int = Field(
        default=5,
        description="Redis connection pool timeout in seconds",
        validation_alias=AliasChoices("AGENTEXEC_REDIS_POOL_TIMEOUT", "REDIS_POOL_TIMEOUT"),
    )

    result_ttl: int = Field(
        default=3600,
        description="TTL in seconds for task results",
        validation_alias="AGENTEXEC_RESULT_TTL",
    )

    state_backend: str = Field(
        default="agentexec.state.redis",
        description="State backend: 'agentexec.state.redis' or 'agentexec.state.kafka'",
        validation_alias="AGENTEXEC_STATE_BACKEND",
    )

    kafka_bootstrap_servers: str | None = Field(
        default=None,
        description="Kafka bootstrap servers (e.g. 'localhost:9092')",
        validation_alias=AliasChoices(
            "AGENTEXEC_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"
        ),
    )
    kafka_default_partitions: int = Field(
        default=6,
        description="Default number of partitions for auto-created topics",
        validation_alias="AGENTEXEC_KAFKA_DEFAULT_PARTITIONS",
    )
    kafka_replication_factor: int = Field(
        default=1,
        description="Replication factor for auto-created topics",
        validation_alias="AGENTEXEC_KAFKA_REPLICATION_FACTOR",
    )
    kafka_max_batch_size: int = Field(
        default=16384,
        description="Producer max batch size in bytes",
        validation_alias="AGENTEXEC_KAFKA_MAX_BATCH_SIZE",
    )
    kafka_linger_ms: int = Field(
        default=5,
        description="Producer linger time in milliseconds",
        validation_alias="AGENTEXEC_KAFKA_LINGER_MS",
    )
    kafka_retention_ms: int = Field(
        default=-1,
        description="Retention for compacted topics in ms (-1 = forever)",
        validation_alias="AGENTEXEC_KAFKA_RETENTION_MS",
    )

    key_prefix: str = Field(
        default="agentexec",
        description="Prefix for state backend keys",
        validation_alias="AGENTEXEC_KEY_PREFIX",
    )

    scheduler_poll_interval: int = Field(
        default=10,
        description="Seconds between schedule polls",
        validation_alias="AGENTEXEC_SCHEDULER_POLL_INTERVAL",
    )

    scheduler_timezone: str = Field(
        default="UTC",
        description=(
            "IANA timezone for cron schedule evaluation (e.g. 'America/New_York', 'UTC'). "
            "Set this so cron expressions read naturally in your local time."
        ),
        validation_alias="AGENTEXEC_SCHEDULER_TIMEZONE",
    )
    max_task_retries: int = Field(
        default=3,
        description=(
            "Maximum number of times a failed task will be retried before "
            "being marked as a permanent error. Set to 0 to disable retries. "
            "With the Kafka backend, retries preserve partition ordering — "
            "the task stays in its original position in the queue."
        ),
        validation_alias="AGENTEXEC_MAX_TASK_RETRIES",
    )

    lock_ttl: int = Field(
        default=1800,
        description=(
            "TTL in seconds for task lock keys (Redis backend only). "
            "Safety net for worker process death (OOM, SIGKILL) — "
            "locks are always explicitly released on task completion or error. "
            "Ignored by the Kafka backend (partition assignment handles isolation)."
        ),
        validation_alias="AGENTEXEC_LOCK_TTL",
    )


    @property
    def scheduler_tz(self) -> ZoneInfo:
        """Resolved ZoneInfo for the configured scheduler timezone."""
        return ZoneInfo(self.scheduler_timezone)


CONF = Config()
