from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    table_prefix: str = Field(
        default="agentexec_",
        description="Prefix for database table names",
        validation_alias="AGENTEXEC_TABLE_PREFIX",
    )
    queue_name: str = Field(
        default="agentexec_tasks",
        description="Name of the Redis list to use as task queue",
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
        description="TTL in seconds for task results in Redis",
        validation_alias="AGENTEXEC_RESULT_TTL",
    )

    state_backend: str = Field(
        default="redis",
        description="State backend to use (redis, memory, etc.)",
        validation_alias="AGENTEXEC_STATE_BACKEND",
    )

    key_prefix: str = Field(
        default="agentexec",
        description="Prefix for state backend keys",
        validation_alias="AGENTEXEC_KEY_PREFIX",
    )


CONF = Config()
