"""Test configuration handling."""

import os

import pytest

from agentexec.config import CONF, Config


class TestConfig:
    """Tests for Config class."""

    def test_default_table_prefix(self):
        """Test default table prefix."""
        config = Config()
        assert config.table_prefix == "agentexec_"

    def test_default_queue_name(self):
        """Test default queue name."""
        config = Config()
        assert config.queue_name == "agentexec_tasks"

    def test_default_num_workers(self):
        """Test default number of workers."""
        config = Config()
        assert config.num_workers == 4

    def test_default_graceful_shutdown_timeout(self):
        """Test default graceful shutdown timeout."""
        config = Config()
        assert config.graceful_shutdown_timeout == 300

    def test_default_redis_url_is_none(self):
        """Test default Redis URL is None."""
        config = Config()
        assert config.redis_url is None

    def test_default_redis_pool_size(self):
        """Test default Redis pool size."""
        config = Config()
        assert config.redis_pool_size == 10

    def test_default_redis_pool_timeout(self):
        """Test default Redis pool timeout."""
        config = Config()
        assert config.redis_pool_timeout == 5

    def test_default_result_ttl(self):
        """Test default result TTL."""
        config = Config()
        assert config.result_ttl == 3600

    def test_default_activity_messages(self):
        """Test default activity messages."""
        config = Config()
        assert config.activity_message_create == "Waiting to start."
        assert config.activity_message_started == "Task started."
        assert config.activity_message_complete == "Task completed successfully."
        assert "{error}" in config.activity_message_error


class TestConfigEnvironmentVariables:
    """Tests for configuration from environment variables."""

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Clean up environment variables after each test."""
        env_vars = [
            "AGENTEXEC_TABLE_PREFIX",
            "AGENTEXEC_QUEUE_NAME",
            "AGENTEXEC_NUM_WORKERS",
            "AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT",
            "AGENTEXEC_REDIS_URL",
            "REDIS_URL",
            "AGENTEXEC_REDIS_POOL_SIZE",
            "REDIS_POOL_SIZE",
            "AGENTEXEC_REDIS_POOL_TIMEOUT",
            "REDIS_POOL_TIMEOUT",
            "AGENTEXEC_RESULT_TTL",
            "AGENTEXEC_ACTIVITY_MESSAGE_CREATE",
            "AGENTEXEC_ACTIVITY_MESSAGE_STARTED",
            "AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE",
            "AGENTEXEC_ACTIVITY_MESSAGE_ERROR",
        ]

        # Save original values
        original = {var: os.environ.get(var) for var in env_vars}

        yield

        # Restore original values
        for var, value in original.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

    def test_num_workers_from_env(self):
        """Test num_workers from environment variable."""
        os.environ["AGENTEXEC_NUM_WORKERS"] = "8"
        config = Config()
        assert config.num_workers == 8

    def test_table_prefix_from_env(self):
        """Test table_prefix from environment variable."""
        os.environ["AGENTEXEC_TABLE_PREFIX"] = "custom_"
        config = Config()
        assert config.table_prefix == "custom_"

    def test_queue_name_from_env(self):
        """Test queue_name from environment variable."""
        os.environ["AGENTEXEC_QUEUE_NAME"] = "my_queue"
        config = Config()
        assert config.queue_name == "my_queue"

    def test_graceful_shutdown_timeout_from_env(self):
        """Test graceful_shutdown_timeout from environment variable."""
        os.environ["AGENTEXEC_GRACEFUL_SHUTDOWN_TIMEOUT"] = "600"
        config = Config()
        assert config.graceful_shutdown_timeout == 600

    def test_redis_url_from_agentexec_env(self):
        """Test redis_url from AGENTEXEC_REDIS_URL."""
        os.environ["AGENTEXEC_REDIS_URL"] = "redis://custom:6379"
        config = Config()
        assert config.redis_url == "redis://custom:6379"

    def test_redis_url_from_generic_env(self):
        """Test redis_url from REDIS_URL (alias)."""
        os.environ["REDIS_URL"] = "redis://generic:6379"
        config = Config()
        assert config.redis_url == "redis://generic:6379"

    def test_agentexec_redis_url_takes_precedence(self):
        """Test AGENTEXEC_REDIS_URL takes precedence over REDIS_URL."""
        os.environ["AGENTEXEC_REDIS_URL"] = "redis://agentexec:6379"
        os.environ["REDIS_URL"] = "redis://generic:6379"
        config = Config()
        assert config.redis_url == "redis://agentexec:6379"

    def test_redis_pool_size_from_env(self):
        """Test redis_pool_size from environment variable."""
        os.environ["AGENTEXEC_REDIS_POOL_SIZE"] = "20"
        config = Config()
        assert config.redis_pool_size == 20

    def test_redis_pool_timeout_from_env(self):
        """Test redis_pool_timeout from environment variable."""
        os.environ["AGENTEXEC_REDIS_POOL_TIMEOUT"] = "10"
        config = Config()
        assert config.redis_pool_timeout == 10

    def test_result_ttl_from_env(self):
        """Test result_ttl from environment variable."""
        os.environ["AGENTEXEC_RESULT_TTL"] = "7200"
        config = Config()
        assert config.result_ttl == 7200

    def test_activity_message_create_from_env(self):
        """Test activity_message_create from environment variable."""
        os.environ["AGENTEXEC_ACTIVITY_MESSAGE_CREATE"] = "Custom waiting message"
        config = Config()
        assert config.activity_message_create == "Custom waiting message"

    def test_activity_message_started_from_env(self):
        """Test activity_message_started from environment variable."""
        os.environ["AGENTEXEC_ACTIVITY_MESSAGE_STARTED"] = "Custom started"
        config = Config()
        assert config.activity_message_started == "Custom started"

    def test_activity_message_complete_from_env(self):
        """Test activity_message_complete from environment variable."""
        os.environ["AGENTEXEC_ACTIVITY_MESSAGE_COMPLETE"] = "Custom complete"
        config = Config()
        assert config.activity_message_complete == "Custom complete"

    def test_activity_message_error_from_env(self):
        """Test activity_message_error from environment variable."""
        os.environ["AGENTEXEC_ACTIVITY_MESSAGE_ERROR"] = "Error: {error}"
        config = Config()
        assert config.activity_message_error == "Error: {error}"


class TestGlobalConfig:
    """Tests for the global CONF instance."""

    def test_conf_is_config_instance(self):
        """Test that CONF is a Config instance."""
        assert isinstance(CONF, Config)

    def test_conf_has_expected_attributes(self):
        """Test that CONF has all expected attributes."""
        assert hasattr(CONF, "table_prefix")
        assert hasattr(CONF, "queue_name")
        assert hasattr(CONF, "num_workers")
        assert hasattr(CONF, "graceful_shutdown_timeout")
        assert hasattr(CONF, "redis_url")
        assert hasattr(CONF, "redis_pool_size")
        assert hasattr(CONF, "redis_pool_timeout")
        assert hasattr(CONF, "result_ttl")
        assert hasattr(CONF, "activity_message_create")
        assert hasattr(CONF, "activity_message_started")
        assert hasattr(CONF, "activity_message_complete")
        assert hasattr(CONF, "activity_message_error")
