"""Test worker logging functionality."""

import logging
import time

import pytest
import fakeredis

from agentexec.worker.logging import (
    DEFAULT_FORMAT,
    LOG_CHANNEL,
    LOGGER_NAME,
    LogMessage,
    RedisLogHandler,
    get_worker_logger,
)


class TestLogMessage:
    """Tests for LogMessage schema."""

    def test_from_log_record(self):
        """Test creating LogMessage from a logging.LogRecord."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/path.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.processName = "TestProcess"
        record.process = 12345
        record.thread = 67890
        record.created = time.time()

        log_message = LogMessage.from_log_record(record)

        assert log_message.name == "test.logger"
        assert log_message.levelno == logging.INFO
        assert log_message.levelname == "INFO"
        assert log_message.msg == "Test message"
        assert log_message.processName == "TestProcess"
        assert log_message.process == 12345
        assert log_message.thread == 67890

    def test_to_log_record(self):
        """Test converting LogMessage back to logging.LogRecord."""
        original_time = time.time()
        log_message = LogMessage(
            name="test.logger",
            levelno=logging.WARNING,
            levelname="WARNING",
            msg="Warning message",
            processName="Worker-1",
            process=99999,
            thread=11111,
            created=original_time,
        )

        record = log_message.to_log_record()

        assert record.name == "test.logger"
        assert record.levelno == logging.WARNING
        assert record.msg == "Warning message"
        assert record.processName == "Worker-1"
        assert record.process == 99999
        assert record.created == original_time

    def test_roundtrip_log_record(self):
        """Test LogRecord -> LogMessage -> LogRecord roundtrip."""
        original = logging.LogRecord(
            name="roundtrip.test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error occurred",
            args=(),
            exc_info=None,
        )
        original.processName = "MainProcess"
        original.process = 11111
        original.thread = 22222
        original.created = time.time()

        # Convert to LogMessage and back
        log_message = LogMessage.from_log_record(original)
        restored = log_message.to_log_record()

        assert restored.name == original.name
        assert restored.levelno == original.levelno
        assert restored.msg == original.getMessage()
        assert restored.processName == original.processName
        assert restored.process == original.process
        assert restored.created == original.created

    def test_json_serialization(self):
        """Test LogMessage JSON serialization and deserialization."""
        log_message = LogMessage(
            name="test.json",
            levelno=logging.DEBUG,
            levelname="DEBUG",
            msg="Debug info",
            processName="Worker",
            process=1234,
            thread=5678,
            created=time.time(),
        )

        # Serialize to JSON
        json_str = log_message.model_dump_json()

        # Deserialize from JSON
        restored = LogMessage.model_validate_json(json_str)

        assert restored.name == log_message.name
        assert restored.levelno == log_message.levelno
        assert restored.msg == log_message.msg

    def test_log_message_with_none_values(self):
        """Test LogMessage handles None values for optional fields."""
        log_message = LogMessage(
            name="test.none",
            levelno=logging.INFO,
            levelname="INFO",
            msg="Message",
            processName="Process",
            process=None,
            thread=None,
            created=time.time(),
        )

        assert log_message.process is None
        assert log_message.thread is None


class TestRedisLogHandler:
    """Tests for RedisLogHandler."""

    @pytest.fixture
    def fake_redis_sync(self, monkeypatch):
        """Setup fake sync redis."""
        fake_redis = fakeredis.FakeRedis(decode_responses=False)

        def get_fake_redis_sync():
            return fake_redis

        monkeypatch.setattr(
            "agentexec.worker.logging.get_redis_sync", get_fake_redis_sync
        )

        return fake_redis

    def test_handler_initialization(self):
        """Test RedisLogHandler initializes with default channel."""
        handler = RedisLogHandler()
        assert handler.channel == LOG_CHANNEL

    def test_handler_custom_channel(self):
        """Test RedisLogHandler with custom channel."""
        handler = RedisLogHandler(channel="custom:logs")
        assert handler.channel == "custom:logs"

    def test_handler_emit(self, fake_redis_sync):
        """Test RedisLogHandler.emit() publishes to Redis."""
        handler = RedisLogHandler()

        # Subscribe to the channel to capture the message
        pubsub = fake_redis_sync.pubsub()
        pubsub.subscribe(LOG_CHANNEL)
        # Get the subscribe message
        pubsub.get_message()

        # Create and emit a log record
        record = logging.LogRecord(
            name="emit.test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Emitted message",
            args=(),
            exc_info=None,
        )

        handler.emit(record)

        # Get the published message
        message = pubsub.get_message()

        assert message is not None
        assert message["type"] == "message"
        assert message["channel"] == LOG_CHANNEL.encode()

        # Verify the message content
        log_message = LogMessage.model_validate_json(message["data"])
        assert log_message.msg == "Emitted message"
        assert log_message.levelno == logging.INFO


class TestGetWorkerLogger:
    """Tests for get_worker_logger function."""

    @pytest.fixture(autouse=True)
    def reset_logging_state(self, monkeypatch):
        """Reset the worker logging configured state before each test."""
        # Reset the global state
        monkeypatch.setattr("agentexec.worker.logging._worker_logging_configured", False)

        # Setup fake redis
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        monkeypatch.setattr(
            "agentexec.worker.logging.get_redis_sync", lambda: fake_redis
        )

        yield

        # Cleanup handlers added during tests
        root = logging.getLogger(LOGGER_NAME)
        root.handlers.clear()

    def test_get_worker_logger_returns_logger(self):
        """Test get_worker_logger returns a logger instance."""
        logger = get_worker_logger("test.module")

        assert isinstance(logger, logging.Logger)

    def test_get_worker_logger_namespaced(self):
        """Test get_worker_logger returns logger under agentexec namespace."""
        logger = get_worker_logger("mymodule")

        assert logger.name == f"{LOGGER_NAME}.mymodule"

    def test_get_worker_logger_existing_namespace(self):
        """Test get_worker_logger with existing namespace prefix."""
        logger = get_worker_logger(f"{LOGGER_NAME}.submodule")

        # Should not double-prefix
        assert logger.name == f"{LOGGER_NAME}.submodule"

    def test_get_worker_logger_configures_handler(self):
        """Test get_worker_logger adds RedisLogHandler on first call."""
        logger = get_worker_logger("first.call")

        root = logging.getLogger(LOGGER_NAME)
        handler_types = [type(h).__name__ for h in root.handlers]

        assert "RedisLogHandler" in handler_types

    def test_get_worker_logger_idempotent(self):
        """Test get_worker_logger only configures once."""
        # First call
        get_worker_logger("first")

        root = logging.getLogger(LOGGER_NAME)
        initial_handler_count = len(root.handlers)

        # Second call
        get_worker_logger("second")

        # Should not add more handlers
        assert len(root.handlers) == initial_handler_count


class TestConstants:
    """Tests for module constants."""

    def test_logger_name(self):
        """Test LOGGER_NAME constant."""
        assert LOGGER_NAME == "agentexec"

    def test_log_channel(self):
        """Test LOG_CHANNEL constant."""
        assert LOG_CHANNEL == "agentexec:logs"

    def test_default_format(self):
        """Test DEFAULT_FORMAT constant."""
        assert "[%(levelname)s/%(processName)s]" in DEFAULT_FORMAT
        assert "%(name)s" in DEFAULT_FORMAT
        assert "%(message)s" in DEFAULT_FORMAT
