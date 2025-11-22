"""Database configuration for agent-runner.

This module provides the declarative base class for all SQLAlchemy models.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models in agent-runner.

    Users can reference this base in their Alembic migrations to include
    agent-runner's tables alongside their own application tables.

    Example:
        # In alembic/env.py
        import agentexec as ax
        from models import Base

        target_metadata = [Base.metadata, ax.Base.metadata]
    """

    pass
