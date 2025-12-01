"""Database configuration and utilities for LangChain example."""

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Database setup - users manage their own connection
# For PostgreSQL: "postgresql://user:password@localhost:5432/dbname"
# For SQLite: "sqlite:///agents.db"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///agents.db")

# Create engine and session factory (standard SQLAlchemy setup)
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Dependency: Get database session (standard FastAPI pattern)
def get_db() -> Generator[Session, None, None]:
    """Provide a database session for each request.

    This is the standard FastAPI pattern for database session management.
    Users have full control over connection pooling, timeouts, etc.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
