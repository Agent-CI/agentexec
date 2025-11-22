"""FastAPI application demonstrating agentexec integration with LangChain."""

import os
from collections.abc import Generator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
import agentexec as ax

from .views import router

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: setup and teardown."""
    print("✓ Activity tracking configured")
    print(f"✓ Redis URL: {ax.CONF.redis_url}")
    print(f"✓ Queue name: {ax.CONF.queue_name}")
    print(f"✓ Number of workers: {ax.CONF.num_workers}")

    yield

    # Cleanup: cancel any pending agents
    with SessionLocal() as db:
        try:
            canceled = ax.activity.cancel_pending(db)
            db.commit()
            print(f"✓ Canceled {canceled} pending agents")
        except Exception as e:
            db.rollback()
            print(f"✗ Error canceling pending agents: {e}")


# Create FastAPI app
app = FastAPI(
    title="AgentExec LangChain Example",
    description="Example FastAPI application using agentexec with LangChain agents",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
