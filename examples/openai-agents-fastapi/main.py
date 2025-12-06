"""FastAPI application demonstrating agentexec integration."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
import agentexec as ax

from db import SessionLocal
from views import router


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
            print(f"✓ Canceled {canceled} pending agents")
        except Exception as e:
            print(f"✗ Error canceling pending agents: {e}")


# Create FastAPI app
app = FastAPI(
    title="AgentExec Example",
    description="Example FastAPI application using agentexec for background agent orchestration",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
