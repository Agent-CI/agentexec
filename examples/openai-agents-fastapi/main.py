"""FastAPI application demonstrating agentexec integration."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import agentexec as ax

from db import SessionLocal
from views import router

# Check if frontend build exists
FRONTEND_DIR = Path(__file__).parent / "ui" / "dist"
SERVE_FRONTEND = FRONTEND_DIR.exists() and os.getenv("SERVE_FRONTEND", "true").lower() != "false"


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
    title="AgentExec Example",
    description="Example FastAPI application using agentexec for background agent orchestration",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)

# Serve frontend in production (when built)
if SERVE_FRONTEND:
    # Serve static assets
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/")
    async def serve_frontend():
        """Serve the React frontend."""
        return FileResponse(FRONTEND_DIR / "index.html")

    @app.get("/{path:path}")
    async def serve_frontend_routes(path: str):
        """Serve the React frontend for all non-API routes (SPA routing)."""
        # Check if file exists in dist directory
        file_path = FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        # Fall back to index.html for SPA routing
        return FileResponse(FRONTEND_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
