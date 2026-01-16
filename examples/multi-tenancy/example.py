"""Multi-tenancy example demonstrating activity metadata filtering.

This example shows how to:
1. Attach metadata (like organization_id) when enqueueing tasks
2. Filter activities by metadata for tenant isolation
3. Use metadata in both list and detail views
"""

import asyncio
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import agentexec as ax


# --- Models ---


class DocumentContext(BaseModel):
    """Input context for document processing."""

    file_id: str
    filename: str


class ProcessedDocument(BaseModel):
    """Result of document processing."""

    file_id: str
    word_count: int
    summary: str


# --- Setup ---

# Create in-memory SQLite database for demo
engine = create_engine("sqlite:///multi_tenant_demo.db", echo=False)
SessionLocal = sessionmaker(bind=engine)

# Create tables
ax.Base.metadata.create_all(bind=engine)

# Create worker pool
pool = ax.Pool(engine=engine)


# --- Task Definition ---


@pool.task("process_document")
async def process_document(
    *,
    agent_id: UUID,
    context: DocumentContext,
) -> ProcessedDocument:
    """Simulate document processing."""
    # Simulate some work
    ax.activity.update(agent_id, "Extracting text...", percentage=25)
    await asyncio.sleep(0.1)

    ax.activity.update(agent_id, "Analyzing content...", percentage=50)
    await asyncio.sleep(0.1)

    ax.activity.update(agent_id, "Generating summary...", percentage=75)
    await asyncio.sleep(0.1)

    return ProcessedDocument(
        file_id=context.file_id,
        word_count=1234,
        summary=f"Summary of {context.filename}",
    )


# --- Demo Functions ---


async def enqueue_tasks_for_tenants():
    """Enqueue tasks for different organizations."""
    print("\n=== Enqueueing Tasks ===\n")

    # Organization A: Enqueue 2 tasks
    task1 = await ax.enqueue(
        "process_document",
        DocumentContext(file_id="doc-001", filename="report.pdf"),
        metadata={"organization_id": "org-A", "user_id": "user-1"},
    )
    print(f"Org A - Task 1: {task1.agent_id}")

    task2 = await ax.enqueue(
        "process_document",
        DocumentContext(file_id="doc-002", filename="invoice.pdf"),
        metadata={"organization_id": "org-A", "user_id": "user-2"},
    )
    print(f"Org A - Task 2: {task2.agent_id}")

    # Organization B: Enqueue 1 task
    task3 = await ax.enqueue(
        "process_document",
        DocumentContext(file_id="doc-003", filename="contract.pdf"),
        metadata={"organization_id": "org-B", "user_id": "user-3"},
    )
    print(f"Org B - Task 1: {task3.agent_id}")

    return task1, task2, task3


def list_activities_for_tenant(org_id: str):
    """List activities filtered by organization."""
    print(f"\n=== Activities for {org_id} ===\n")

    with SessionLocal() as session:
        result = ax.activity.list(
            session,
            metadata_filter={"organization_id": org_id},
        )

        print(f"Total activities: {result.total}")
        for item in result.items:
            print(f"  - {item.agent_id}: {item.agent_type} ({item.status})")
            print(f"    Metadata: {item.metadata}")


def get_activity_detail_with_tenant_check(agent_id: UUID, org_id: str):
    """Get activity detail with tenant validation."""
    print(f"\n=== Detail for {agent_id} (checking {org_id}) ===\n")

    with SessionLocal() as session:
        # This returns None if the activity doesn't belong to the org
        detail = ax.activity.detail(
            session,
            agent_id,
            metadata_filter={"organization_id": org_id},
        )

        if detail:
            print(f"Found: {detail.agent_type}")
            print(f"Metadata: {detail.metadata}")
            print(f"Logs: {len(detail.logs)} entries")
        else:
            print(f"Not found (or doesn't belong to {org_id})")


async def main():
    """Run the multi-tenancy demo."""
    print("Multi-Tenancy Demo with Activity Metadata")
    print("=" * 50)

    # 1. Enqueue tasks for different organizations
    task1, task2, task3 = await enqueue_tasks_for_tenants()

    # 2. List all activities (no filter)
    print("\n=== All Activities (no filter) ===\n")
    with SessionLocal() as session:
        all_activities = ax.activity.list(session)
        print(f"Total: {all_activities.total}")
        for item in all_activities.items:
            print(f"  - {item.agent_type}: {item.metadata}")

    # 3. List activities filtered by organization
    list_activities_for_tenant("org-A")  # Should show 2 tasks
    list_activities_for_tenant("org-B")  # Should show 1 task
    list_activities_for_tenant("org-C")  # Should show 0 tasks

    # 4. Detail view with tenant validation
    # Try to access Org A's task as Org A (should work)
    get_activity_detail_with_tenant_check(task1.agent_id, "org-A")

    # Try to access Org A's task as Org B (should return None)
    get_activity_detail_with_tenant_check(task1.agent_id, "org-B")

    # 5. Filter by multiple metadata fields
    print("\n=== Filter by org AND user ===\n")
    with SessionLocal() as session:
        result = ax.activity.list(
            session,
            metadata_filter={"organization_id": "org-A", "user_id": "user-1"},
        )
        print(f"Found {result.total} activities for org-A + user-1")

    print("\n" + "=" * 50)
    print("Demo complete!")


if __name__ == "__main__":
    asyncio.run(main())
