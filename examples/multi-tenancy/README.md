# Multi-Tenancy with Activity Metadata

This example demonstrates how to use activity metadata for multi-tenant applications.

## Overview

When building multi-tenant applications, you often need to:
1. Associate background tasks with specific organizations/tenants
2. Filter activity views to only show tasks belonging to the current tenant
3. Ensure proper data isolation between tenants

The `metadata` parameter on `ax.enqueue()` and `pipeline.enqueue()` enables this by attaching arbitrary key-value pairs to activity records.

## Usage

### Enqueueing with Metadata

```python
import agentexec as ax

# Enqueue a task with organization context
task = await ax.enqueue(
    "process_document",
    DocumentContext(file_id="doc-123"),
    metadata={"organization_id": "org-456", "user_id": "user-789"}
)
```

### Filtering Activities by Metadata

```python
from agentexec import activity

# List only activities for a specific organization
activities = activity.list(
    session,
    metadata_filter={"organization_id": "org-456"}
)

# Get activity detail with tenant validation
# Returns None if the activity doesn't belong to this organization
detail = activity.detail(
    session,
    agent_id="...",
    metadata_filter={"organization_id": "org-456"}
)
```

### Pipeline Example

```python
pipeline = ax.Pipeline(pool)

class DocumentPipeline(pipeline.Base):
    @pipeline.step(0)
    async def extract(self, ctx: DocumentContext) -> ExtractedData:
        ...

# Enqueue pipeline with metadata
task = await pipeline.enqueue(
    context=DocumentContext(file_id="doc-123"),
    metadata={"organization_id": "org-456"}
)
```

## Running the Example

```bash
# Install dependencies
pip install agentexec

# Run the example
python example.py
```

## Database Schema

The metadata is stored as a JSON column on the `agentexec_activity` table:

```sql
ALTER TABLE agentexec_activity ADD COLUMN metadata JSON;
```

For PostgreSQL, this maps to JSONB which supports efficient filtering.

## Notes

- Metadata is immutable once set at enqueue time
- Filtering uses exact string matching on metadata values
- **Metadata is excluded from API serialization by default** to prevent accidental leakage of tenant info
- Access metadata programmatically via the `.metadata` attribute (e.g., `activity.metadata`)
- To include metadata in API responses, explicitly add it to your response model
