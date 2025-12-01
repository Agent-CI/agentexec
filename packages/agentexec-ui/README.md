# agentexec-ui

React components for monitoring agentexec background agents. Provides a GitHub-inspired dark mode UI for tracking agent execution status and progress.

## Installation

```bash
npm install agentexec-ui
# or
yarn add agentexec-ui
# or
pnpm add agentexec-ui
```

## Quick Start

```tsx
import {
  TaskList,
  TaskDetail,
  ActiveAgentsBadge,
  Pagination,
  useActivityList,
  useActivityDetail,
  useActiveCount,
} from 'agentexec-ui';

// Import the GitHub dark theme
import 'agentexec-ui/styles';

function App() {
  const { count } = useActiveCount({ pollInterval: 15000 });
  const { data } = useActivityList({ page: 1, pageSize: 20 });

  return (
    <div>
      <ActiveAgentsBadge count={count} />
      <TaskList items={data?.items || []} />
    </div>
  );
}
```

## Components

### TaskList

Displays a paginated list of agent tasks with status and progress.

```tsx
<TaskList
  items={activityList.items}
  loading={isLoading}
  onTaskClick={(agentId) => setSelectedId(agentId)}
  selectedAgentId={selectedId}
/>
```

### TaskDetail

Shows detailed information about a specific agent including full log history.

```tsx
<TaskDetail
  activity={activityDetail}
  loading={isLoading}
  error={error}
  onClose={() => setSelectedId(null)}
/>
```

### ActiveAgentsBadge

Displays the count of currently active (queued or running) agents.

```tsx
<ActiveAgentsBadge count={activeCount} loading={isLoading} />
```

### StatusBadge

Shows the status of an agent with appropriate styling.

```tsx
<StatusBadge status="running" />
```

### ProgressBar

Displays completion progress for an agent.

```tsx
<ProgressBar percentage={75} status="running" />
```

### Pagination

Navigation component for paginated lists.

```tsx
<Pagination
  page={currentPage}
  totalPages={totalPages}
  onPageChange={setCurrentPage}
/>
```

## Hooks

### useActivityList

Fetches paginated activity list with optional polling.

```tsx
const { data, loading, error, refetch } = useActivityList({
  baseUrl: '',        // API base URL (default: '')
  page: 1,            // Page number
  pageSize: 50,       // Items per page
  pollInterval: 15000 // Poll interval in ms (optional)
});
```

### useActivityDetail

Fetches detailed activity for a specific agent.

```tsx
const { data, loading, error, refetch } = useActivityDetail({
  baseUrl: '',
  agentId: 'uuid-here',
  pollInterval: 5000
});
```

### useActiveCount

Fetches the count of active agents.

```tsx
const { count, loading, error, refetch } = useActiveCount({
  baseUrl: '',
  pollInterval: 15000
});
```

## Styling

The package includes a GitHub-inspired dark theme. Import it in your app:

```tsx
import 'agentexec-ui/styles';
```

All components use CSS custom properties (CSS variables) that you can override:

```css
:root {
  --ax-color-bg-primary: #0d1117;
  --ax-color-bg-secondary: #161b22;
  --ax-color-text-primary: #e6edf3;
  --ax-color-status-running: #58a6ff;
  /* ... see github-dark.css for all variables */
}
```

## TypeScript

Full TypeScript support with exported types:

```tsx
import type {
  Status,
  ActivityLog,
  ActivityDetail,
  ActivityListItem,
  ActivityList,
} from 'agentexec-ui';
```

## API Requirements

This library expects your API to provide the following endpoints:

- `GET /api/agents/activity` - List activities (paginated)
- `GET /api/agents/activity/{agent_id}` - Get activity detail
- `GET /api/agents/active/count` - Get active agent count

See the [agentexec FastAPI example](../../examples/openai-agents-fastapi) for reference implementations.

## License

MIT
