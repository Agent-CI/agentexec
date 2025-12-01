# agentexec-ui

React components for monitoring agentexec background agents. Provides a GitHub-inspired dark mode UI for tracking agent execution status and progress.

This is a **presentational component library** - you bring your own data fetching solution (we recommend [TanStack Query](https://tanstack.com/query)).

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
import { TaskList, TaskDetail, ActiveAgentsBadge } from 'agentexec-ui';
import type { ActivityListItem } from 'agentexec-ui';

function App() {
  // Use your preferred data fetching solution (TanStack Query, SWR, etc.)
  const { data, isLoading } = useYourDataFetching();

  return (
    <div>
      <ActiveAgentsBadge count={data?.activeCount ?? 0} loading={isLoading} />
      <TaskList
        items={data?.items || []}
        loading={isLoading}
        onTaskClick={(agentId) => console.log('Selected:', agentId)}
      />
    </div>
  );
}
```

## Components

### TaskList

Displays a list of agent tasks with status and progress.

```tsx
<TaskList
  items={activityList.items}
  loading={isLoading}
  onTaskClick={(agentId) => navigate(`/agents/${agentId}`)}
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
  onClose={() => navigate('/agents')}
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

## Styling

Components use CSS custom properties (CSS variables) for theming. You must provide your own stylesheet that defines these variables:

```css
:root {
  --ax-color-bg-primary: #0d1117;
  --ax-color-bg-secondary: #161b22;
  --ax-color-bg-tertiary: #21262d;
  --ax-color-border-default: #30363d;
  --ax-color-text-primary: #e6edf3;
  --ax-color-text-secondary: #8b949e;
  --ax-color-status-queued: #8b949e;
  --ax-color-status-running: #58a6ff;
  --ax-color-status-complete: #3fb950;
  --ax-color-status-error: #f85149;
  --ax-color-status-canceled: #f0883e;
  /* ... and more */
}
```

See the [example frontend](../../examples/openai-agents-fastapi/ui/src/styles/github-dark.css) for a complete GitHub-inspired dark theme implementation.

The example styles also include pagination classes (`.ax-pagination*`) compatible with [react-paginate](https://www.npmjs.com/package/react-paginate).

## TypeScript

Full TypeScript support with exported types:

```tsx
import type {
  Status,
  ActivityLog,
  ActivityDetail,
  ActivityListItem,
  ActivityList,
  ActiveCountResponse,
} from 'agentexec-ui';
```

## Data Fetching

This library does not include data fetching utilities. We recommend using [TanStack Query](https://tanstack.com/query) for data fetching with polling support.

See the [FastAPI example frontend](../../examples/openai-agents-fastapi/ui) for a complete implementation using TanStack Query.

## API Compatibility

The types in this library match the agentexec Python package API schemas:

- `ActivityList` - Response from `GET /api/agents/activity`
- `ActivityDetail` - Response from `GET /api/agents/activity/{agent_id}`
- `ActiveCountResponse` - Response from `GET /api/agents/active/count`

## License

MIT
