import type { ActivityList, ActivityDetail, ActiveCountResponse } from 'agentexec-ui';

/**
 * Fetch paginated activity list
 */
export async function fetchActivityList(page: number, pageSize: number): Promise<ActivityList> {
  const response = await fetch(`/api/agents/activity?page=${page}&page_size=${pageSize}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch activity list: ${response.status}`);
  }
  return response.json();
}

/**
 * Fetch activity detail for a specific agent
 */
export async function fetchActivityDetail(agentId: string): Promise<ActivityDetail> {
  const response = await fetch(`/api/agents/activity/${agentId}`);
  if (!response.ok) {
    if (response.status === 404) {
      throw new Error('Agent not found');
    }
    throw new Error(`Failed to fetch activity detail: ${response.status}`);
  }
  return response.json();
}

/**
 * Fetch count of active agents
 */
export async function fetchActiveCount(): Promise<ActiveCountResponse> {
  const response = await fetch('/api/agents/active/count');
  if (!response.ok) {
    throw new Error(`Failed to fetch active count: ${response.status}`);
  }
  return response.json();
}
