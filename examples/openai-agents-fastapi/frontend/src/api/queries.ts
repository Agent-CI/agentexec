import { useQuery } from '@tanstack/react-query';
import { fetchActivityList, fetchActivityDetail, fetchActiveCount } from './agents';

const POLL_INTERVAL = 15000; // 15 seconds
const DETAIL_POLL_INTERVAL = 5000; // 5 seconds

/**
 * Query key factory for activity queries
 */
export const activityKeys = {
  all: ['activity'] as const,
  lists: () => [...activityKeys.all, 'list'] as const,
  list: (page: number, pageSize: number) => [...activityKeys.lists(), { page, pageSize }] as const,
  details: () => [...activityKeys.all, 'detail'] as const,
  detail: (agentId: string) => [...activityKeys.details(), agentId] as const,
  activeCount: () => [...activityKeys.all, 'activeCount'] as const,
};

/**
 * Hook to fetch paginated activity list
 */
export function useActivityList(page: number, pageSize: number) {
  return useQuery({
    queryKey: activityKeys.list(page, pageSize),
    queryFn: () => fetchActivityList(page, pageSize),
    refetchInterval: POLL_INTERVAL,
  });
}

/**
 * Hook to fetch activity detail for a specific agent
 */
export function useActivityDetail(agentId: string | null) {
  return useQuery({
    queryKey: activityKeys.detail(agentId ?? ''),
    queryFn: () => fetchActivityDetail(agentId!),
    enabled: !!agentId,
    refetchInterval: DETAIL_POLL_INTERVAL,
  });
}

/**
 * Hook to fetch count of active agents
 */
export function useActiveCount() {
  return useQuery({
    queryKey: activityKeys.activeCount(),
    queryFn: fetchActiveCount,
    refetchInterval: POLL_INTERVAL,
  });
}
