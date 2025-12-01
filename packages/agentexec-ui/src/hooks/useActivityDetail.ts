import { useState, useEffect, useCallback } from 'react';
import type { ActivityDetail } from '../types';

interface UseActivityDetailOptions {
  baseUrl?: string;
  agentId: string | null;
  pollInterval?: number;
}

interface UseActivityDetailResult {
  data: ActivityDetail | null;
  loading: boolean;
  error: Error | null;
  refetch: () => void;
}

/**
 * Hook to fetch detailed activity information for a specific agent
 */
export function useActivityDetail(options: UseActivityDetailOptions): UseActivityDetailResult {
  const { baseUrl = '', agentId, pollInterval } = options;

  const [data, setData] = useState<ActivityDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchData = useCallback(async () => {
    if (!agentId) {
      setData(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const url = `${baseUrl}/api/agents/activity/${agentId}`;
      const response = await fetch(url);

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Agent not found');
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();
      setData(result);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e : new Error('Unknown error'));
    } finally {
      setLoading(false);
    }
  }, [baseUrl, agentId]);

  useEffect(() => {
    fetchData();

    if (pollInterval && pollInterval > 0 && agentId) {
      const interval = setInterval(fetchData, pollInterval);
      return () => clearInterval(interval);
    }
  }, [fetchData, pollInterval, agentId]);

  return { data, loading, error, refetch: fetchData };
}
