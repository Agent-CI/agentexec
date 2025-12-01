import { useState, useEffect, useCallback } from 'react';
import type { ActivityList } from '../types';

interface UseActivityListOptions {
  baseUrl?: string;
  page?: number;
  pageSize?: number;
  pollInterval?: number;
}

interface UseActivityListResult {
  data: ActivityList | null;
  loading: boolean;
  error: Error | null;
  refetch: () => void;
}

/**
 * Hook to fetch paginated activity list from the API
 */
export function useActivityList(options: UseActivityListOptions = {}): UseActivityListResult {
  const {
    baseUrl = '',
    page = 1,
    pageSize = 50,
    pollInterval,
  } = options;

  const [data, setData] = useState<ActivityList | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const url = `${baseUrl}/api/agents/activity?page=${page}&page_size=${pageSize}`;
      const response = await fetch(url);

      if (!response.ok) {
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
  }, [baseUrl, page, pageSize]);

  useEffect(() => {
    fetchData();

    if (pollInterval && pollInterval > 0) {
      const interval = setInterval(fetchData, pollInterval);
      return () => clearInterval(interval);
    }
  }, [fetchData, pollInterval]);

  return { data, loading, error, refetch: fetchData };
}
