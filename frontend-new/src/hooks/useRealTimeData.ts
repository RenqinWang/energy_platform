// 实时数据更新Hook

import { useState, useEffect, useCallback, useRef } from 'react';

interface UseRealTimeDataOptions {
  interval?: number;
  enabled?: boolean;
}

export function useRealTimeData<T>(
  apiCall: () => Promise<T>,
  options: UseRealTimeDataOptions = {}
) {
  const { interval = 30000, enabled = true } = options;

  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const result = await apiCall();
      setData(result);
      setError(null);
    } catch (err) {
      setError(err as Error);
      console.error('Failed to fetch data:', err);
    } finally {
      setLoading(false);
    }
  }, [apiCall]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    queueMicrotask(() => {
      void fetchData();
    });

    if (interval > 0) {
      timerRef.current = setInterval(fetchData, interval);
    }

    return () => {
      if (timerRef.current) {
        clearInterval(timerRef.current);
      }
    };
  }, [fetchData, interval, enabled]);

  const refresh = useCallback(() => {
    setLoading(true);
    void fetchData();
  }, [fetchData]);

  return { data, loading, error, refresh };
}
