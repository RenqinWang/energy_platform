// 预测数据Hook

import { useState, useEffect } from 'react';
import { getForecast } from '../api/forecast';
import type { ForecastRecord } from '../types/forecast';
import type { ForecastParams } from '../api/forecast';

export function useForecast(params: ForecastParams) {
  const { station_id, equipment_id, hours, limit } = params;
  const [data, setData] = useState<ForecastRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchForecast = async () => {
      try {
        setLoading(true);
        const result = await getForecast({ station_id, equipment_id, hours, limit });
        setData(result);
        setError(null);
      } catch (err) {
        setError(err as Error);
        console.error('Failed to fetch forecast:', err);
      } finally {
        setLoading(false);
      }
    };

    if (equipment_id) {
      fetchForecast();
    }
  }, [station_id, equipment_id, hours, limit]);

  return { data, loading, error };
}
