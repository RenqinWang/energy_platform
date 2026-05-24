import { useState, useEffect } from 'react';
import { getRevenueForecast } from '../api/report';
import type { RevenueForecastRecord } from '../api/report';

export function useRevenueForecast(
  equipmentId?: string,
  forecastDate?: string,
  limit: number = 1000
) {
  const [data, setData] = useState<RevenueForecastRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const result = await getRevenueForecast({
          equipment_id: equipmentId,
          forecast_date: forecastDate,
          limit,
        });
        setData(result);
        setError(null);
      } catch (err) {
        setError(err as Error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [equipmentId, forecastDate, limit]);

  return { data, loading, error };
}
