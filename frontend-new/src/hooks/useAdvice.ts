// 运行建议Hook

import { useState, useEffect } from 'react';
import { getAdvice } from '../api/advice';
import type { AdviceRecord } from '../types/advice';
import type { AdviceParams } from '../api/advice';

export function useAdvice(params: AdviceParams) {
  const { station_id, equipment_id, risk_level, advice_type, limit } = params;
  const [data, setData] = useState<AdviceRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchAdvice = async () => {
      try {
        setLoading(true);
        const result = await getAdvice({ station_id, equipment_id, risk_level, advice_type, limit });
        setData(result);
        setError(null);
      } catch (err) {
        setError(err as Error);
        console.error('Failed to fetch advice:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchAdvice();
  }, [station_id, equipment_id, risk_level, advice_type, limit]);

  return { data, loading, error };
}
