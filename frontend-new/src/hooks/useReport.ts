import { useState, useEffect } from 'react';
import { getDailyReport, getWeeklyReport, getMonthlyReport } from '../api/report';
import type { DailyReportRecord, WeeklyReportRecord, MonthlyReportRecord } from '../api/report';

export function useDailyReport(
  equipmentId?: string,
  startDate?: string,
  endDate?: string,
  limit: number = 100
) {
  const [data, setData] = useState<DailyReportRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const result = await getDailyReport({
          equipment_id: equipmentId,
          start_date: startDate,
          end_date: endDate,
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
  }, [equipmentId, startDate, endDate, limit]);

  return { data, loading, error };
}

export function useWeeklyReport(
  equipmentId?: string,
  startWeek?: string,
  endWeek?: string,
  limit: number = 100
) {
  const [data, setData] = useState<WeeklyReportRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const result = await getWeeklyReport({
          equipment_id: equipmentId,
          start_week: startWeek,
          end_week: endWeek,
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
  }, [equipmentId, startWeek, endWeek, limit]);

  return { data, loading, error };
}

export function useMonthlyReport(
  equipmentId?: string,
  startMonth?: string,
  endMonth?: string,
  limit: number = 100
) {
  const [data, setData] = useState<MonthlyReportRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const result = await getMonthlyReport({
          equipment_id: equipmentId,
          start_month: startMonth,
          end_month: endMonth,
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
  }, [equipmentId, startMonth, endMonth, limit]);

  return { data, loading, error };
}
