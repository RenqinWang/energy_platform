import client from './client';
import type { DailyReport } from '../types/equipment';

export type DailyReportRecord = DailyReport;

export type SystemType = 'chiller' | 'heating' | 'cchp';

export interface SystemReportCommon {
  station_id: string;
  system_type: SystemType;
  equipment_id: string;
  peak_supply_kwh?: number;
  valley_supply_kwh?: number;
  peak_cooling_kwh?: number;
  valley_cooling_kwh?: number;
  peak_valley_ratio?: number;
  peak_duration_hours?: number;
  valley_duration_hours?: number;
  total_supply_kwh?: number;
  total_cooling_supply_kwh?: number;
  total_heating_supply_kwh?: number;
  total_electric_supply_kwh?: number;
  total_energy_consumption_kwh?: number;
  total_runtime_hours?: number;
  total_run_minutes?: number;
  daily_operation_rate?: number;
  avg_supply_kwh?: number;
  avg_cooling_supply_kwh?: number;
  avg_cop?: number;
  avg_supply_temp?: number;
  equipment_utilization_rate?: number;
  load_factor?: number;
  total_energy_cost?: number;
  total_supply_revenue?: number;
  energy_cost?: number;
  cooling_revenue?: number;
  total_cooling_revenue?: number;
  net_profit?: number;
  data_completeness_rate?: number;
  hour_count?: number;
  missing_supply_hours?: number;
  missing_energy_hours?: number;
  anomaly_count?: number;
  dt?: string;
}

export interface SystemSupplyCurveRecord {
  station_id: string;
  system_type: SystemType;
  equipment_id: string;
  stat_hour: string;
  avg_supply_temp?: number | null;
  avg_return_temp?: number | null;
  avg_pressure?: number | null;
  avg_flow?: number | null;
  avg_power?: number | null;
  energy_consumption_kwh?: number | null;
  energy_input_kwh?: number | null;
  cooling_capacity_kw?: number | null;
  supply_kwh?: number | null;
  cooling_supply_kwh?: number | null;
  heating_supply_kwh?: number | null;
  electric_supply_kwh?: number | null;
  runtime_hours?: number | null;
  run_minutes?: number | null;
  operation_rate?: number | null;
  record_count?: number | null;
  metric_quality?: string | null;
  dt?: string | null;
}

export interface SystemDailyReportRecord extends SystemReportCommon {
  stat_date: string;
}

export interface SystemWeeklyReportRecord extends SystemReportCommon {
  stat_week_str: string;
  week_start_date: string;
  week_end_date: string;
}

export interface SystemMonthlyReportRecord extends SystemReportCommon {
  stat_month_str: string;
  month_start_date: string;
  month_end_date: string;
  days_in_month?: number;
}

export interface WeeklyReportRecord {
  station_id: string;
  equipment_id: string;
  stat_week_str: string;
  week_start_date: string;
  week_end_date: string;
  peak_cooling_kwh?: number;
  valley_cooling_kwh?: number;
  peak_valley_ratio?: number;
  peak_duration_hours?: number;
  valley_duration_hours?: number;
  total_cooling_supply_kwh?: number;
  total_energy_consumption_kwh?: number;
  total_runtime_hours?: number;
  avg_cooling_supply_kwh?: number;
  avg_cop?: number;
  avg_supply_temp?: number;
  equipment_utilization_rate?: number;
  load_factor?: number;
  total_energy_cost?: number;
  total_cooling_revenue?: number;
  net_profit?: number;
  data_completeness_rate?: number;
  hour_count?: number;
  dt?: string;
}

export interface MonthlyReportRecord {
  station_id: string;
  equipment_id: string;
  stat_month_str: string;
  month_start_date: string;
  month_end_date: string;
  days_in_month?: number;
  peak_cooling_kwh?: number;
  valley_cooling_kwh?: number;
  peak_valley_ratio?: number;
  peak_duration_hours?: number;
  valley_duration_hours?: number;
  total_cooling_supply_kwh?: number;
  total_energy_consumption_kwh?: number;
  total_runtime_hours?: number;
  avg_cooling_supply_kwh?: number;
  avg_cop?: number;
  avg_supply_temp?: number;
  equipment_utilization_rate?: number;
  load_factor?: number;
  total_energy_cost?: number;
  total_cooling_revenue?: number;
  net_profit?: number;
  data_completeness_rate?: number;
  hour_count?: number;
  dt?: string;
}

export interface RevenueForecastRecord {
  station_id: string;
  equipment_id: string;
  forecast_date: string;
  target_hour: string;
  forecast_hour?: number;
  predicted_cooling_kwh?: number;
  predicted_energy_kwh?: number;
  energy_price?: number;
  cooling_price?: number;
  predicted_energy_cost?: number;
  predicted_cooling_revenue?: number;
  predicted_profit?: number;
  profit_margin?: number;
  model_version?: string;
  dt?: string;
}

export interface SystemRevenueForecastRecord {
  station_id: string;
  system_type: SystemType;
  equipment_id: string;
  forecast_date: string;
  target_hour: string;
  forecast_hour?: number;
  predicted_supply_kwh?: number;
  predicted_cooling_kwh?: number;
  predicted_heating_kwh?: number;
  predicted_electric_kwh?: number;
  predicted_energy_kwh?: number;
  energy_price?: number;
  cooling_price?: number;
  heating_price?: number;
  predicted_energy_cost?: number;
  predicted_supply_revenue?: number;
  predicted_profit?: number;
  profit_margin?: number;
  model_version?: string;
  algorithm?: string;
  dt?: string;
  created_at?: string;
}

export const getWeeklyReport = async (params?: {
  station_id?: string;
  equipment_id?: string;
  start_week?: string;
  end_week?: string;
  limit?: number;
}): Promise<WeeklyReportRecord[]> => {
  return client.get<unknown, WeeklyReportRecord[]>('/weekly-report', { params });
};

export const getDailyReport = async (params?: {
  station_id?: string;
  equipment_id?: string;
  start_date?: string;
  end_date?: string;
  limit?: number;
}): Promise<DailyReportRecord[]> => {
  return client.get<unknown, DailyReportRecord[]>('/daily-report', { params });
};

export const getMonthlyReport = async (params?: {
  station_id?: string;
  equipment_id?: string;
  start_month?: string;
  end_month?: string;
  limit?: number;
}): Promise<MonthlyReportRecord[]> => {
  return client.get<unknown, MonthlyReportRecord[]>('/monthly-report', { params });
};

export const getRevenueForecast = async (params?: {
  station_id?: string;
  equipment_id?: string;
  forecast_date?: string;
  limit?: number;
}): Promise<RevenueForecastRecord[]> => {
  return client.get<unknown, RevenueForecastRecord[]>('/revenue-forecast', { params });
};

export const getSystemRevenueForecast = async (params?: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  forecast_date?: string;
  limit?: number;
}): Promise<SystemRevenueForecastRecord[]> => {
  return client.get<unknown, SystemRevenueForecastRecord[]>('/system-revenue-forecast', { params });
};

export const getSystemEquipment = async (params?: {
  system_type?: SystemType;
  station_id?: string;
}): Promise<string[]> => {
  const response = await client.get<unknown, { equipment: string[] }>('/system-equipment', { params });
  return response.equipment;
};

export const getSystemDailyReport = async (params?: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  start_date?: string;
  end_date?: string;
  limit?: number;
}): Promise<SystemDailyReportRecord[]> => {
  return client.get<unknown, SystemDailyReportRecord[]>('/system-daily-report', { params });
};

export const getSystemSupplyCurve = async (params?: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  start_date?: string;
  end_date?: string;
  limit?: number;
}): Promise<SystemSupplyCurveRecord[]> => {
  return client.get<unknown, SystemSupplyCurveRecord[]>('/system-supply-curve', { params });
};

export const getSystemWeeklyReport = async (params?: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  start_week?: string;
  end_week?: string;
  limit?: number;
}): Promise<SystemWeeklyReportRecord[]> => {
  return client.get<unknown, SystemWeeklyReportRecord[]>('/system-weekly-report', { params });
};

export const getSystemMonthlyReport = async (params?: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  start_month?: string;
  end_month?: string;
  limit?: number;
}): Promise<SystemMonthlyReportRecord[]> => {
  return client.get<unknown, SystemMonthlyReportRecord[]>('/system-monthly-report', { params });
};
