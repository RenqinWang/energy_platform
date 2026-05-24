// 设备相关类型定义

export interface EquipmentStatus {
  station_id: string;
  equipment_id: string;
  stat_time: string;
  supply_temp: number | null;
  return_temp: number | null;
  pressure: number | null;
  flow: number | null;
  power: number | null;
  runtime_hours: number | null;
  start_count: number | null;
  run_flag: number | null;
  record_count: number | null;
  dt: string | null;
}

export interface SupplyCurve {
  station_id: string;
  equipment_id: string;
  stat_hour: string;
  avg_supply_temp: number | null;
  max_supply_temp: number | null;
  min_supply_temp: number | null;
  avg_return_temp: number | null;
  avg_pressure: number | null;
  avg_flow: number | null;
  avg_power: number | null;
  run_minutes: number | null;
  energy_consumption_kwh: number | null;
  cooling_capacity_kw: number | null;
  cooling_supply_kwh: number | null;
  runtime_hours: number | null;
  operation_rate: number | null;
  record_count: number | null;
  dt: string | null;
}

export interface DailyReport {
  station_id: string;
  equipment_id: string;
  stat_date: string;
  peak_cooling_kwh: number | null;
  valley_cooling_kwh: number | null;
  peak_valley_ratio: number | null;
  peak_duration_hours: number | null;
  valley_duration_hours: number | null;
  avg_supply_temp: number | null;
  total_energy_consumption_kwh: number | null;
  total_cooling_supply_kwh: number | null;
  total_runtime_hours: number | null;
  avg_cooling_supply_kwh: number | null;
  total_run_minutes: number | null;
  daily_operation_rate: number | null;
  avg_cop: number | null;
  energy_cost: number | null;
  cooling_revenue: number | null;
  net_profit: number | null;
  hour_count: number | null;
  dt: string | null;
}
