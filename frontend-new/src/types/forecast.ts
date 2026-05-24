// 预测相关类型定义

export interface ForecastRecord {
  station_id: string;
  equipment_id: string;
  forecast_time: string;
  target_hour: string;
  predicted_cooling_kwh: number;
  confidence_lower: number;
  confidence_upper: number;
  model_version: string;
  dt: string | null;
}

export interface ModelInfo {
  equipment_id: string;
  model_version: string;
  train_date: string;
  mape: number;
  r2: number;
  feature_importance?: Array<{
    feature: string;
    importance: number;
  }>;
}

export interface SystemForecastRecord {
  station_id: string;
  system_type: 'chiller' | 'heating' | 'cchp';
  equipment_id: string;
  forecast_time: string;
  target_hour: string;
  forecast_hour_offset?: number;
  predicted_supply_kwh: number;
  predicted_cooling_kwh?: number;
  predicted_heating_kwh?: number;
  predicted_electric_kwh?: number;
  predicted_energy_kwh?: number;
  confidence_lower?: number;
  confidence_upper?: number;
  recent_24_supply_kwh?: number | null;
  recent_24_operation_rate?: number | null;
  current_operation_status?: 'recent_active' | 'recent_inactive' | string;
  forecast_interpretation?: string;
  profile_sample_count?: number;
  model_version: string;
  algorithm?: string;
  feature_set?: string;
  train_test_split?: string;
  dt: string | null;
}

export interface SystemForecastMetric {
  station_id: string;
  system_type: 'chiller' | 'heating' | 'cchp';
  equipment_id: string;
  model_version: string;
  algorithm: string;
  target_col: string;
  feature_list?: string;
  feature_design?: string;
  window_design?: string;
  model_reason?: string;
  training_method?: string;
  evaluation_method?: string;
  train_start?: string;
  train_end?: string;
  test_start?: string;
  test_end?: string;
  train_row_count?: number;
  test_row_count?: number;
  mae?: number | null;
  rmse?: number | null;
  r2?: number | null;
  mape?: number | null;
  residual_std?: number | null;
  top_features?: string;
  result_summary?: string;
  dt?: string | null;
}
