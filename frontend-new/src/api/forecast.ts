// 预测相关API

import client from './client';
import type { ForecastRecord, SystemForecastMetric, SystemForecastRecord } from '../types/forecast';
import type { SystemType } from './report';

export interface ForecastParams {
  station_id?: string;
  equipment_id?: string;
  hours?: number;
  limit?: number;
}

// 获取预测数据
export const getForecast = async (params: ForecastParams): Promise<ForecastRecord[]> => {
  return client.get<unknown, ForecastRecord[]>('/forecast', { params });
};

export const getSystemForecast = async (params: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  limit?: number;
}): Promise<SystemForecastRecord[]> => {
  return client.get<unknown, SystemForecastRecord[]>('/system-forecast', { params });
};

export const getSystemForecastMetrics = async (params: {
  system_type?: SystemType;
  station_id?: string;
  equipment_id?: string;
  limit?: number;
}): Promise<SystemForecastMetric[]> => {
  return client.get<unknown, SystemForecastMetric[]>('/system-forecast-metrics', { params });
};
