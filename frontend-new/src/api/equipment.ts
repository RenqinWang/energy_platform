// 设备相关API

import client from './client';
import type { EquipmentStatus, SupplyCurve, DailyReport } from '../types/equipment';

export interface QueryParams {
  station_id?: string;
  equipment_id?: string;
  start_date?: string;
  end_date?: string;
  start_time?: string;
  end_time?: string;
  limit?: number;
}

// 获取站点列表
export const getStations = async (): Promise<string[]> => {
  const response = await client.get<unknown, { stations: string[] }>('/stations');
  return response.stations;
};

// 获取设备列表
export const getEquipment = async (station_id?: string): Promise<string[]> => {
  const response = await client.get<unknown, { equipment: string[] }>('/equipment', {
    params: { station_id }
  });
  return response.equipment;
};

// 获取设备状态（分钟级）
export const getEquipmentStatus = async (params: QueryParams): Promise<EquipmentStatus[]> => {
  return client.get<unknown, EquipmentStatus[]>('/equipment-status', { params });
};

// 获取供能曲线（小时级）
export const getSupplyCurve = async (params: QueryParams): Promise<SupplyCurve[]> => {
  return client.get<unknown, SupplyCurve[]>('/supply-curve', { params });
};

// 获取日报表
export const getDailyReport = async (params: QueryParams): Promise<DailyReport[]> => {
  return client.get<unknown, DailyReport[]>('/daily-report', { params });
};
