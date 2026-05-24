// 运行建议相关API

import client from './client';
import type { AdviceRecord } from '../types/advice';

export interface AdviceParams {
  station_id?: string;
  equipment_id?: string;
  risk_level?: 'low' | 'medium' | 'high';
  advice_type?: 'load_change' | 'anomaly' | 'efficiency' | 'economic';
  limit?: number;
}

// 获取运行建议
export const getAdvice = async (params: AdviceParams): Promise<AdviceRecord[]> => {
  return client.get<unknown, AdviceRecord[]>('/advice', { params });
};
