// 格式化工具函数

import dayjs from 'dayjs';

// 格式化日期时间
export const formatDateTime = (dateStr: string, format: string = 'YYYY-MM-DD HH:mm:ss'): string => {
  return dayjs(dateStr).format(format);
};

// 格式化日期
export const formatDate = (dateStr: string): string => {
  return dayjs(dateStr).format('YYYY-MM-DD');
};

// 格式化时间
export const formatTime = (dateStr: string): string => {
  return dayjs(dateStr).format('HH:mm:ss');
};

// 格式化数字（保留小数位）
export const formatNumber = (num: number | null | undefined, decimals: number = 2): string => {
  if (num === null || num === undefined) return '-';
  return num.toFixed(decimals);
};

// 格式化百分比
export const formatPercent = (num: number | null | undefined, decimals: number = 1): string => {
  if (num === null || num === undefined) return '-';
  return `${(num * 100).toFixed(decimals)}%`;
};

// 格式化能耗（kWh）
export const formatEnergy = (kwh: number | null | undefined): string => {
  if (kwh === null || kwh === undefined) return '-';
  if (kwh >= 1000) {
    return `${(kwh / 1000).toFixed(2)} MWh`;
  }
  return `${kwh.toFixed(2)} kWh`;
};

// 格式化温度
export const formatTemperature = (temp: number | null | undefined): string => {
  if (temp === null || temp === undefined) return '-';
  return `${temp.toFixed(1)}℃`;
};

// 格式化流量
export const formatFlow = (flow: number | null | undefined): string => {
  if (flow === null || flow === undefined) return '-';
  return `${flow.toFixed(2)} m³/h`;
};

// 格式化压力
export const formatPressure = (pressure: number | null | undefined): string => {
  if (pressure === null || pressure === undefined) return '-';
  return `${pressure.toFixed(3)} MPa`;
};

// 格式化功率
export const formatPower = (power: number | null | undefined): string => {
  if (power === null || power === undefined) return '-';
  return `${power.toFixed(2)} kW`;
};

// 计算时间范围
export const getTimeRange = (hours: number): { start_time: string; end_time: string } => {
  const end = dayjs();
  const start = end.subtract(hours, 'hour');
  return {
    start_time: start.format('YYYY-MM-DD HH:mm:ss'),
    end_time: end.format('YYYY-MM-DD HH:mm:ss')
  };
};

// 计算日期范围
export const getDateRange = (days: number): { start_date: string; end_date: string } => {
  const end = dayjs();
  const start = end.subtract(days, 'day');
  return {
    start_date: start.format('YYYY-MM-DD'),
    end_date: end.format('YYYY-MM-DD')
  };
};
