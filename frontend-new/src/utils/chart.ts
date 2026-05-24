// ECharts图表配置工具

import type { EChartsOption } from 'echarts';
import { CHART_COLORS } from './constants';
import type { ForecastRecord } from '../types/forecast';

// 创建基础折线图配置
export const createLineChartOption = (
  title: string,
  xAxisData: string[],
  series: Array<{ name: string; data: number[] }>,
  yAxisName: string = ''
): EChartsOption => {
  return {
    title: { text: title, left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' }
    },
    legend: {
      data: series.map(s => s.name),
      top: 30
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: xAxisData
    },
    yAxis: {
      type: 'value',
      name: yAxisName
    },
    series: series.map((s, index) => ({
      name: s.name,
      type: 'line',
      data: s.data,
      smooth: true,
      itemStyle: { color: CHART_COLORS[index % CHART_COLORS.length] }
    })),
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { start: 0, end: 100 }
    ]
  };
};

// 创建预测图表配置
export const createForecastChartOption = (
  historicalData: Array<[string, number]>,
  forecastData: ForecastRecord[]
): EChartsOption => {
  return {
    title: { text: '供冷量预测', left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' }
    },
    legend: {
      data: ['历史数据', '预测值', '置信区间'],
      top: 30
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: {
      type: 'time',
      axisLabel: {
        formatter: '{HH}:{mm}'
      }
    },
    yAxis: {
      type: 'value',
      name: '供冷量 (kWh)'
    },
    series: [
      {
        name: '历史数据',
        type: 'line',
        data: historicalData,
        itemStyle: { color: '#1890ff' },
        lineStyle: { width: 2 }
      },
      {
        name: '预测值',
        type: 'line',
        data: forecastData.map(d => [d.target_hour, d.predicted_cooling_kwh]),
        itemStyle: { color: '#52c41a' },
        lineStyle: { type: 'dashed', width: 2 }
      },
      {
        name: '置信区间上界',
        type: 'line',
        data: forecastData.map(d => [d.target_hour, d.confidence_upper]),
        lineStyle: { opacity: 0 },
        areaStyle: { opacity: 0.2, color: '#52c41a' },
        stack: 'confidence',
        symbol: 'none',
        showSymbol: false
      },
      {
        name: '置信区间下界',
        type: 'line',
        data: forecastData.map(d => [d.target_hour, d.confidence_lower]),
        lineStyle: { opacity: 0 },
        areaStyle: { opacity: 0.2, color: '#52c41a' },
        stack: 'confidence',
        symbol: 'none',
        showSymbol: false
      }
    ],
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { start: 0, end: 100 }
    ]
  };
};

// 创建柱状图配置
export const createBarChartOption = (
  title: string,
  xAxisData: string[],
  series: Array<{ name: string; data: number[] }>,
  yAxisName: string = ''
): EChartsOption => {
  return {
    title: { text: title, left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' }
    },
    legend: {
      data: series.map(s => s.name),
      top: 30
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: xAxisData
    },
    yAxis: {
      type: 'value',
      name: yAxisName
    },
    series: series.map((s, index) => ({
      name: s.name,
      type: 'bar',
      data: s.data,
      itemStyle: { color: CHART_COLORS[index % CHART_COLORS.length] }
    }))
  };
};

// 创建仪表盘配置
export const createGaugeChartOption = (
  title: string,
  value: number,
  max: number = 100
): EChartsOption => {
  return {
    title: { text: title, left: 'center' },
    series: [
      {
        type: 'gauge',
        startAngle: 180,
        endAngle: 0,
        min: 0,
        max: max,
        splitNumber: 8,
        axisLine: {
          lineStyle: {
            width: 6,
            color: [
              [0.3, '#52c41a'],
              [0.7, '#faad14'],
              [1, '#f5222d']
            ]
          }
        },
        pointer: {
          itemStyle: {
            color: 'auto'
          }
        },
        axisTick: {
          distance: -30,
          length: 8,
          lineStyle: {
            color: '#fff',
            width: 2
          }
        },
        splitLine: {
          distance: -30,
          length: 30,
          lineStyle: {
            color: '#fff',
            width: 4
          }
        },
        axisLabel: {
          color: 'auto',
          distance: 40,
          fontSize: 12
        },
        detail: {
          valueAnimation: true,
          formatter: '{value}',
          color: 'auto',
          fontSize: 20
        },
        data: [{ value: value }]
      }
    ]
  };
};

// 创建时序图表配置（别名，用于DeviceQuery页面）
export const createTimeSeriesChartOption = (
  xAxisData: string[],
  series: Array<{ name: string; data: number[]; color?: string }>
): EChartsOption => {
  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' }
    },
    legend: {
      data: series.map(s => s.name),
      top: 10
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: xAxisData
    },
    yAxis: {
      type: 'value',
      name: '能耗 (kWh)'
    },
    series: series.map(s => ({
      name: s.name,
      type: 'line',
      data: s.data,
      smooth: true,
      itemStyle: s.color ? { color: s.color } : undefined
    }))
  };
};
