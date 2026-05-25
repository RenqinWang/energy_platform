import ReactECharts from 'echarts-for-react';
import type { EChartsOption } from 'echarts';

export interface ConfidencePoint {
  time: string;
  history?: number | null;
  forecast?: number | null;
  lower?: number | null;
  upper?: number | null;
}

export default function ConfidenceBandChart({ data, dark = false, height = 360 }: { data: ConfidencePoint[]; dark?: boolean; height?: number }) {
  const option: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: {
      top: 8,
      textStyle: { color: dark ? '#cbd5e1' : '#475569' },
      data: ['历史供能量', '预测供能量', '置信区间'],
    },
    grid: { left: 48, right: 28, top: 64, bottom: 52, containLabel: true },
    xAxis: {
      type: 'category',
      data: data.map((item) => item.time),
      axisLabel: { color: dark ? '#94a3b8' : '#64748b', rotate: 30 },
      axisLine: { lineStyle: { color: dark ? '#334155' : '#cbd5e1' } },
    },
    yAxis: {
      type: 'value',
      name: 'kWh',
      axisLabel: { color: dark ? '#94a3b8' : '#64748b' },
      splitLine: { lineStyle: { color: dark ? 'rgba(148,163,184,.16)' : '#e5e7eb' } },
    },
    dataZoom: [{ type: 'inside' }, { bottom: 8, height: 18 }],
    series: [
      { name: '历史供能量', type: 'line', smooth: true, data: data.map((item) => item.history ?? null), color: '#38bdf8' },
      { name: '预测供能量', type: 'line', smooth: true, data: data.map((item) => item.forecast ?? null), color: '#f59e0b' },
      {
        name: '置信下界',
        type: 'line',
        stack: 'confidence',
        symbol: 'none',
        lineStyle: { opacity: 0 },
        data: data.map((item) => item.lower ?? null),
      },
      {
        name: '置信区间',
        type: 'line',
        stack: 'confidence',
        symbol: 'none',
        lineStyle: { opacity: 0 },
        areaStyle: { color: 'rgba(245, 158, 11, 0.18)' },
        data: data.map((item) => {
          if (item.lower === null || item.lower === undefined || item.upper === null || item.upper === undefined) return null;
          return Math.max(0, item.upper - item.lower);
        }),
      },
    ],
  };

  return <ReactECharts option={option} style={{ height }} notMerge />;
}
