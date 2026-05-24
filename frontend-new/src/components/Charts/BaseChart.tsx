// 基础ECharts组件封装
import { useEffect, useRef } from 'react';
import * as echarts from 'echarts';
import type { EChartsOption } from 'echarts';

interface BaseChartProps {
  option: EChartsOption;
  height?: string | number;
  loading?: boolean;
}

export default function BaseChart({ option, height = 400, loading = false }: BaseChartProps) {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (chartRef.current) {
      chartInstance.current = echarts.init(chartRef.current);
    }

    // 窗口大小变化时自动调整
    const handleResize = () => {
      chartInstance.current?.resize();
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chartInstance.current?.dispose();
    };
  }, []);

  useEffect(() => {
    if (chartInstance.current) {
      if (loading) {
        chartInstance.current.showLoading();
      } else {
        chartInstance.current.hideLoading();
        chartInstance.current.setOption(option, true);
      }
    }
  }, [option, loading]);

  return <div ref={chartRef} style={{ height, width: '100%' }} />;
}
