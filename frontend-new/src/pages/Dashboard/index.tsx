import { useEffect, useMemo, useState } from 'react';
import { Alert, Button, DatePicker, Segmented, Select, Tag } from 'antd';
import {
  CalendarOutlined,
  FullscreenOutlined,
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import ReactECharts from 'echarts-for-react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { getStations } from '../../api/equipment';
import { getDashboardDates, getDashboardSummary } from '../../api/report';
import ChartContainer from '../../components/Common/ChartContainer';
import DateTimeDisplay from '../../components/Common/DateTimeDisplay';
import { useAppStore } from '../../store/useAppStore';
import { formatDateTime, formatNumber } from '../../utils/format';
import type { EChartsOption } from 'echarts';
import type { SystemType } from '../../api/report';

const n = (value?: number | null) => (typeof value === 'number' && Number.isFinite(value) ? value : 0);
type DashboardRange = { stat_date?: string; start_date?: string; end_date?: string };

const equipmentLabel = (systemType: SystemType, equipmentId: string) => {
  if (systemType === 'chiller') return equipmentId.replace('chiller_', '冷机');
  if (systemType === 'heating') return equipmentId.replace('heating_', '热机');
  if (equipmentId === 'cchp_system') return '三联供';
  return equipmentId;
};

const statusColor = (status: string) => {
  if (status === 'running') return '#22c55e';
  if (status === 'warning') return '#f59e0b';
  if (status === 'nodata') return '#cbd5e1';
  return '#b6beca';
};

const gaugeOption = (value: number, unit = 'kWh'): EChartsOption => {
  const max = Math.max(100, Math.ceil((value || 1) * 1.25 / 100) * 100);
  return {
    backgroundColor: 'transparent',
    series: [
      {
        type: 'gauge',
        startAngle: 210,
        endAngle: -30,
        min: 0,
        max,
        radius: '104%',
        center: ['50%', '64%'],
        splitNumber: 4,
        axisLine: {
          lineStyle: {
            width: 13,
            color: [
              [0.68, '#22c55e'],
              [0.86, '#f59e0b'],
              [1, '#ef4444'],
            ],
          },
        },
        progress: { show: true, width: 13, itemStyle: { color: '#38bdf8' } },
        pointer: { width: 4, length: '58%', itemStyle: { color: '#0f172a' } },
        anchor: { show: true, size: 9, itemStyle: { color: '#ffffff', borderColor: '#0f172a', borderWidth: 2 } },
        axisTick: { show: false },
        splitLine: { distance: -16, length: 8, lineStyle: { color: '#94a3b8', width: 1 } },
        axisLabel: { show: false },
        detail: {
          valueAnimation: true,
          formatter: `{value|${formatNumber(value, 1)}} {unit|${unit}}`,
          rich: {
            value: { color: '#0f172a', fontSize: 18, fontWeight: 700, lineHeight: 24 },
            unit: { color: '#64748b', fontSize: 11, lineHeight: 16 },
          },
          offsetCenter: [0, '45%'],
        },
        title: { show: false },
        data: [{ value: Number(value.toFixed(2)) }],
      },
    ],
  };
};

export default function DashboardPage() {
  const navigate = useNavigate();
  const stationId = useAppStore((state) => state.stationId);
  const setStationId = useAppStore((state) => state.setStationId);
  const setSystemType = useAppStore((state) => state.setSystemType);
  const [dashboardDate, setDashboardDate] = useState<Dayjs | null>(dayjs('2018-07-31'));
  const [rangeMode, setRangeMode] = useState<'day' | '7d' | '30d'>('day');
  const selectedDate = dashboardDate?.format('YYYY-MM-DD');

  const stationsQuery = useQuery({
    queryKey: ['stations'],
    queryFn: getStations,
    staleTime: 5 * 60_000,
  });

  const datesQuery = useQuery({
    queryKey: ['dashboard-dates', stationId],
    queryFn: () => getDashboardDates({ station_id: stationId }),
    staleTime: 10 * 60_000,
  });

  const availableDates = datesQuery.data || [];
  const availableDateSet = useMemo(() => new Set(availableDates), [availableDates]);

  useEffect(() => {
    if (!availableDates.length) return;
    const current = dashboardDate?.format('YYYY-MM-DD');
    if (current && availableDateSet.has(current)) return;
    const preferred = availableDateSet.has('2018-07-31') ? '2018-07-31' : availableDates[availableDates.length - 1];
    setDashboardDate(dayjs(preferred));
  }, [availableDates, availableDateSet, dashboardDate]);

  const dashboardRange = useMemo<DashboardRange>(() => {
    if (!selectedDate) return {};
    if (rangeMode === 'day') return { stat_date: selectedDate };
    const size = rangeMode === '7d' ? 7 : 30;
    const dates = availableDates.filter((date) => date <= selectedDate).slice(-size);
    if (!dates.length) return { stat_date: selectedDate };
    return { start_date: dates[0], end_date: dates[dates.length - 1] };
  }, [availableDates, rangeMode, selectedDate]);

  const summaryQuery = useQuery({
    queryKey: ['dashboard-summary', stationId, rangeMode, dashboardRange],
    queryFn: () => getDashboardSummary({ station_id: stationId, ...dashboardRange }),
    enabled: Boolean(selectedDate),
    refetchInterval: 60_000,
  });

  const summary = summaryQuery.data;

  const lineChartOption: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 48, right: 20, top: 24, bottom: 42, containLabel: true },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: (summary?.trend || []).map((item) => formatDateTime(item.time, rangeMode === 'day' ? 'HH:mm' : 'MM-DD')),
      axisLabel: { color: '#64748b' },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    yAxis: {
      type: 'value',
      name: 'kWh',
      nameTextStyle: { color: '#64748b' },
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: [
      {
        name: '系统供能',
        type: 'line',
        smooth: false,
        symbol: 'circle',
        symbolSize: 5,
        color: '#b65f5f',
        lineStyle: { width: 1.8 },
        data: (summary?.trend || []).map((item) => Number(n(item.history).toFixed(2))),
        markLine: { silent: true, lineStyle: { color: '#94a3b8', type: 'dashed' }, data: [{ type: 'average', name: '均值' }] },
      },
    ],
  };

  const equipment = summary?.equipment || [];
  const matrixItems = equipment
    .filter((item) => ['chiller', 'heating', 'cchp'].includes(item.system_type))
    .map((item, index) => ({
      item,
      x: index,
      y: ['cchp', 'heating', 'chiller'].indexOf(item.system_type),
      label: equipmentLabel(item.system_type, item.equipment_id),
    }));
  const matrixChartOption: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (params) => {
        const item = Array.isArray(params) ? params[0] : params;
        const data = item.data as [number, number, number, string, string, string];
        return `${data[4]}<br/>状态：${data[3]}<br/>供能：${formatNumber(data[2], 1)} kWh`;
      },
    },
    grid: { left: 52, right: 18, top: 20, bottom: 48, containLabel: true },
    xAxis: {
      type: 'category',
      data: matrixItems.map((item) => item.label),
      axisLabel: { color: '#64748b', rotate: 35, fontSize: 10 },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
      splitArea: { show: true, areaStyle: { color: ['#f8fafc', '#eef2f7'] } },
    },
    yAxis: {
      type: 'category',
      data: ['三联供', '热机', '冷机'],
      axisLabel: { color: '#334155', fontWeight: 600 },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    series: [
      {
        type: 'scatter',
        data: matrixItems.map(({ item, x, y, label }) => [
          x,
          y,
          n(item.supply),
          item.status,
          label,
          item.equipment_id,
        ]),
        symbolSize: (value) => (value[3] === 'running' ? 20 : value[3] === 'warning' ? 16 : 11),
        itemStyle: { color: (params) => statusColor((params.data as unknown[])[3] as string) },
      },
    ],
  };

  const systemByType = new Map((summary?.systems || []).map((item) => [item.system_type, item]));
  const cchp = systemByType.get('cchp');
  const heating = systemByType.get('heating');
  const chiller = systemByType.get('chiller');
  const gaugeCards = [
    { title: '三联供产热', caption: '当前小时热量输出', value: n(cchp?.total_heating), accent: 'text-violet-600' },
    { title: '三联供产冷', caption: '当前小时冷量输出', value: n(cchp?.total_cooling), accent: 'text-cyan-600' },
    { title: '热机产热', caption: '热机系统当前输出', value: n(heating?.total_heating), accent: 'text-rose-600' },
    { title: '冷机产冷', caption: '冷机系统当前输出', value: n(chiller?.total_cooling), accent: 'text-blue-600' },
  ];

  const forecastChartOption: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 24, top: 38, bottom: 38, containLabel: true },
    xAxis: {
      type: 'category',
      data: (summary?.forecast || []).map((item) => formatDateTime(item.time, 'HH:mm')),
      axisLabel: { color: '#64748b' },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    yAxis: {
      type: 'value',
      name: 'kWh',
      nameTextStyle: { color: '#64748b' },
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: [
      {
        name: '预测供能',
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 5,
        color: '#2563eb',
        areaStyle: { color: 'rgba(37, 99, 235, 0.12)' },
        data: (summary?.forecast || []).map((item) => Number(n(item.forecast).toFixed(2))),
      },
    ],
  };

  const systemCompareOption: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { top: 0, textStyle: { color: '#475569' } },
    grid: { left: 48, right: 20, top: 42, bottom: 32, containLabel: true },
    xAxis: {
      type: 'category',
      data: (summary?.systems || []).map((item) => ({ chiller: '冷机', heating: '热机', cchp: '三联供' }[item.system_type] || item.system_type)),
      axisLabel: { color: '#64748b' },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: [
      {
        name: '供能量',
        type: 'bar',
        barWidth: 18,
        itemStyle: { color: '#38bdf8', borderRadius: [4, 4, 0, 0] },
        data: (summary?.systems || []).map((item) => Number(n(item.total_supply).toFixed(2))),
      },
      {
        name: '能耗',
        type: 'bar',
        barWidth: 18,
        itemStyle: { color: '#f97316', borderRadius: [4, 4, 0, 0] },
        data: (summary?.systems || []).map((item) => Number(n(item.total_energy).toFixed(2))),
      },
    ],
  };

  const rankedEquipment = [...equipment].sort((a, b) => n(b.supply) - n(a.supply)).slice(0, 8).reverse();
  const equipmentRankOption: EChartsOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 78, right: 22, top: 20, bottom: 24, containLabel: true },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    yAxis: {
      type: 'category',
      data: rankedEquipment.map((item) => equipmentLabel(item.system_type, item.equipment_id)),
      axisLabel: { color: '#475569' },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    series: [
      {
        name: '供能量',
        type: 'bar',
        data: rankedEquipment.map((item) => Number(n(item.supply).toFixed(2))),
        itemStyle: { color: '#22c55e', borderRadius: [0, 4, 4, 0] },
        label: { show: true, position: 'right', formatter: '{c}', color: '#475569', fontSize: 10 },
      },
    ],
  };

  return (
    <div className="dashboard-shell min-h-[calc(100vh-64px)] px-5 py-4 text-slate-900 lg:px-8">
      <header className="mb-4 flex flex-wrap items-center justify-between gap-4">
        <div>
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="m-0 text-2xl font-semibold tracking-normal text-slate-950 lg:text-3xl">智慧能源网监测大屏</h1>
            <Tag color="cyan">Gold 实时查询</Tag>
            <Tag color="green">Silver 价格维表</Tag>
          </div>
          <p className="mt-2 text-sm text-slate-500">冷机、热机、冷热电三联供系统运行态势、预测和收益联动展示</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <Segmented
            value={rangeMode}
            onChange={(value) => setRangeMode(value as 'day' | '7d' | '30d')}
            options={[
              { label: '单日', value: 'day' },
              { label: '最近7天', value: '7d' },
              { label: '最近30天', value: '30d' },
            ]}
          />
          <DatePicker
            allowClear={false}
            value={dashboardDate}
            onChange={setDashboardDate}
            format="YYYY-MM-DD"
            suffixIcon={<CalendarOutlined />}
            disabledDate={(current) => Boolean(current && availableDateSet.size && !availableDateSet.has(current.format('YYYY-MM-DD')))}
          />
          <Select
            allowClear
            placeholder="全部站点"
            value={stationId}
            onChange={setStationId}
            className="min-w-36"
            options={(stationsQuery.data || []).map((station) => ({ label: station, value: station }))}
          />
          <Button icon={<FullscreenOutlined />} onClick={() => document.documentElement.requestFullscreen?.()}>
            全屏
          </Button>
          <DateTimeDisplay dataTime={summary?.latest_data_time} />
        </div>
      </header>

      {summaryQuery.error ? (
        <Alert
          className="mb-4"
          type="error"
          showIcon
          message="大屏数据加载失败"
          description={(summaryQuery.error as Error).message}
        />
      ) : null}

      {!datesQuery.isLoading && !availableDates.length ? (
        <Alert className="mb-4" type="warning" showIcon message="当前站点没有可用大屏日期" />
      ) : null}

      <section className="grid grid-cols-1 gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <ChartContainer
          title={rangeMode === 'day' ? '系统小时供能趋势' : '系统日供能趋势'}
          subtitle={rangeMode === 'day' ? `${selectedDate || '-'} 00:00-23:00` : `${dashboardRange.start_date || '-'} 至 ${dashboardRange.end_date || '-'}`}
          loading={summaryQuery.isLoading}
          empty={!summary?.trend.length}
        >
          <ReactECharts option={lineChartOption} style={{ height: 230 }} />
        </ChartContainer>

        <ChartContainer title="能源站设备运行状态" subtitle="绿色为运行，灰色为停机或无负荷；点击下方仪表或设备可继续下钻" loading={summaryQuery.isLoading} empty={!equipment.length}>
          <ReactECharts option={matrixChartOption} style={{ height: 230 }} />
        </ChartContainer>
      </section>

      <section className="my-5 text-center">
        <h2 className="m-0 text-xl font-semibold text-slate-900">机组产能实时监控</h2>
        <div className="mt-1 text-sm font-medium text-slate-500">{summary?.latest_data_time || '-'}</div>
      </section>

      <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {gaugeCards.map((card) => (
          <button
            key={card.title}
            type="button"
            className="page-card rounded-lg bg-white p-4 text-left transition hover:-translate-y-0.5 hover:border-sky-300"
            onClick={() => {
              const target = card.title.includes('冷机') ? 'chiller' : card.title.includes('热机') ? 'heating' : 'cchp';
              setSystemType(target);
              navigate('/system');
            }}
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <div className={`text-base font-semibold ${card.accent}`}>{card.title}</div>
                <div className="mt-1 text-xs text-slate-500">{card.caption}</div>
              </div>
              <Tag color={card.value > 0 ? 'green' : 'default'}>{card.value > 0 ? '运行' : '低负荷'}</Tag>
            </div>
            <ReactECharts option={gaugeOption(card.value)} style={{ height: 178 }} />
          </button>
        ))}
      </section>

      <section className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[1fr_.85fr_.85fr]">
        <div className="page-card rounded-lg bg-white p-4">
          <div className="text-sm text-slate-500">{rangeMode === 'day' ? '当日总供能量' : '周期总供能量'}</div>
          <div className="mt-1 text-2xl font-semibold text-slate-950">{formatNumber(n(summary?.kpis.today_total_supply), 1)} kWh</div>
        </div>
        <div className="page-card rounded-lg bg-white p-4">
          <div className="text-sm text-slate-500">当前小时 / 能耗</div>
          <div className="mt-1 text-2xl font-semibold text-slate-950">
            {formatNumber(n(summary?.kpis.current_total_supply), 1)} / {formatNumber(n(summary?.kpis.current_total_energy), 1)} kWh
          </div>
        </div>
        <div className="page-card rounded-lg bg-white p-4">
          <div className="text-sm text-slate-500">
            24h 预测利润（{summary?.kpis.forecast_profit_scope === 'chiller_10' ? '冷机10' : summary?.kpis.forecast_profit_scope || '收益页默认口径'}）
          </div>
          <div className={`mt-1 text-2xl font-semibold ${n(summary?.kpis.forecast_profit_24h) >= 0 ? 'text-emerald-600' : 'text-rose-600'}`}>
            {formatNumber(n(summary?.kpis.forecast_profit_24h), 1)} 元
          </div>
          <div className="mt-1 text-xs text-slate-500">
            全系统合计：{formatNumber(n(summary?.kpis.forecast_profit_all_systems), 1)} 元
          </div>
        </div>
      </section>

      <section className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[1.05fr_.95fr]">
        <ChartContainer title="未来24小时供能预测" subtitle="按设备预测结果汇总到系统级曲线" loading={summaryQuery.isLoading} empty={!summary?.forecast.length}>
          <ReactECharts option={forecastChartOption} style={{ height: 285 }} />
        </ChartContainer>

        <ChartContainer title="系统供能与能耗对比" subtitle="当前小时三类系统的供能/输入能耗对比" loading={summaryQuery.isLoading} empty={!summary?.systems.length}>
          <ReactECharts option={systemCompareOption} style={{ height: 285 }} />
        </ChartContainer>
      </section>

      <section className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[1fr_1fr]">
        <ChartContainer title="设备供能排行" subtitle="按当前小时供能量排序，便于发现主力运行设备" loading={summaryQuery.isLoading} empty={!rankedEquipment.length}>
          <ReactECharts option={equipmentRankOption} style={{ height: 300 }} />
        </ChartContainer>

        <section className="page-card rounded-lg bg-white p-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <div>
              <h3 className="m-0 text-base font-semibold text-slate-900">运行分析与决策建议</h3>
              <p className="m-0 mt-1 text-sm text-slate-500">按设备状态、供能预测和收益口径生成的当前建议</p>
            </div>
            <Tag>{summary?.kpis.running_equipment || 0} 台运行</Tag>
          </div>
          <div className="space-y-3">
            {(summary?.advice || []).slice(0, 5).map((item, index) => (
              <div key={`${item.equipment_id || 'system'}-${index}`} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                <div className="mb-1 flex flex-wrap items-center gap-2">
                  <Tag color={item.risk_level === 'high' ? 'red' : item.risk_level === 'medium' ? 'orange' : 'blue'}>
                    {item.risk_level || 'low'}
                  </Tag>
                  <span className="text-sm font-semibold text-slate-800">{item.equipment_id || '系统'} · {item.advice_type || '运行建议'}</span>
                </div>
                <div className="text-sm leading-6 text-slate-600">{item.advice_text}</div>
              </div>
            ))}
            {summary?.advice?.length ? null : <div className="rounded-lg border border-slate-200 p-4 text-sm text-slate-500">暂无运行建议</div>}
          </div>
        </section>
      </section>
    </div>
  );
}
