import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Row, Select, Space, Spin, Statistic, Table, Tag, Typography } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import {
  getSystemDailyReport,
  getSystemEquipment,
  getSystemMonthlyReport,
  getSystemWeeklyReport,
} from '../../api/report';
import { formatNumber } from '../../utils/format';
import type { EChartsOption } from 'echarts';
import type {
  SystemDailyReportRecord,
  SystemMonthlyReportRecord,
  SystemType,
  SystemWeeklyReportRecord,
} from '../../api/report';

const { Option } = Select;
const { Text, Title } = Typography;

type ReportType = 'daily' | 'weekly' | 'monthly';
type EquipmentSelection = 'all' | string;
type SourceReportRecord = SystemDailyReportRecord | SystemWeeklyReportRecord | SystemMonthlyReportRecord;

interface ReportViewRecord {
  period: string;
  equipment_id: string;
  peak_cooling_kwh: number | null;
  valley_cooling_kwh: number | null;
  peak_valley_ratio: number | null;
  peak_duration_hours: number | null;
  valley_duration_hours: number | null;
  total_cooling_supply_kwh: number | null;
  total_energy_consumption_kwh: number | null;
  total_runtime_hours: number | null;
  total_run_minutes: number | null;
  avg_cooling_supply_kwh: number | null;
  operation_rate_pct: number | null;
  avg_cop: number | null;
  total_energy_cost: number | null;
  total_cooling_revenue: number | null;
  net_profit: number | null;
  data_completeness_rate_pct: number | null;
  hour_count: number | null;
}

const reportTypeLabel: Record<ReportType, string> = {
  daily: '日度报表',
  weekly: '周度报表',
  monthly: '月度报表',
};

const systemOptions: Array<{ value: SystemType; label: string; disabled?: boolean }> = [
  { value: 'chiller', label: '冷机系统' },
  { value: 'heating', label: '热机系统' },
  { value: 'cchp', label: '冷热电三联供' },
];

const n = (value: number | null | undefined) =>
  typeof value === 'number' && Number.isFinite(value) ? value : null;

const sumNumbers = (values: Array<number | null | undefined>) => {
  const valid = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
  return valid.length ? valid.reduce((total, value) => total + value, 0) : null;
};

const avgNumbers = (values: Array<number | null | undefined>) => {
  const valid = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
  return valid.length ? valid.reduce((total, value) => total + value, 0) / valid.length : null;
};

const minPositive = (values: Array<number | null | undefined>) => {
  const valid = values.filter((value): value is number => typeof value === 'number' && value > 0);
  return valid.length ? Math.min(...valid) : null;
};

const periodOf = (item: SourceReportRecord, reportType: ReportType) => {
  if (reportType === 'daily') return (item as SystemDailyReportRecord).stat_date;
  if (reportType === 'weekly') return (item as SystemWeeklyReportRecord).stat_week_str;
  return (item as SystemMonthlyReportRecord).stat_month_str;
};

const toViewRecord = (item: SourceReportRecord, reportType: ReportType): ReportViewRecord => {
  const weeklyOrMonthly = item as SystemWeeklyReportRecord | SystemMonthlyReportRecord;
  const daily = item as SystemDailyReportRecord;
  const totalSupply = n(item.total_supply_kwh ?? item.total_cooling_supply_kwh);
  const totalEnergy = n(item.total_energy_consumption_kwh);
  const peakSupply = n(item.peak_supply_kwh ?? item.peak_cooling_kwh);
  const avgSupply = n(item.avg_supply_kwh ?? item.avg_cooling_supply_kwh);
  const operationRatePct = reportType === 'daily'
    ? n(daily.daily_operation_rate)
    : n(weeklyOrMonthly.equipment_utilization_rate) !== null
      ? (weeklyOrMonthly.equipment_utilization_rate as number) * 100
      : null;

  return {
    period: periodOf(item, reportType),
    equipment_id: item.equipment_id,
    peak_cooling_kwh: peakSupply,
    valley_cooling_kwh: n(item.valley_supply_kwh ?? item.valley_cooling_kwh),
    peak_valley_ratio: n(item.peak_valley_ratio),
    peak_duration_hours: n(item.peak_duration_hours),
    valley_duration_hours: n(item.valley_duration_hours),
    total_cooling_supply_kwh: totalSupply,
    total_energy_consumption_kwh: totalEnergy,
    total_runtime_hours: n(item.total_runtime_hours),
    total_run_minutes: n(daily.total_run_minutes),
    avg_cooling_supply_kwh: avgSupply,
    operation_rate_pct: operationRatePct,
    avg_cop: n(item.avg_cop),
    total_energy_cost: n(item.total_energy_cost ?? daily.energy_cost),
    total_cooling_revenue: n(item.total_supply_revenue ?? item.total_cooling_revenue ?? daily.cooling_revenue),
    net_profit: n(item.net_profit),
    data_completeness_rate_pct: n(weeklyOrMonthly.data_completeness_rate) !== null
      ? (weeklyOrMonthly.data_completeness_rate as number) * 100
      : null,
    hour_count: n(item.hour_count),
  };
};

const aggregateByPeriod = (records: ReportViewRecord[]) => {
  const groups = new Map<string, ReportViewRecord[]>();
  records.forEach((record) => {
    const group = groups.get(record.period) || [];
    group.push(record);
    groups.set(record.period, group);
  });

  return Array.from(groups.entries()).map(([period, group]) => {
    const totalCooling = sumNumbers(group.map((item) => item.total_cooling_supply_kwh));
    const totalEnergy = sumNumbers(group.map((item) => item.total_energy_consumption_kwh));
    const peakCooling = group.some((item) => item.peak_cooling_kwh !== null)
      ? Math.max(...group.map((item) => item.peak_cooling_kwh || 0))
      : null;
    const valleyCooling = minPositive(group.map((item) => item.valley_cooling_kwh));
    const hourCount = sumNumbers(group.map((item) => item.hour_count));
    const avgCooling = totalCooling !== null && hourCount && hourCount > 0
      ? totalCooling / hourCount
      : avgNumbers(group.map((item) => item.avg_cooling_supply_kwh));

    return {
      period,
      equipment_id: '全部设备',
      peak_cooling_kwh: peakCooling,
      valley_cooling_kwh: valleyCooling,
      peak_valley_ratio: peakCooling !== null && valleyCooling && valleyCooling > 0 ? peakCooling / valleyCooling : null,
      peak_duration_hours: sumNumbers(group.map((item) => item.peak_duration_hours)),
      valley_duration_hours: sumNumbers(group.map((item) => item.valley_duration_hours)),
      total_cooling_supply_kwh: totalCooling,
      total_energy_consumption_kwh: totalEnergy,
      total_runtime_hours: sumNumbers(group.map((item) => item.total_runtime_hours)),
      total_run_minutes: sumNumbers(group.map((item) => item.total_run_minutes)),
      avg_cooling_supply_kwh: avgCooling,
      operation_rate_pct: avgNumbers(group.map((item) => item.operation_rate_pct)),
      avg_cop: totalCooling !== null && totalEnergy && totalEnergy > 0 ? totalCooling / totalEnergy : null,
      total_energy_cost: sumNumbers(group.map((item) => item.total_energy_cost)),
      total_cooling_revenue: sumNumbers(group.map((item) => item.total_cooling_revenue)),
      net_profit: sumNumbers(group.map((item) => item.net_profit)),
      data_completeness_rate_pct: avgNumbers(group.map((item) => item.data_completeness_rate_pct)),
      hour_count: hourCount,
    };
  });
};

const equipmentLabel = (systemType: SystemType, equipmentId: string) => {
  if (systemType === 'chiller') return equipmentId.replace('chiller_', '冷机 ');
  if (systemType === 'heating') return equipmentId.replace('heating_', '热机 ');
  if (equipmentId === 'cchp_system') return '三联供系统';
  return equipmentId;
};

const chartGrid = {
  left: 48,
  right: 24,
  top: 72,
  bottom: 60,
  containLabel: true,
};

export default function ComprehensiveReport() {
  const [reportType, setReportType] = useState<ReportType>('daily');
  const [systemType, setSystemType] = useState<SystemType>('chiller');
  const [selectedEquipment, setSelectedEquipment] = useState<EquipmentSelection>('all');
  const [equipmentOptions, setEquipmentOptions] = useState<string[]>([]);
  const [rawData, setRawData] = useState<SourceReportRecord[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getSystemEquipment({ system_type: systemType })
      .then((items) => {
        setEquipmentOptions(items.sort((a, b) => a.localeCompare(b, undefined, { numeric: true })));
        setSelectedEquipment('all');
      })
      .catch(() => setEquipmentOptions([]));
  }, [systemType]);

  const equipmentId = selectedEquipment === 'all' ? undefined : selectedEquipment;

  useEffect(() => {
    let cancelled = false;

    const loadReport = async () => {
      setLoading(true);
      try {
        const limit = reportType === 'daily'
          ? selectedEquipment === 'all' ? 800 : 180
          : reportType === 'weekly'
            ? selectedEquipment === 'all' ? 260 : 80
            : selectedEquipment === 'all' ? 120 : 36;

        const params = {
          system_type: systemType,
          equipment_id: equipmentId,
          limit,
        };

        const result = reportType === 'daily'
          ? await getSystemDailyReport(params)
          : reportType === 'weekly'
            ? await getSystemWeeklyReport(params)
            : await getSystemMonthlyReport(params);

        if (!cancelled) {
          setRawData(result);
        }
      } catch {
        if (!cancelled) {
          setRawData([]);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    loadReport();

    return () => {
      cancelled = true;
    };
  }, [equipmentId, reportType, selectedEquipment, systemType]);

  const data = useMemo(() => {
    const records = rawData.map((item) => toViewRecord(item, reportType));
    const nextData = selectedEquipment === 'all' ? aggregateByPeriod(records) : records;
    return nextData.sort((a, b) => String(b.period).localeCompare(String(a.period)));
  }, [rawData, reportType, selectedEquipment]);

  const latestData = data[0] || null;
  const chartData = [...data].reverse();
  const xAxisData = chartData.map((item) => item.period);
  const currentSystemLabel = systemOptions.find((item) => item.value === systemType)?.label || '';

  const peakValleyChartOption: EChartsOption = {
    title: { text: '峰值/谷值/平均供能', left: 'center' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['峰值', '平均值', '谷值'], top: 32 },
    grid: chartGrid,
    xAxis: { type: 'category', data: xAxisData, axisLabel: { rotate: 35 } },
    yAxis: { type: 'value', name: 'kWh' },
    series: [
      { name: '峰值', type: 'line', data: chartData.map((item) => item.peak_cooling_kwh), smooth: true, itemStyle: { color: '#d4380d' } },
      { name: '平均值', type: 'line', data: chartData.map((item) => item.avg_cooling_supply_kwh), smooth: true, itemStyle: { color: '#1677ff' } },
      { name: '谷值', type: 'line', data: chartData.map((item) => item.valley_cooling_kwh), smooth: true, itemStyle: { color: '#389e0d' } },
    ],
  };

  const totalEnergyChartOption: EChartsOption = {
    title: { text: '总供能量与总能耗', left: 'center' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: ['总供能量', '总能耗'], top: 32 },
    grid: chartGrid,
    xAxis: { type: 'category', data: xAxisData, axisLabel: { rotate: 35 } },
    yAxis: { type: 'value', name: 'kWh' },
    series: [
      { name: '总供能量', type: 'bar', data: chartData.map((item) => item.total_cooling_supply_kwh), itemStyle: { color: '#1677ff' } },
      { name: '总能耗', type: 'bar', data: chartData.map((item) => item.total_energy_consumption_kwh), itemStyle: { color: '#fa8c16' } },
    ],
  };

  const durationChartOption: EChartsOption = {
    title: { text: '峰值期与谷值期时长', left: 'center' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: ['峰值期时长', '谷值期时长'], top: 32 },
    grid: chartGrid,
    xAxis: { type: 'category', data: xAxisData, axisLabel: { rotate: 35 } },
    yAxis: { type: 'value', name: '小时' },
    series: [
      { name: '峰值期时长', type: 'bar', data: chartData.map((item) => item.peak_duration_hours), itemStyle: { color: '#d4380d' } },
      { name: '谷值期时长', type: 'bar', data: chartData.map((item) => item.valley_duration_hours), itemStyle: { color: '#389e0d' } },
    ],
  };

  const efficiencyChartOption: EChartsOption = {
    title: { text: '利用率、负荷与收益', left: 'center' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['运行/利用率', '平均COP', '净利润'], top: 32 },
    grid: chartGrid,
    xAxis: { type: 'category', data: xAxisData, axisLabel: { rotate: 35 } },
    yAxis: [
      { type: 'value', name: '% / COP' },
      { type: 'value', name: '元' },
    ],
    series: [
      { name: '运行/利用率', type: 'line', data: chartData.map((item) => item.operation_rate_pct), smooth: true, itemStyle: { color: '#722ed1' } },
      { name: '平均COP', type: 'line', data: chartData.map((item) => item.avg_cop), smooth: true, itemStyle: { color: '#13a8a8' } },
      { name: '净利润', type: 'bar', yAxisIndex: 1, data: chartData.map((item) => item.net_profit), itemStyle: { color: '#52c41a' } },
    ],
  };

  const columns = [
    { title: '时间周期', dataIndex: 'period', key: 'period', fixed: 'left' as const, width: 130 },
    { title: '对象', dataIndex: 'equipment_id', key: 'equipment_id', width: 120 },
    { title: '峰值 (kWh)', dataIndex: 'peak_cooling_kwh', key: 'peak', width: 130, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '谷值 (kWh)', dataIndex: 'valley_cooling_kwh', key: 'valley', width: 130, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '峰谷比', dataIndex: 'peak_valley_ratio', key: 'ratio', width: 110, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '峰值期时长 (h)', dataIndex: 'peak_duration_hours', key: 'peak_duration', width: 150, render: (value: number | null) => formatNumber(value || 0, 1) },
    { title: '谷值期时长 (h)', dataIndex: 'valley_duration_hours', key: 'valley_duration', width: 150, render: (value: number | null) => formatNumber(value || 0, 1) },
    { title: '总供能量 (kWh)', dataIndex: 'total_cooling_supply_kwh', key: 'total_supply', width: 160, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '总能耗 (kWh)', dataIndex: 'total_energy_consumption_kwh', key: 'total_energy', width: 150, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '平均COP', dataIndex: 'avg_cop', key: 'cop', width: 110, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '运行/利用率', dataIndex: 'operation_rate_pct', key: 'utilization', width: 130, render: (value: number | null) => `${formatNumber(value || 0, 2)}%` },
    { title: '净利润 (元)', dataIndex: 'net_profit', key: 'profit', width: 140, render: (value: number | null) => formatNumber(value || 0, 2) },
    { title: '数据完整率', dataIndex: 'data_completeness_rate_pct', key: 'completeness', width: 130, render: (value: number | null) => value === null ? '-' : `${formatNumber(value, 2)}%` },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Space direction="vertical" size={20} style={{ width: '100%' }}>
        <Title level={2} style={{ margin: 0 }}>综合报表</Title>

        <Row gutter={[16, 16]}>
          <Col xs={24} md={8}>
            <Card styles={{ body: { minHeight: 112 } }}>
              <Text strong>报表类型</Text>
              <Select value={reportType} onChange={setReportType} style={{ width: '100%', marginTop: 10 }}>
                <Option value="daily">日度报表</Option>
                <Option value="weekly">周度报表</Option>
                <Option value="monthly">月度报表</Option>
              </Select>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card styles={{ body: { minHeight: 112 } }}>
              <Text strong>系统范围</Text>
              <Select value={systemType} onChange={setSystemType} style={{ width: '100%', marginTop: 10 }}>
                {systemOptions.map((item) => (
                  <Option key={item.value} value={item.value}>
                    {item.label}
                  </Option>
                ))}
              </Select>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card styles={{ body: { minHeight: 112 } }}>
              <Text strong>设备选择</Text>
              <Select value={selectedEquipment} onChange={setSelectedEquipment} style={{ width: '100%', marginTop: 10 }}>
                <Option value="all">全部{currentSystemLabel}</Option>
                {equipmentOptions.map((equipmentId) => (
                  <Option key={equipmentId} value={equipmentId}>{equipmentLabel(systemType, equipmentId)}</Option>
                ))}
              </Select>
            </Card>
          </Col>
        </Row>

        <Alert
          type="info"
          showIcon
          message={`当前展示 ${reportTypeLabel[reportType]} / ${currentSystemLabel}`}
          description="当前综合报表读取统一 Gold 表，已覆盖冷机、热机和冷热电三联供，支持峰值、谷值、总供能量、峰谷时长、能效、利用率、收益和异常统计。"
        />

        {loading ? (
          <Card>
            <div style={{ textAlign: 'center', padding: 48 }}>
              <Spin size="large" />
            </div>
          </Card>
        ) : data.length > 0 ? (
          <>
            <Row gutter={[16, 16]}>
              <Col xs={24} sm={12} lg={6}>
                <Card>
                  <Statistic title="总供能量" value={latestData?.total_cooling_supply_kwh || 0} precision={2} suffix="kWh" valueStyle={{ color: '#1677ff' }} />
                  <Tag color="blue" style={{ marginTop: 10 }}>{latestData?.period}</Tag>
                </Card>
              </Col>
              <Col xs={24} sm={12} lg={6}>
                <Card>
                  <Statistic title="峰值/谷值" value={latestData?.peak_cooling_kwh || 0} precision={2} suffix="kWh" valueStyle={{ color: '#d4380d' }} />
                  <Text type="secondary">谷值 {formatNumber(latestData?.valley_cooling_kwh || 0, 2)} kWh</Text>
                </Card>
              </Col>
              <Col xs={24} sm={12} lg={6}>
                <Card>
                  <Statistic title="峰值期时长" value={latestData?.peak_duration_hours || 0} precision={1} suffix="h" valueStyle={{ color: '#d4380d' }} />
                  <Text type="secondary">谷值期 {formatNumber(latestData?.valley_duration_hours || 0, 1)} h</Text>
                </Card>
              </Col>
              <Col xs={24} sm={12} lg={6}>
                <Card>
                  <Statistic
                    title="平均COP"
                    value={latestData?.avg_cop || 0}
                    precision={2}
                    prefix={(latestData?.avg_cop || 0) >= 3 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                    valueStyle={{ color: (latestData?.avg_cop || 0) >= 3 ? '#389e0d' : '#d4380d' }}
                  />
                  <Text type="secondary">净利润 {formatNumber(latestData?.net_profit || 0, 2)} 元</Text>
                </Card>
              </Col>
            </Row>

            <Row gutter={[16, 16]}>
              <Col xs={24} xl={12}>
                <Card>
                  <ReactECharts option={peakValleyChartOption} style={{ height: 380 }} />
                </Card>
              </Col>
              <Col xs={24} xl={12}>
                <Card>
                  <ReactECharts option={totalEnergyChartOption} style={{ height: 380 }} />
                </Card>
              </Col>
              <Col xs={24} xl={12}>
                <Card>
                  <ReactECharts option={durationChartOption} style={{ height: 380 }} />
                </Card>
              </Col>
              <Col xs={24} xl={12}>
                <Card>
                  <ReactECharts option={efficiencyChartOption} style={{ height: 380 }} />
                </Card>
              </Col>
            </Row>

            <Card title="详细数据表格">
              <Table
                columns={columns}
                dataSource={data}
                rowKey={(record) => `${record.period}-${record.equipment_id}`}
                scroll={{ x: 1900 }}
                pagination={{ pageSize: 10, showSizeChanger: true }}
              />
            </Card>
          </>
        ) : (
          <Alert message="暂无数据" description="当前选择的系统、设备和时间范围内没有报表数据" type="info" showIcon />
        )}
      </Space>
    </div>
  );
}
