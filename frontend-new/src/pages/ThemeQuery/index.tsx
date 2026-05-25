// 主题级数据查询页面
import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, DatePicker, Empty, Radio, Row, Select, Space, Statistic, Table, Tag } from 'antd';
import {
  CompressOutlined,
  DashboardOutlined,
  FireOutlined,
  FundOutlined,
  ThunderboltOutlined,
  VerticalAlignMiddleOutlined,
} from '@ant-design/icons';
import BaseChart from '../../components/Charts/BaseChart';
import Loading from '../../components/Common/Loading';
import { getSystemEquipment, getSystemSupplyCurve } from '../../api/report';
import {
  formatDate,
  formatDateTime,
  formatEnergy,
  formatFlow,
  formatNumber,
  formatPressure,
  formatTemperature,
} from '../../utils/format';
import type { SystemSupplyCurveRecord, SystemType } from '../../api/report';
import type { EChartsOption } from 'echarts';
import type { ColumnsType } from 'antd/es/table';
import type { RangePickerProps } from 'antd/es/date-picker';

const { RangePicker } = DatePicker;

type SystemFilter = SystemType | 'all';
type RangeMode = 'latest_96' | 'latest_168' | 'latest_1000' | 'custom';
type ThemeType = 'supply' | 'energy' | 'temperature' | 'flow' | 'pressure' | 'operation';

interface EquipmentOption {
  key: string;
  system_type?: SystemType;
  equipment_id: string;
}

interface SummaryRow {
  key: string;
  system_type?: SystemType;
  equipment_id: string;
  records: number;
  total: number;
  avg: number;
  max: number;
  min: number;
  positiveRecords: number;
}

const systemOptions: Array<{ value: SystemFilter; label: string }> = [
  { value: 'all', label: '全部系统' },
  { value: 'chiller', label: '冷机系统' },
  { value: 'heating', label: '热机系统' },
  { value: 'cchp', label: '冷热电三联供' },
];

const systemLabel: Record<SystemType, string> = {
  chiller: '冷机系统',
  heating: '热机系统',
  cchp: '冷热电三联供',
};

const systemTagColor: Record<SystemType, string> = {
  chiller: 'blue',
  heating: 'volcano',
  cchp: 'purple',
};

const rangeOptions: Array<{ value: RangeMode; label: string; limit: number }> = [
  { value: 'latest_96', label: '最新96小时', limit: 96 },
  { value: 'latest_168', label: '最新168小时', limit: 168 },
  { value: 'latest_1000', label: '最新1000小时', limit: 1000 },
  { value: 'custom', label: '自定义日期', limit: 1000 },
];

const THEMES: Array<{
  label: string;
  value: ThemeType;
  icon: React.ReactNode;
  unit: string;
  color: string;
  aggregatable: boolean;
}> = [
  { label: '供能主题', value: 'supply', icon: <FundOutlined />, unit: 'kWh', color: '#1677ff', aggregatable: true },
  { label: '能耗主题', value: 'energy', icon: <ThunderboltOutlined />, unit: 'kWh', color: '#52c41a', aggregatable: true },
  { label: '温度主题', value: 'temperature', icon: <FireOutlined />, unit: '℃', color: '#fa8c16', aggregatable: false },
  { label: '流量主题', value: 'flow', icon: <DashboardOutlined />, unit: 'm³/h', color: '#13c2c2', aggregatable: false },
  { label: '压力主题', value: 'pressure', icon: <VerticalAlignMiddleOutlined />, unit: 'MPa', color: '#722ed1', aggregatable: false },
  { label: '运行主题', value: 'operation', icon: <CompressOutlined />, unit: '%', color: '#eb2f96', aggregatable: false },
];

const seriesColors = ['#1677ff', '#52c41a', '#fa8c16', '#f5222d', '#722ed1', '#13c2c2', '#eb2f96', '#faad14'];

const n = (value?: number | null) => (typeof value === 'number' && Number.isFinite(value) ? value : 0);

const inferSystemType = (equipmentId: string): SystemType | undefined => {
  if (equipmentId.startsWith('chiller_')) return 'chiller';
  if (equipmentId.startsWith('heating_')) return 'heating';
  if (equipmentId === 'cchp_system') return 'cchp';
  return undefined;
};

const equipmentKey = (systemType: SystemType | undefined, equipmentId: string) => `${systemType || inferSystemType(equipmentId) || 'unknown'}:${equipmentId}`;

const equipmentLabel = (option: EquipmentOption) => {
  const systemType = option.system_type || inferSystemType(option.equipment_id);
  const prefix = systemType ? `${systemLabel[systemType]} / ` : '';
  if (systemType === 'chiller') return `${prefix}${option.equipment_id.replace('chiller_', '冷机 ')}`;
  if (systemType === 'heating') return `${prefix}${option.equipment_id.replace('heating_', '热机 ')}`;
  if (option.equipment_id === 'cchp_system') return `${prefix}三联供系统`;
  return `${prefix}${option.equipment_id}`;
};

const primarySupply = (item: SystemSupplyCurveRecord) =>
  n(item.supply_kwh) || n(item.cooling_supply_kwh) + n(item.heating_supply_kwh) + n(item.electric_supply_kwh);

const getThemeValue = (record: SystemSupplyCurveRecord, selectedTheme: ThemeType): number => {
  switch (selectedTheme) {
    case 'supply':
      return primarySupply(record);
    case 'energy':
      return n(record.energy_consumption_kwh);
    case 'temperature':
      return n(record.avg_supply_temp);
    case 'flow':
      return n(record.avg_flow);
    case 'pressure':
      return n(record.avg_pressure);
    case 'operation':
      return n(record.operation_rate);
    default:
      return 0;
  }
};

const formatThemeValue = (selectedTheme: ThemeType, value: number): string => {
  switch (selectedTheme) {
    case 'supply':
    case 'energy':
      return formatEnergy(value);
    case 'temperature':
      return formatTemperature(value);
    case 'flow':
      return formatFlow(value);
    case 'pressure':
      return formatPressure(value);
    case 'operation':
      return `${value.toFixed(1)}%`;
    default:
      return formatNumber(value);
  }
};

const averageMetricTitle = (selectedTheme: ThemeType) => {
  switch (selectedTheme) {
    case 'supply':
      return '平均供能量';
    case 'energy':
      return '平均能耗';
    case 'temperature':
      return '平均温度';
    case 'flow':
      return '平均流量';
    case 'pressure':
      return '平均压力';
    case 'operation':
      return '平均运行率';
    default:
      return '平均值';
  }
};

export default function ThemeQueryPage() {
  const [systemFilter, setSystemFilter] = useState<SystemFilter>('all');
  const [equipmentOptions, setEquipmentOptions] = useState<EquipmentOption[]>([]);
  const [selectedEquipmentKeys, setSelectedEquipmentKeys] = useState<string[]>([]);
  const [selectedTheme, setSelectedTheme] = useState<ThemeType>('supply');
  const [rangeMode, setRangeMode] = useState<RangeMode>('latest_96');
  const [timeRange, setTimeRange] = useState({ start: '', end: '' });
  const [loading, setLoading] = useState(true);
  const [dataMap, setDataMap] = useState<Record<string, SystemSupplyCurveRecord[]>>({});

  useEffect(() => {
    let cancelled = false;

    const loadEquipment = async () => {
      setLoading(true);
      setSelectedEquipmentKeys([]);
      setDataMap({});

      try {
        const systemsToLoad: SystemType[] = systemFilter === 'all' ? ['chiller', 'heating', 'cchp'] : [systemFilter];
        const results = await Promise.all(
          systemsToLoad.map(async (systemType) => ({
            systemType,
            equipment: await getSystemEquipment({ system_type: systemType }),
          })),
        );
        if (cancelled) return;

        const options = results.flatMap(({ systemType, equipment }) =>
          equipment.map((equipmentId) => ({
            key: equipmentKey(systemType, equipmentId),
            system_type: systemType,
            equipment_id: equipmentId,
          })),
        ).sort((a, b) => a.key.localeCompare(b.key, undefined, { numeric: true }));

        setEquipmentOptions(options);
        const preferred = options.find((item) => item.equipment_id === 'chiller_10') || options[0];
        const nextSelected = [preferred, ...options.filter((item) => item.key !== preferred?.key)].slice(0, 4).map((item) => item.key);
        setSelectedEquipmentKeys(nextSelected);
      } catch (error) {
        console.error('Failed to load system equipment:', error);
        if (!cancelled) {
          setEquipmentOptions([]);
          setSelectedEquipmentKeys([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadEquipment();

    return () => {
      cancelled = true;
    };
  }, [systemFilter]);

  useEffect(() => {
    if (selectedEquipmentKeys.length === 0) return;

    let cancelled = false;
    const selectedRange = rangeOptions.find((item) => item.value === rangeMode) || rangeOptions[0];
    const optionByKey = new Map(equipmentOptions.map((item) => [item.key, item]));

    const loadData = async () => {
      setLoading(true);
      try {
        const results = await Promise.all(
          selectedEquipmentKeys.map(async (key) => {
            const option = optionByKey.get(key);
            if (!option) return { key, rows: [] as SystemSupplyCurveRecord[] };

            const rows = await getSystemSupplyCurve({
              system_type: option.system_type,
              equipment_id: option.equipment_id,
              start_date: rangeMode === 'custom' && timeRange.start ? formatDate(timeRange.start) : undefined,
              end_date: rangeMode === 'custom' && timeRange.end ? formatDate(timeRange.end) : undefined,
              limit: selectedRange.limit,
            });
            return { key, rows };
          }),
        );

        if (!cancelled) {
          setDataMap(Object.fromEntries(results.map(({ key, rows }) => [key, rows])));
        }
      } catch (error) {
        console.error('Failed to load theme data:', error);
        if (!cancelled) setDataMap({});
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadData();

    return () => {
      cancelled = true;
    };
  }, [selectedEquipmentKeys, equipmentOptions, rangeMode, timeRange]);

  const selectedThemeConfig = THEMES.find((item) => item.value === selectedTheme) || THEMES[0];
  const selectedOptions = selectedEquipmentKeys
    .map((key) => equipmentOptions.find((item) => item.key === key))
    .filter((item): item is EquipmentOption => Boolean(item));

  const xAxisHours = useMemo(() => {
    const hours = new Set<string>();
    selectedEquipmentKeys.forEach((key) => {
      (dataMap[key] || []).forEach((record) => hours.add(record.stat_hour));
    });
    return Array.from(hours).sort((a, b) => new Date(a).getTime() - new Date(b).getTime());
  }, [dataMap, selectedEquipmentKeys]);

  const chartOption: EChartsOption = {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
    },
    legend: {
      data: selectedOptions.map(equipmentLabel),
      top: 8,
      type: 'scroll',
    },
    grid: {
      left: 48,
      right: 28,
      top: 64,
      bottom: 54,
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: xAxisHours.map((hour) => formatDateTime(hour, 'MM-DD HH:mm')),
      axisLabel: { rotate: 30 },
    },
    yAxis: {
      type: 'value',
      name: `${selectedThemeConfig.label} (${selectedThemeConfig.unit})`,
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { start: 0, end: 100 },
    ],
    series: selectedOptions.map((option, idx) => {
      const rowsByHour = new Map((dataMap[option.key] || []).map((record) => [record.stat_hour, record]));
      return {
        name: equipmentLabel(option),
        type: 'line',
        data: xAxisHours.map((hour) => {
          const row = rowsByHour.get(hour);
          return row ? getThemeValue(row, selectedTheme) : null;
        }),
        smooth: true,
        connectNulls: false,
        itemStyle: { color: seriesColors[idx % seriesColors.length] },
      };
    }),
  };

  const calculateStats = (rows: SystemSupplyCurveRecord[]) => {
    const values = rows.map((record) => getThemeValue(record, selectedTheme));
    return {
      total: values.reduce((sum, value) => sum + value, 0),
      avg: values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0,
      max: values.length ? Math.max(...values) : 0,
      min: values.length ? Math.min(...values) : 0,
      positiveRecords: values.filter((value) => value > 0).length,
    };
  };

  const summaryData: SummaryRow[] = selectedOptions.map((option) => {
    const rows = dataMap[option.key] || [];
    return {
      key: option.key,
      system_type: option.system_type,
      equipment_id: option.equipment_id,
      records: rows.length,
      ...calculateStats(rows),
    };
  });

  const totalRecords = summaryData.reduce((sum, item) => sum + item.records, 0);
  const totalPositiveRecords = summaryData.reduce((sum, item) => sum + item.positiveRecords, 0);
  const overallValues = selectedEquipmentKeys.flatMap((key) => (dataMap[key] || []).map((record) => getThemeValue(record, selectedTheme)));
  const overallAvg = overallValues.length ? overallValues.reduce((sum, value) => sum + value, 0) / overallValues.length : 0;
  const overallMax = overallValues.length ? Math.max(...overallValues) : 0;
  const overallMin = overallValues.length ? Math.min(...overallValues) : 0;

  const summaryColumns: ColumnsType<SummaryRow> = [
    {
      title: '系统',
      dataIndex: 'system_type',
      key: 'system_type',
      width: 130,
      render: (value?: SystemType) => value ? <Tag color={systemTagColor[value]}>{systemLabel[value]}</Tag> : '-',
    },
    {
      title: '设备',
      dataIndex: 'equipment_id',
      key: 'equipment_id',
      width: 150,
      render: (_value, record) => equipmentLabel({
        key: record.key,
        system_type: record.system_type,
        equipment_id: record.equipment_id,
      }),
    },
    { title: '记录数', dataIndex: 'records', key: 'records', width: 100 },
    {
      title: '有效记录',
      dataIndex: 'positiveRecords',
      key: 'positiveRecords',
      width: 110,
    },
    {
      title: selectedThemeConfig.aggregatable ? `总计 (${selectedThemeConfig.unit})` : `累计值 (${selectedThemeConfig.unit})`,
      dataIndex: 'total',
      key: 'total',
      render: (value: number) => formatThemeValue(selectedTheme, value),
      sorter: (a, b) => a.total - b.total,
    },
    {
      title: `平均值 (${selectedThemeConfig.unit})`,
      dataIndex: 'avg',
      key: 'avg',
      render: (value: number) => formatThemeValue(selectedTheme, value),
      sorter: (a, b) => a.avg - b.avg,
    },
    {
      title: `最大值 (${selectedThemeConfig.unit})`,
      dataIndex: 'max',
      key: 'max',
      render: (value: number) => formatThemeValue(selectedTheme, value),
      sorter: (a, b) => a.max - b.max,
    },
    {
      title: `最小值 (${selectedThemeConfig.unit})`,
      dataIndex: 'min',
      key: 'min',
      render: (value: number) => formatThemeValue(selectedTheme, value),
      sorter: (a, b) => a.min - b.min,
    },
  ];

  const handleRangeChange = (mode: RangeMode) => {
    setRangeMode(mode);
    if (mode !== 'custom') {
      setTimeRange({ start: '', end: '' });
    }
  };

  const handleCustomRangeChange: RangePickerProps['onChange'] = (dates) => {
    if (dates?.[0] && dates[1]) {
      setTimeRange({
        start: dates[0].format('YYYY-MM-DD HH:mm:ss'),
        end: dates[1].format('YYYY-MM-DD HH:mm:ss'),
      });
    } else {
      setTimeRange({ start: '', end: '' });
    }
  };

  if (loading && equipmentOptions.length === 0) {
    return <Loading tip="加载设备列表..." />;
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Space size="middle" wrap>
            <span style={{ fontWeight: 500 }}>主题:</span>
            <Radio.Group
              value={selectedTheme}
              onChange={(event) => setSelectedTheme(event.target.value)}
              optionType="button"
              buttonStyle="solid"
            >
              {THEMES.map((theme) => (
                <Radio.Button key={theme.value} value={theme.value}>
                  <Space size={4}>{theme.icon}<span>{theme.label}</span></Space>
                </Radio.Button>
              ))}
            </Radio.Group>
          </Space>
          <Space size="middle" wrap>
            <Space>
              <span style={{ fontWeight: 500 }}>系统:</span>
              <Select
                value={systemFilter}
                onChange={setSystemFilter}
                style={{ width: 180 }}
                options={systemOptions}
              />
            </Space>
            <Space>
              <span style={{ fontWeight: 500 }}>时间:</span>
              <Select
                value={rangeMode}
                onChange={handleRangeChange}
                style={{ width: 150 }}
                options={rangeOptions.map(({ value, label }) => ({ value, label }))}
              />
            </Space>
            {rangeMode === 'custom' && (
              <RangePicker showTime onChange={handleCustomRangeChange} format="YYYY-MM-DD HH:mm" />
            )}
          </Space>
          <Space align="start" style={{ width: '100%' }}>
            <span style={{ fontWeight: 500, paddingTop: 5 }}>设备:</span>
            <Select
              mode="multiple"
              value={selectedEquipmentKeys}
              onChange={setSelectedEquipmentKeys}
              style={{ minWidth: 520, maxWidth: 900 }}
              placeholder="请选择要对比的设备"
              maxTagCount="responsive"
              options={equipmentOptions.map((option) => ({
                label: equipmentLabel(option),
                value: option.key,
              }))}
            />
          </Space>
        </Space>
      </Card>

      {loading ? (
        <Loading tip="加载数据中..." />
      ) : (
        <>
          <Alert
            type="info"
            showIcon
            message="主题级查询已切换到统一 Gold 小时明细"
            description="设备列表覆盖冷机、热机和冷热电三联供。默认时间范围按数据湖最新小时回看；图表时间轴由所有选中设备的实际 stat_hour 合并生成，会随时间范围和设备选择同步变化。"
          />

          {totalRecords === 0 && (
            <Alert
              type="warning"
              showIcon
              message="当前条件没有查询到数据"
              description="请切换系统、设备或选择数据集覆盖的历史日期范围。"
            />
          )}

          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic title="对比设备数" value={selectedOptions.length} suffix="台/组" />
                <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                  记录 {totalRecords} 条，有效 {totalPositiveRecords} 条
                </div>
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title={averageMetricTitle(selectedTheme)}
                  value={overallAvg}
                  precision={2}
                  suffix={selectedThemeConfig.unit}
                  prefix={selectedThemeConfig.icon}
                  valueStyle={{ color: selectedThemeConfig.color }}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="最大值"
                  value={overallMax}
                  precision={2}
                  suffix={selectedThemeConfig.unit}
                  valueStyle={{ color: '#d4380d' }}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="最小值"
                  value={overallMin}
                  precision={2}
                  suffix={selectedThemeConfig.unit}
                  valueStyle={{ color: '#389e0d' }}
                />
              </Card>
            </Col>
          </Row>

          <Card title={`${selectedThemeConfig.label} - 多设备对比`}>
            {xAxisHours.length > 0 ? (
              <BaseChart option={chartOption} height={420} />
            ) : (
              <Empty description="暂无图表数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Card>

          <Card title={`${selectedThemeConfig.label} - 统计汇总`}>
            <Table
              columns={summaryColumns}
              dataSource={summaryData}
              rowKey="key"
              pagination={false}
              scroll={{ x: 1100 }}
            />
          </Card>
        </>
      )}
    </Space>
  );
}
