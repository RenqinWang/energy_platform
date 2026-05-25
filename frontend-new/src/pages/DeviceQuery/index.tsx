// 设备级数据查询页面
import { useState, useEffect } from 'react';
import { Alert, Badge, Card, Col, DatePicker, Empty, Row, Select, Space, Statistic, Switch, Table, Tag } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, SyncOutlined } from '@ant-design/icons';
import BaseChart from '../../components/Charts/BaseChart';
import Loading from '../../components/Common/Loading';
import { getSystemEquipment, getSystemSupplyCurve } from '../../api/report';
import { useAppStore } from '../../store/useAppStore';
import { createTimeSeriesChartOption } from '../../utils/chart';
import {
  formatDate,
  formatDateTime,
  formatEnergy,
  formatFlow,
  formatNumber,
  formatPower,
  formatPressure,
  formatTemperature,
} from '../../utils/format';
import type { SystemSupplyCurveRecord, SystemType } from '../../api/report';
import type { EChartsOption } from 'echarts';
import type { ColumnsType } from 'antd/es/table';
import type { RangePickerProps } from 'antd/es/date-picker';

const { RangePicker } = DatePicker;

type RangeMode = 'latest_96' | 'latest_168' | 'latest_1000' | 'custom';

const systemOptions: Array<{ value: SystemType; label: string }> = [
  { value: 'chiller', label: '冷机系统' },
  { value: 'heating', label: '热机系统' },
  { value: 'cchp', label: '冷热电三联供' },
];

const rangeOptions: Array<{ value: RangeMode; label: string; limit: number }> = [
  { value: 'latest_96', label: '最新96小时', limit: 96 },
  { value: 'latest_168', label: '最新168小时', limit: 168 },
  { value: 'latest_1000', label: '最新1000小时', limit: 1000 },
  { value: 'custom', label: '自定义日期', limit: 1000 },
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

const equipmentLabel = (systemType: SystemType, equipmentId: string) => {
  if (systemType === 'chiller') return equipmentId.replace('chiller_', '冷机 ');
  if (systemType === 'heating') return equipmentId.replace('heating_', '热机 ');
  if (equipmentId === 'cchp_system') return '三联供系统';
  return equipmentId;
};

const supplyTitle = (systemType: SystemType) => {
  if (systemType === 'chiller') return '供冷量';
  if (systemType === 'heating') return '供热量';
  return '综合供能量';
};

const n = (value?: number | null) => (typeof value === 'number' && Number.isFinite(value) ? value : 0);

const primarySupply = (item: SystemSupplyCurveRecord) =>
  n(item.supply_kwh) || n(item.cooling_supply_kwh) + n(item.heating_supply_kwh) + n(item.electric_supply_kwh);

const hasPositive = (data: SystemSupplyCurveRecord[], field: keyof SystemSupplyCurveRecord) =>
  data.some((item) => n(item[field] as number | null | undefined) > 0);

export default function DeviceQueryPage() {
  const storeSystemType = useAppStore((state) => state.systemType);
  const storeEquipmentId = useAppStore((state) => state.equipmentId);
  const setStoreSystemType = useAppStore((state) => state.setSystemType);
  const setStoreEquipmentId = useAppStore((state) => state.setEquipmentId);
  const [systemType, setSystemType] = useState<SystemType>(storeSystemType || 'chiller');
  const [equipmentList, setEquipmentList] = useState<string[]>([]);
  const [selectedEquipment, setSelectedEquipment] = useState<string>('');
  const [rangeMode, setRangeMode] = useState<RangeMode>('latest_96');
  const [timeRange, setTimeRange] = useState({ start: '', end: '' });
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<SystemSupplyCurveRecord[]>([]);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdateTime, setLastUpdateTime] = useState<string>('');

  useEffect(() => {
    let cancelled = false;

    const loadEquipment = async () => {
      setLoading(true);
      setSelectedEquipment('');
      setData([]);
      try {
        const equipment = await getSystemEquipment({ system_type: systemType });
        if (cancelled) return;

        const sorted = equipment.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
        setEquipmentList(sorted);
        const preferred = storeEquipmentId && sorted.includes(storeEquipmentId)
          ? storeEquipmentId
          : sorted.includes('chiller_10') ? 'chiller_10' : sorted[0] || '';
        setSelectedEquipment(preferred);
      } catch (error) {
        console.error('Failed to load equipment:', error);
        if (!cancelled) {
          setEquipmentList([]);
          setSelectedEquipment('');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadEquipment();

    return () => {
      cancelled = true;
    };
  }, [storeEquipmentId, systemType]);

  useEffect(() => {
    if (!selectedEquipment) return;

    let cancelled = false;
    const selectedRange = rangeOptions.find((item) => item.value === rangeMode) || rangeOptions[0];

    const loadData = async () => {
      setLoading(true);
      try {
        const result = await getSystemSupplyCurve({
          system_type: systemType,
          equipment_id: selectedEquipment,
          start_date: rangeMode === 'custom' && timeRange.start ? formatDate(timeRange.start) : undefined,
          end_date: rangeMode === 'custom' && timeRange.end ? formatDate(timeRange.end) : undefined,
          limit: selectedRange.limit,
        });

        if (!cancelled) {
          setData(result);
          setLastUpdateTime(new Date().toLocaleTimeString('zh-CN'));
        }
      } catch (error) {
        console.error('Failed to load device data:', error);
        if (!cancelled) setData([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadData();

    let intervalId: ReturnType<typeof setInterval> | null = null;
    if (autoRefresh) {
      intervalId = setInterval(loadData, 30000);
    }

    return () => {
      cancelled = true;
      if (intervalId) clearInterval(intervalId);
    };
  }, [selectedEquipment, systemType, rangeMode, timeRange, autoRefresh]);

  const selectedName = selectedEquipment ? equipmentLabel(systemType, selectedEquipment) : '-';
  const supplyName = supplyTitle(systemType);
  const validRows = data.filter((item) => primarySupply(item) > 0 || n(item.energy_consumption_kwh) > 0 || n(item.operation_rate) > 0);
  const supplyValues = data.map(primarySupply);
  const energyValues = data.map((item) => n(item.energy_consumption_kwh));
  const operationValues = data.map((item) => n(item.operation_rate));

  const stats = {
    total: data.length,
    validData: validRows.length,
    totalSupply: supplyValues.reduce((sum, value) => sum + value, 0),
    avgSupply: data.length ? supplyValues.reduce((sum, value) => sum + value, 0) / data.length : 0,
    maxSupply: data.length ? Math.max(...supplyValues) : 0,
    minSupply: data.length ? Math.min(...supplyValues) : 0,
    avgEnergy: data.length ? energyValues.reduce((sum, value) => sum + value, 0) / data.length : 0,
    avgOperationRate: data.length ? operationValues.reduce((sum, value) => sum + value, 0) / data.length : 0,
  };

  const dataQuality = stats.total > 0 ? (stats.validData / stats.total) * 100 : 0;
  const chartData = [...data].reverse();
  const chartSeries = [
    {
      name: supplyName,
      data: chartData.map(primarySupply),
      color: '#1677ff',
    },
    ...(hasPositive(data, 'cooling_supply_kwh') ? [{
      name: '供冷量',
      data: chartData.map((item) => n(item.cooling_supply_kwh)),
      color: '#13c2c2',
    }] : []),
    ...(hasPositive(data, 'heating_supply_kwh') ? [{
      name: '供热量',
      data: chartData.map((item) => n(item.heating_supply_kwh)),
      color: '#fa541c',
    }] : []),
    ...(hasPositive(data, 'electric_supply_kwh') ? [{
      name: '供电量',
      data: chartData.map((item) => n(item.electric_supply_kwh)),
      color: '#722ed1',
    }] : []),
    {
      name: '能耗',
      data: chartData.map((item) => n(item.energy_consumption_kwh)),
      color: '#52c41a',
    },
  ];

  const chartOption: EChartsOption = createTimeSeriesChartOption(
    chartData.map((item) => formatDateTime(item.stat_hour, 'MM-DD HH:mm')),
    chartSeries,
  );

  const columns: ColumnsType<SystemSupplyCurveRecord> = [
    {
      title: '时间',
      dataIndex: 'stat_hour',
      key: 'stat_hour',
      render: (text: string) => formatDateTime(text),
      width: 170,
      fixed: 'left',
    },
    {
      title: '系统',
      dataIndex: 'system_type',
      key: 'system_type',
      width: 120,
      render: (value: SystemType) => <Tag color={systemTagColor[value]}>{systemLabel[value]}</Tag>,
    },
    {
      title: `${supplyName} (kWh)`,
      key: 'supply_kwh',
      render: (_, record) => formatEnergy(primarySupply(record)),
      sorter: (a, b) => primarySupply(a) - primarySupply(b),
      width: 150,
    },
    {
      title: '供冷量',
      dataIndex: 'cooling_supply_kwh',
      key: 'cooling_supply_kwh',
      render: formatEnergy,
      sorter: (a, b) => n(a.cooling_supply_kwh) - n(b.cooling_supply_kwh),
      width: 130,
    },
    {
      title: '供热量',
      dataIndex: 'heating_supply_kwh',
      key: 'heating_supply_kwh',
      render: formatEnergy,
      sorter: (a, b) => n(a.heating_supply_kwh) - n(b.heating_supply_kwh),
      width: 130,
    },
    {
      title: '供电量',
      dataIndex: 'electric_supply_kwh',
      key: 'electric_supply_kwh',
      render: formatEnergy,
      sorter: (a, b) => n(a.electric_supply_kwh) - n(b.electric_supply_kwh),
      width: 130,
    },
    {
      title: '能耗',
      dataIndex: 'energy_consumption_kwh',
      key: 'energy_consumption_kwh',
      render: formatEnergy,
      sorter: (a, b) => n(a.energy_consumption_kwh) - n(b.energy_consumption_kwh),
      width: 130,
    },
    {
      title: '运行率',
      dataIndex: 'operation_rate',
      key: 'operation_rate',
      render: (value: number | null) => (value === null || value === undefined ? '-' : `${value.toFixed(1)}%`),
      sorter: (a, b) => n(a.operation_rate) - n(b.operation_rate),
      width: 110,
    },
    {
      title: '供水温度',
      dataIndex: 'avg_supply_temp',
      key: 'avg_supply_temp',
      render: formatTemperature,
      width: 120,
    },
    {
      title: '回水温度',
      dataIndex: 'avg_return_temp',
      key: 'avg_return_temp',
      render: formatTemperature,
      width: 120,
    },
    {
      title: '压力',
      dataIndex: 'avg_pressure',
      key: 'avg_pressure',
      render: formatPressure,
      width: 120,
    },
    {
      title: '流量',
      dataIndex: 'avg_flow',
      key: 'avg_flow',
      render: formatFlow,
      width: 130,
    },
    {
      title: '功率',
      dataIndex: 'avg_power',
      key: 'avg_power',
      render: formatPower,
      width: 130,
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

  if (loading && !selectedEquipment) {
    return <Loading tip="加载设备列表..." />;
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between" gutter={[16, 16]}>
          <Col flex="auto">
            <Space wrap size="middle">
              <Space>
                <span style={{ fontWeight: 500 }}>系统:</span>
                <Select
                  value={systemType}
                  onChange={(value) => {
                    setSystemType(value);
                    setStoreSystemType(value);
                    setStoreEquipmentId(undefined);
                  }}
                  style={{ width: 180 }}
                  options={systemOptions}
                />
              </Space>
              <Space>
                <span style={{ fontWeight: 500 }}>设备:</span>
                <Select
                  value={selectedEquipment || undefined}
                  onChange={(value) => {
                    setSelectedEquipment(value);
                    setStoreEquipmentId(value);
                  }}
                  placeholder="请选择设备"
                  style={{ width: 220 }}
                  options={equipmentList.map((equipmentId) => ({
                    label: equipmentLabel(systemType, equipmentId),
                    value: equipmentId,
                  }))}
                />
              </Space>
              <Space>
                <span style={{ fontWeight: 500 }}>范围:</span>
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
          </Col>
          <Col>
            <Space wrap>
              <span>自动刷新:</span>
              <Switch
                checked={autoRefresh}
                onChange={setAutoRefresh}
                checkedChildren="开"
                unCheckedChildren="关"
              />
              {lastUpdateTime && (
                <Space>
                  <Badge status={autoRefresh ? 'processing' : 'default'} />
                  <span style={{ fontSize: 12, color: '#666' }}>
                    {autoRefresh ? '持续查询' : '已暂停'} | 最后更新: {lastUpdateTime}
                  </span>
                </Space>
              )}
            </Space>
          </Col>
        </Row>
      </Card>

      {loading ? (
        <Loading tip="加载数据中..." />
      ) : (
        <>
          <Alert
            message={`${systemLabel[systemType]} / ${selectedName}`}
            description="设备级查询现在读取统一 Gold 小时明细，覆盖冷机、热机和冷热电三联供。默认范围按数据湖最新小时回看，不使用系统当前日期筛选，避免历史样本被误查为空。"
            type="info"
            showIcon
          />

          {data.length === 0 ? (
            <Alert
              message="当前条件没有查询到数据"
              description="请切换设备、选择最新小时范围，或用自定义日期查询数据集覆盖的历史日期。"
              type="warning"
              showIcon
            />
          ) : dataQuality < 20 ? (
            <Alert
              message="当前窗口多为停机或零负荷记录"
              description={`当前设备有效运行/供能记录占比为 ${dataQuality.toFixed(1)}%。这通常表示该设备在所选窗口内停机，不等同于数据未入湖。可以切换更长范围或查看其他设备。`}
              type="warning"
              showIcon
            />
          ) : null}

          {autoRefresh && (
            <Alert
              message={<Space><SyncOutlined spin /><span>自动刷新已启用</span></Space>}
              description="页面每30秒重新查询后端最新 Gold 表；如果上游继续写入新批次，查询结果会随刷新更新。"
              type="info"
              showIcon
            />
          )}

          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic title="数据记录数" value={stats.total} suffix="条" />
                <div style={{ marginTop: 8 }}>
                  <Tag color={dataQuality > 50 ? 'green' : dataQuality > 20 ? 'orange' : 'red'}>
                    有效记录 {stats.validData} 条 ({dataQuality.toFixed(1)}%)
                  </Tag>
                </div>
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title={`平均${supplyName}`}
                  value={stats.avgSupply}
                  precision={2}
                  suffix="kWh"
                  valueStyle={{ color: '#1677ff' }}
                  prefix={<ArrowUpOutlined />}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title={`累计${supplyName}`}
                  value={stats.totalSupply}
                  precision={2}
                  suffix="kWh"
                  valueStyle={{ color: '#13c2c2' }}
                />
                <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                  峰值 {formatNumber(stats.maxSupply)} kWh / 谷值 {formatNumber(stats.minSupply)} kWh
                </div>
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="平均能耗"
                  value={stats.avgEnergy}
                  precision={2}
                  suffix="kWh"
                  valueStyle={{ color: '#52c41a' }}
                  prefix={<ArrowDownOutlined />}
                />
                <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                  平均运行率 {formatNumber(stats.avgOperationRate, 1)}%
                </div>
              </Card>
            </Col>
          </Row>

          <Card title={`${selectedName} 历史供能与能耗时序`}>
            {data.length > 0 ? (
              <BaseChart option={chartOption} height={400} />
            ) : (
              <Empty description="暂无时序数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Card>

          <Card
            title="详细数据"
            extra={
              <Space wrap>
                <Tag color={systemTagColor[systemType]}>{systemLabel[systemType]}</Tag>
                <Tag>{selectedName}</Tag>
              </Space>
            }
          >
            <Table
              columns={columns}
              dataSource={data}
              rowKey={(record) => `${record.system_type}-${record.equipment_id}-${record.stat_hour}`}
              pagination={{
                pageSize: 20,
                showSizeChanger: true,
                showTotal: (total) => `共 ${total} 条记录`,
              }}
              scroll={{ x: 1600 }}
            />
          </Card>
        </>
      )}
    </Space>
  );
}
