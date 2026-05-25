import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Row, Select, Space, Statistic, Table, Tag } from 'antd';
import { DollarOutlined, FallOutlined, RiseOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import { getPriceHistory, getSystemEquipment, getSystemRevenueForecast } from '../../api/report';
import Loading from '../../components/Common/Loading';
import { formatDateTime, formatEnergy, formatNumber } from '../../utils/format';
import type { PriceHistoryRecord, SystemRevenueForecastRecord, SystemType } from '../../api/report';
import type { EChartsOption } from 'echarts';
import type { ColumnsType } from 'antd/es/table';

const systemOptions: Array<{ value: SystemType; label: string }> = [
  { value: 'chiller', label: '冷机系统' },
  { value: 'heating', label: '热机系统' },
  { value: 'cchp', label: '冷热电三联供' },
];

const systemLabel: Record<SystemType, string> = {
  chiller: '冷机系统',
  heating: '热机系统',
  cchp: '冷热电三联供',
};

const equipmentLabel = (systemType: SystemType, equipmentId: string) => {
  if (systemType === 'chiller') return equipmentId.replace('chiller_', '冷机 ');
  if (systemType === 'heating') return equipmentId.replace('heating_', '热机 ');
  if (equipmentId === 'cchp_system') return '三联供系统';
  return equipmentId;
};

const n = (value?: number | null) => (typeof value === 'number' && Number.isFinite(value) ? value : 0);

const formatMoney = (value?: number | null) => `${formatNumber(n(value), 2)} 元`;

const energyPriceLabel = (price?: number | null) => {
  const value = n(price);
  if (value >= 1.0) return <Tag color="red">峰时</Tag>;
  if (value >= 0.7) return <Tag color="orange">平时</Tag>;
  return <Tag color="green">谷时</Tag>;
};

export default function RevenueForecast() {
  const [systemType, setSystemType] = useState<SystemType>('chiller');
  const [equipmentList, setEquipmentList] = useState<string[]>([]);
  const [selectedEquipment, setSelectedEquipment] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<SystemRevenueForecastRecord[]>([]);
  const [priceHistory, setPriceHistory] = useState<PriceHistoryRecord[]>([]);
  const [error, setError] = useState<Error | null>(null);

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
        setSelectedEquipment(sorted.includes('chiller_10') ? 'chiller_10' : sorted[0] || '');
        setError(null);
      } catch (err) {
        console.error('Failed to load revenue equipment:', err);
        if (!cancelled) {
          setEquipmentList([]);
          setSelectedEquipment('');
          setError(err as Error);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadEquipment();

    return () => {
      cancelled = true;
    };
  }, [systemType]);

  useEffect(() => {
    if (!selectedEquipment) return;

    let cancelled = false;

    const loadRevenue = async () => {
      setLoading(true);
      try {
        const result = await getSystemRevenueForecast({
          system_type: systemType,
          equipment_id: selectedEquipment,
          limit: 24,
        });

        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        console.error('Failed to load system revenue forecast:', err);
        if (!cancelled) {
          setData([]);
          setError(err as Error);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    loadRevenue();

    return () => {
      cancelled = true;
    };
  }, [systemType, selectedEquipment]);

  useEffect(() => {
    let cancelled = false;
    const loadPrices = async () => {
      const end = new Date();
      const start = new Date(end.getTime() - 6 * 24 * 60 * 60 * 1000);
      const format = (date: Date) => date.toISOString().slice(0, 10);
      try {
        const rows = await getPriceHistory({
          start_date: format(start),
          end_date: format(end),
          limit: 200,
        });
        if (!cancelled) setPriceHistory(rows);
      } catch (err) {
        console.error('Failed to load price history:', err);
        if (!cancelled) setPriceHistory([]);
      }
    };

    loadPrices();

    return () => {
      cancelled = true;
    };
  }, []);

  const selectedName = selectedEquipment ? equipmentLabel(systemType, selectedEquipment) : '-';
  const forecastDate = data[0]?.forecast_date || data[0]?.dt || '-';
  const modelVersion = data[0]?.model_version || '-';
  const algorithm = data[0]?.algorithm || '-';

  const totals = useMemo(() => {
    const totalRevenue = data.reduce((sum, item) => sum + n(item.predicted_supply_revenue), 0);
    const totalCost = data.reduce((sum, item) => sum + n(item.predicted_energy_cost), 0);
    const totalProfit = data.reduce((sum, item) => sum + n(item.predicted_profit), 0);
    return {
      totalRevenue,
      totalCost,
      totalProfit,
      avgMargin: totalRevenue > 0 ? totalProfit / totalRevenue : 0,
      totalSupply: data.reduce((sum, item) => sum + n(item.predicted_supply_kwh), 0),
      totalEnergy: data.reduce((sum, item) => sum + n(item.predicted_energy_kwh), 0),
    };
  }, [data]);

  const hours = data.map((item) => formatDateTime(item.target_hour, 'HH:mm'));

  const revenueForecastChartOption: EChartsOption = {
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: {
      data: ['预测供能量', '预测能耗', '预测成本', '预测收入', '预测利润'],
      top: 8,
      type: 'scroll',
    },
    grid: { left: 48, right: 56, top: 64, bottom: 48, containLabel: true },
    xAxis: { type: 'category', data: hours, name: '时间' },
    yAxis: [
      { type: 'value', name: '能量 (kWh)', position: 'left' },
      { type: 'value', name: '金额 (元)', position: 'right' },
    ],
    series: [
      {
        name: '预测供能量',
        type: 'line',
        yAxisIndex: 0,
        data: data.map((item) => n(item.predicted_supply_kwh)),
        itemStyle: { color: '#1677ff' },
        smooth: true,
      },
      {
        name: '预测能耗',
        type: 'line',
        yAxisIndex: 0,
        data: data.map((item) => n(item.predicted_energy_kwh)),
        itemStyle: { color: '#52c41a' },
        smooth: true,
      },
      {
        name: '预测成本',
        type: 'bar',
        yAxisIndex: 1,
        data: data.map((item) => n(item.predicted_energy_cost)),
        itemStyle: { color: '#ff7875' },
      },
      {
        name: '预测收入',
        type: 'bar',
        yAxisIndex: 1,
        data: data.map((item) => n(item.predicted_supply_revenue)),
        itemStyle: { color: '#95de64' },
      },
      {
        name: '预测利润',
        type: 'line',
        yAxisIndex: 1,
        data: data.map((item) => n(item.predicted_profit)),
        itemStyle: { color: '#389e0d' },
        smooth: true,
      },
    ],
  };

  const priceChartOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    legend: {
      data: ['购电价格', '供冷价格', '供热价格'],
      top: 8,
    },
    grid: { left: 48, right: 28, top: 64, bottom: 48, containLabel: true },
    xAxis: { type: 'category', data: hours, name: '时间' },
    yAxis: { type: 'value', name: '元/kWh' },
    series: [
      {
        name: '购电价格',
        type: 'bar',
        data: data.map((item) => n(item.energy_price)),
        itemStyle: { color: '#fa8c16' },
      },
      {
        name: '供冷价格',
        type: 'line',
        data: data.map((item) => n(item.cooling_price)),
        itemStyle: { color: '#13c2c2' },
        smooth: true,
      },
      {
        name: '供热价格',
        type: 'line',
        data: data.map((item) => n(item.heating_price)),
        itemStyle: { color: '#f5222d' },
        smooth: true,
      },
    ],
  };

  const profitMarginChartOption: EChartsOption = {
    tooltip: {
      trigger: 'axis',
      formatter: '{b}<br/>利润率: {c}%',
    },
    grid: { left: 48, right: 28, top: 32, bottom: 48, containLabel: true },
    xAxis: { type: 'category', data: hours, name: '时间' },
    yAxis: { type: 'value', name: '利润率 (%)' },
    series: [
      {
        name: '利润率',
        type: 'line',
        data: data.map((item) => Number((n(item.profit_margin) * 100).toFixed(2))),
        itemStyle: { color: '#1677ff' },
        smooth: true,
        areaStyle: { opacity: 0.16 },
      },
    ],
  };

  const priceDates = Array.from(new Set(priceHistory.map((item) => item.effective_date))).sort();
  const historicalPriceChartOption: EChartsOption = {
    tooltip: { trigger: 'axis' },
    legend: { data: ['供冷价格', '供热价格', '电价'], top: 8 },
    grid: { left: 48, right: 28, top: 64, bottom: 48, containLabel: true },
    xAxis: { type: 'category', data: priceDates, name: '日期', axisLabel: { rotate: 30 } },
    yAxis: { type: 'value', name: '元/kWh' },
    series: ['cooling', 'heating', 'electricity'].map((type, idx) => {
      const label = type === 'cooling' ? '供冷价格' : type === 'heating' ? '供热价格' : '电价';
      const color = ['#13c2c2', '#f5222d', '#fa8c16'][idx];
      const rows = priceHistory.filter((item) => item.price_type === type);
      const byDate = new Map(rows.map((item) => [item.effective_date, item.price]));
      return {
        name: label,
        type: 'line',
        smooth: true,
        data: priceDates.map((date) => byDate.get(date) ?? null),
        itemStyle: { color },
      };
    }),
  };

  const columns: ColumnsType<SystemRevenueForecastRecord> = [
    {
      title: '时间',
      dataIndex: 'target_hour',
      key: 'target_hour',
      render: (value: string) => formatDateTime(value, 'MM-DD HH:mm'),
      width: 130,
      fixed: 'left',
    },
    {
      title: '电价类型',
      dataIndex: 'energy_price',
      key: 'price_type',
      render: energyPriceLabel,
      width: 100,
    },
    {
      title: '购电价',
      dataIndex: 'energy_price',
      key: 'energy_price',
      render: (value: number) => formatNumber(value, 2),
      width: 100,
    },
    {
      title: '预测供能量',
      dataIndex: 'predicted_supply_kwh',
      key: 'predicted_supply_kwh',
      render: formatEnergy,
      width: 130,
    },
    {
      title: '预测供冷量',
      dataIndex: 'predicted_cooling_kwh',
      key: 'predicted_cooling_kwh',
      render: formatEnergy,
      width: 130,
    },
    {
      title: '预测供热量',
      dataIndex: 'predicted_heating_kwh',
      key: 'predicted_heating_kwh',
      render: formatEnergy,
      width: 130,
    },
    {
      title: '预测供电量',
      dataIndex: 'predicted_electric_kwh',
      key: 'predicted_electric_kwh',
      render: formatEnergy,
      width: 130,
    },
    {
      title: '预测能耗',
      dataIndex: 'predicted_energy_kwh',
      key: 'predicted_energy_kwh',
      render: formatEnergy,
      width: 130,
    },
    {
      title: '预测成本',
      dataIndex: 'predicted_energy_cost',
      key: 'predicted_energy_cost',
      render: formatMoney,
      width: 130,
    },
    {
      title: '预测收入',
      dataIndex: 'predicted_supply_revenue',
      key: 'predicted_supply_revenue',
      render: formatMoney,
      width: 130,
    },
    {
      title: '预测利润',
      dataIndex: 'predicted_profit',
      key: 'predicted_profit',
      render: (value: number) => (
        <span style={{ color: n(value) >= 0 ? '#389e0d' : '#d4380d' }}>
          {formatMoney(value)}
        </span>
      ),
      width: 130,
    },
    {
      title: '利润率',
      dataIndex: 'profit_margin',
      key: 'profit_margin',
      render: (value: number) => `${formatNumber(n(value) * 100, 2)}%`,
      width: 100,
    },
  ];

  if (loading && !selectedEquipment) {
    return <Loading tip="加载收益预测设备..." />;
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between" gutter={[16, 16]}>
          <Col>
            <Space size="large">
              <DollarOutlined style={{ fontSize: 32, color: '#1677ff' }} />
              <div>
                <h2 style={{ margin: 0 }}>收益预测</h2>
                <p style={{ margin: 0, color: '#666' }}>
                  基于供能趋势预测和能源价格计算未来24小时收益
                </p>
              </div>
            </Space>
          </Col>
          <Col>
            <Space wrap>
              <span style={{ fontWeight: 500 }}>系统:</span>
              <Select
                value={systemType}
                onChange={setSystemType}
                style={{ width: 180 }}
                options={systemOptions}
              />
              <span style={{ fontWeight: 500 }}>设备:</span>
              <Select
                value={selectedEquipment || undefined}
                onChange={setSelectedEquipment}
                placeholder="请选择设备"
                style={{ width: 220 }}
                options={equipmentList.map((equipmentId) => ({
                  label: equipmentLabel(systemType, equipmentId),
                  value: equipmentId,
                }))}
              />
            </Space>
          </Col>
        </Row>
      </Card>

      {loading ? (
        <Loading tip="加载收益预测数据..." />
      ) : error ? (
        <Alert message="加载失败" description={error.message} type="error" showIcon />
      ) : data.length > 0 ? (
        <>
          <Alert
            type={totals.totalProfit < 0 ? 'warning' : 'info'}
            showIcon
            message={`${systemLabel[systemType]} / ${selectedName}`}
            description={
              totals.totalProfit < 0
                ? `预测日期 ${forecastDate}，未来24小时预计亏损 ${formatMoney(totals.totalProfit)}，建议降低高价时段供能或调整设备负荷。模型 ${modelVersion}，算法 ${algorithm}。`
                : `预测日期 ${forecastDate}，模型 ${modelVersion}，算法 ${algorithm}。当前页面读取统一 Gold 收益预测表，覆盖冷机、热机和冷热电三联供。`
            }
          />

          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="预测总收入"
                  value={totals.totalRevenue}
                  precision={2}
                  suffix="元"
                  prefix={<DollarOutlined />}
                  valueStyle={{ color: '#389e0d' }}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="预测总成本"
                  value={totals.totalCost}
                  precision={2}
                  suffix="元"
                  prefix={<DollarOutlined />}
                  valueStyle={{ color: '#d4380d' }}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="预测总利润"
                  value={totals.totalProfit}
                  precision={2}
                  suffix="元"
                  prefix={totals.totalProfit >= 0 ? <RiseOutlined /> : <FallOutlined />}
                  valueStyle={{ color: totals.totalProfit >= 0 ? '#389e0d' : '#d4380d' }}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card>
                <Statistic
                  title="平均利润率"
                  value={totals.avgMargin * 100}
                  precision={2}
                  suffix="%"
                  valueStyle={{ color: '#1677ff' }}
                />
                <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                  供能 {formatEnergy(totals.totalSupply)} / 能耗 {formatEnergy(totals.totalEnergy)}
                </div>
              </Card>
            </Col>
          </Row>

          <Card title="未来24小时收益预测">
            <ReactECharts option={revenueForecastChartOption} style={{ height: 430 }} />
          </Card>

          <Row gutter={[16, 16]}>
            <Col xs={24} xl={12}>
              <Card title="价格分布">
                <ReactECharts option={priceChartOption} style={{ height: 340 }} />
              </Card>
            </Col>
            <Col xs={24} xl={12}>
              <Card title="利润率趋势">
                <ReactECharts option={profitMarginChartOption} style={{ height: 340 }} />
              </Card>
            </Col>
          </Row>

          <Card title="近7日能源价格变化">
            <ReactECharts option={historicalPriceChartOption} style={{ height: 340 }} />
          </Card>

          <Card title="详细预测数据">
            <Table
              columns={columns}
              dataSource={data}
              rowKey={(record) => `${record.system_type}-${record.equipment_id}-${record.target_hour}`}
              pagination={false}
              scroll={{ x: 1700 }}
            />
          </Card>

          <Card title="运行分析与决策建议">
            <Row gutter={[16, 16]}>
              <Col xs={24} md={12}>
                <Space direction="vertical" size="small">
                  <strong>价格策略</strong>
                  <span>峰时电价较高时优先控制能耗，谷时电价较低时可结合预测负荷安排蓄能或提前供能。</span>
                  <span>当前收益测算已区分购电成本、供冷收益、供热收益和三联供供电收益。</span>
                </Space>
              </Col>
              <Col xs={24} md={12}>
                <Space direction="vertical" size="small">
                  <strong>运行建议</strong>
                  <span>当前平均利润率为 {formatNumber(totals.avgMargin * 100, 2)}%，总利润 {formatMoney(totals.totalProfit)}。</span>
                  <span>若预测利润偏低，应优先检查能耗价格、设备效率和负荷分配策略。</span>
                </Space>
              </Col>
            </Row>
          </Card>
        </>
      ) : (
        <Alert
          message="暂无数据"
          description="当前系统和设备没有收益预测数据，请先运行统一趋势预测训练任务。"
          type="info"
          showIcon
        />
      )}
    </Space>
  );
}
