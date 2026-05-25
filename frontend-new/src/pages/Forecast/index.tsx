import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Descriptions, Empty, List, Row, Select, Space, Statistic, Tag, Tabs } from 'antd';
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  BulbOutlined,
  CheckCircleOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
  LineChartOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import BaseChart from '../../components/Charts/BaseChart';
import Loading from '../../components/Common/Loading';
import { getSystemEquipment, getSystemSupplyCurve, getOperationAdvice } from '../../api/report';
import { getSystemForecast, getSystemForecastMetrics } from '../../api/forecast';
import { formatDateTime } from '../../utils/format';
import { ADVICE_TYPE_LABELS, RISK_LEVEL_COLORS, RISK_LEVEL_LABELS } from '../../types/advice';
import type { AdviceRecord } from '../../types/advice';
import type { SystemType, SystemSupplyCurveRecord } from '../../api/report';
import type { SystemForecastMetric, SystemForecastRecord } from '../../types/forecast';
import type { EChartsOption } from 'echarts';

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

const formatMetric = (value?: number | null, digits = 2) =>
  typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '-';

const safeNumber = (value?: number | null) =>
  typeof value === 'number' && Number.isFinite(value) ? value : 0;

const parseTopFeatures = (raw?: string) => {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as Array<{ feature: string; importance: number }>;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

export default function ForecastPage() {
  const [systemType, setSystemType] = useState<SystemType>('chiller');
  const [equipmentList, setEquipmentList] = useState<string[]>([]);
  const [selectedEquipment, setSelectedEquipment] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [forecastData, setForecastData] = useState<SystemForecastRecord[]>([]);
  const [historicalData, setHistoricalData] = useState<SystemSupplyCurveRecord[]>([]);
  const [metricData, setMetricData] = useState<SystemForecastMetric[]>([]);
  const [adviceData, setAdviceData] = useState<AdviceRecord[]>([]);

  useEffect(() => {
    let cancelled = false;

    const loadEquipment = async () => {
      setLoading(true);
      try {
        const equipment = await getSystemEquipment({ system_type: systemType });
        if (cancelled) return;

        const sorted = equipment.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
        setEquipmentList(sorted);
        setSelectedEquipment(sorted.includes('chiller_10') ? 'chiller_10' : sorted[0] || '');
      } catch {
        if (!cancelled) {
          setEquipmentList([]);
          setSelectedEquipment('');
        }
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

    const loadData = async () => {
      setLoading(true);
      try {
        const [forecast, historical, metrics, advice] = await Promise.all([
          getSystemForecast({ system_type: systemType, equipment_id: selectedEquipment, limit: 24 }),
          getSystemSupplyCurve({ system_type: systemType, equipment_id: selectedEquipment, limit: 96 }),
          getSystemForecastMetrics({ system_type: systemType, equipment_id: selectedEquipment, limit: 1 }),
          getOperationAdvice({ system_type: systemType, equipment_id: selectedEquipment, limit: 20 }) as Promise<AdviceRecord[]>,
        ]);

        if (!cancelled) {
          setForecastData(forecast);
          setHistoricalData(historical);
          setMetricData(metrics);
          setAdviceData(advice);
        }
      } catch (error) {
        console.error('Failed to load forecast data:', error);
        if (!cancelled) {
          setForecastData([]);
          setHistoricalData([]);
          setMetricData([]);
          setAdviceData([]);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    loadData();

    return () => {
      cancelled = true;
    };
  }, [selectedEquipment, systemType]);

  const metric = metricData[0];
  const topFeatures = useMemo(() => parseTopFeatures(metric?.top_features), [metric?.top_features]);
  const forecastValues = forecastData.map((item) => item.predicted_supply_kwh || 0);
  const avgPrediction = forecastValues.length ? forecastValues.reduce((sum, value) => sum + value, 0) / forecastValues.length : 0;
  const maxPrediction = forecastValues.length ? Math.max(...forecastValues) : 0;
  const minPrediction = forecastValues.length ? Math.min(...forecastValues) : 0;
  const recentHistory = historicalData.slice(0, 24);
  const inferredRecentSupply = recentHistory.reduce((sum, item) => sum + safeNumber(item.supply_kwh ?? item.cooling_supply_kwh), 0);
  const inferredRecentOperationRate = recentHistory.length
    ? recentHistory.reduce((sum, item) => sum + safeNumber(item.operation_rate), 0) / recentHistory.length
    : 0;
  const recent24Supply = forecastData[0]?.recent_24_supply_kwh ?? inferredRecentSupply;
  const recent24OperationRate = forecastData[0]?.recent_24_operation_rate ?? inferredRecentOperationRate;
  const recentlyInactive =
    forecastData[0]?.current_operation_status === 'recent_inactive'
    || (recent24Supply <= 1e-6 && recent24OperationRate <= 1e-6);
  const hasTrendDemand = maxPrediction > 0.01;
  const forecastInterpretation = forecastData[0]?.forecast_interpretation;

  const historyForChart = [...historicalData].reverse();
  const xAxisData = [
    ...historyForChart.map((item) => formatDateTime(item.stat_hour, 'MM-DD HH:mm')),
    ...forecastData.map((item) => formatDateTime(item.target_hour, 'MM-DD HH:mm')),
  ];
  const historyValues = historyForChart.map((item) => item.supply_kwh || item.cooling_supply_kwh || 0);
  const forecastSeriesOffset = Array(historyValues.length).fill(null);
  const historySeriesTail = Array(forecastData.length).fill(null);

  const chartOption: EChartsOption = {
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['历史供能量', '预测供能量', '置信下界', '置信上界'], top: 8 },
    grid: { left: 48, right: 28, top: 64, bottom: 48, containLabel: true },
    xAxis: { type: 'category', data: xAxisData, axisLabel: { rotate: 30 } },
    yAxis: { type: 'value', name: 'kWh' },
    series: [
      {
        name: '历史供能量',
        type: 'line',
        data: [...historyValues, ...historySeriesTail],
        smooth: true,
        itemStyle: { color: '#1677ff' },
      },
      {
        name: '预测供能量',
        type: 'line',
        data: [...forecastSeriesOffset, ...forecastValues],
        smooth: true,
        itemStyle: { color: '#fa8c16' },
      },
      {
        name: '置信下界',
        type: 'line',
        data: [...forecastSeriesOffset, ...forecastData.map((item) => item.confidence_lower ?? null)],
        smooth: true,
        lineStyle: { type: 'dashed' },
        itemStyle: { color: '#95de64' },
      },
      {
        name: '置信上界',
        type: 'line',
        data: [...forecastSeriesOffset, ...forecastData.map((item) => item.confidence_upper ?? null)],
        smooth: true,
        lineStyle: { type: 'dashed' },
        itemStyle: { color: '#ff7875' },
      },
    ],
  };

  const adviceByRisk = {
    high: adviceData.filter((item) => item.risk_level === 'high'),
    medium: adviceData.filter((item) => item.risk_level === 'medium'),
    low: adviceData.filter((item) => item.risk_level === 'low'),
  };

  if (loading && !selectedEquipment) {
    return <Loading tip="加载设备列表..." />;
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between" gutter={[16, 16]}>
          <Col>
            <Space size="large">
              <LineChartOutlined style={{ fontSize: 32, color: '#1677ff' }} />
              <div>
                <h2 style={{ margin: 0 }}>供能趋势预测</h2>
                <p style={{ margin: 0, color: '#666' }}>
                  {systemLabel[systemType]}未来24小时供能量预测
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
                value={selectedEquipment}
                onChange={setSelectedEquipment}
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
        <Loading tip="加载预测数据..." />
      ) : (
        <>
          <Row gutter={[16, 16]}>
            <Col xs={24} md={8}>
              <Card>
                <Statistic
                  title="预测平均供能量"
                  value={avgPrediction}
                  precision={2}
                  suffix="kWh"
                  prefix={<DashboardOutlined />}
                  valueStyle={{ color: '#1677ff' }}
                />
                <div style={{ marginTop: 12, fontSize: 13, color: '#666' }}>
                  模型 {metric?.algorithm || forecastData[0]?.algorithm || '-'}
                </div>
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card>
                <Statistic
                  title="预测峰值"
                  value={maxPrediction}
                  precision={2}
                  suffix="kWh"
                  prefix={<ArrowUpOutlined />}
                  valueStyle={{ color: '#d4380d' }}
                />
                <div style={{ marginTop: 12, fontSize: 13, color: '#666' }}>
                  未来24小时最大预测负荷
                </div>
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card>
                <Statistic
                  title="预测谷值"
                  value={minPrediction}
                  precision={2}
                  suffix="kWh"
                  prefix={<ArrowDownOutlined />}
                  valueStyle={{ color: '#389e0d' }}
                />
                <div style={{ marginTop: 12, fontSize: 13, color: '#666' }}>
                  未来24小时最小预测负荷
                </div>
              </Card>
            </Col>
          </Row>

          {forecastData.length > 0 && (
            <Alert
              showIcon
              type={recentlyInactive && hasTrendDemand ? 'warning' : 'info'}
              message={
                recentlyInactive && hasTrendDemand
                  ? '当前停机，但模型预测存在潜在供能需求'
                  : '预测结果按历史规律保留趋势曲线'
              }
              description={
                recentlyInactive
                  ? `最近24小时供能量 ${formatMetric(recent24Supply)} kWh，平均运行率 ${formatMetric(recent24OperationRate)}%。预测曲线表示季节、日期、时段和历史负荷规律下的潜在需求，不代表设备已经被调度启机。`
                  : forecastInterpretation || `最近24小时供能量 ${formatMetric(recent24Supply)} kWh，平均运行率 ${formatMetric(recent24OperationRate)}%。`
              }
            />
          )}

          <Card title={<Space><LineChartOutlined /><span>历史负荷与未来24小时预测</span></Space>}>
            {forecastData.length > 0 ? (
              <BaseChart option={chartOption} height={430} />
            ) : (
              <Empty description="暂无预测数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Card>

          <Card title="模型训练与评估">
            {metric ? (
              <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                <Row gutter={[16, 16]}>
                  <Col xs={12} lg={6}><Statistic title="MAE" value={formatMetric(metric.mae)} suffix="kWh" /></Col>
                  <Col xs={12} lg={6}><Statistic title="RMSE" value={formatMetric(metric.rmse)} suffix="kWh" /></Col>
                  <Col xs={12} lg={6}><Statistic title="R2" value={formatMetric(metric.r2, 3)} /></Col>
                  <Col xs={12} lg={6}><Statistic title="MAPE" value={formatMetric(metric.mape)} suffix="%" /></Col>
                </Row>

                <Descriptions bordered size="small" column={1}>
                  <Descriptions.Item label="特征设计">{metric.feature_design}</Descriptions.Item>
                  <Descriptions.Item label="时间窗口构造">{metric.window_design}</Descriptions.Item>
                  <Descriptions.Item label="模型选择依据">{metric.model_reason}</Descriptions.Item>
                  <Descriptions.Item label="训练方式">{metric.training_method}</Descriptions.Item>
                  <Descriptions.Item label="评估方式">{metric.evaluation_method}</Descriptions.Item>
                  <Descriptions.Item label="结果解释">{metric.result_summary}</Descriptions.Item>
                </Descriptions>

                <Space wrap>
                  {topFeatures.map((item) => (
                    <Tag key={item.feature} color="blue">
                      {item.feature}: {item.importance.toFixed(3)}
                    </Tag>
                  ))}
                </Space>
              </Space>
            ) : (
              <Alert
                type="info"
                showIcon
                message="暂无模型评估记录"
                description="当前选择对象没有读取到评估记录，请先运行统一趋势预测训练任务。"
              />
            )}
          </Card>

          <Card title={<Space><BulbOutlined /><span>智能运行建议</span></Space>}>
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              {recentlyInactive && hasTrendDemand && (
                <Alert
                  type="warning"
                  showIcon
                  message="建议核对调度计划"
                  description="该设备最近持续停机，但趋势模型预测未来存在供能需求。应结合设备可用性、人工调度计划和系统总负荷判断是否需要启机，而不是直接把预测值当作实际运行结果。"
                />
              )}
              {adviceData.length > 0 ? (
                <Tabs
                  defaultActiveKey="all"
                  items={[
                    { key: 'all', label: `全部建议 (${adviceData.length})`, children: <AdviceList data={adviceData} /> },
                    { key: 'high', label: `高风险 (${adviceByRisk.high.length})`, children: adviceByRisk.high.length ? <AdviceList data={adviceByRisk.high} /> : <Empty description="暂无高风险建议" image={Empty.PRESENTED_IMAGE_SIMPLE} /> },
                    { key: 'medium', label: `中风险 (${adviceByRisk.medium.length})`, children: adviceByRisk.medium.length ? <AdviceList data={adviceByRisk.medium} /> : <Empty description="暂无中风险建议" image={Empty.PRESENTED_IMAGE_SIMPLE} /> },
                    { key: 'low', label: `低风险 (${adviceByRisk.low.length})`, children: adviceByRisk.low.length ? <AdviceList data={adviceByRisk.low} /> : <Empty description="暂无低风险建议" image={Empty.PRESENTED_IMAGE_SIMPLE} /> },
                  ]}
                />
              ) : (
                <Alert message="暂无专项建议" description="当前选择对象没有活跃运行建议。" type="success" showIcon />
              )}
            </Space>
          </Card>
        </>
      )}
    </Space>
  );
}

function AdviceList({ data }: { data: AdviceRecord[] }) {
  return (
    <List
      dataSource={data}
      renderItem={(item) => (
        <List.Item style={{ padding: '16px 0', borderBottom: '1px solid #f0f0f0' }}>
          <List.Item.Meta
            avatar={
              <div style={{
                width: 48,
                height: 48,
                borderRadius: '50%',
                background: RISK_LEVEL_COLORS[item.risk_level],
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'white',
                fontSize: 20,
              }}>
                {item.risk_level === 'high' ? <WarningOutlined /> :
                  item.risk_level === 'medium' ? <InfoCircleOutlined /> :
                    <CheckCircleOutlined />}
              </div>
            }
            title={
              <Space size="middle" wrap>
                <span style={{ fontSize: 16, fontWeight: 500 }}>{item.advice_text}</span>
                <Tag color={RISK_LEVEL_COLORS[item.risk_level]}>{RISK_LEVEL_LABELS[item.risk_level]}</Tag>
                <Tag>{ADVICE_TYPE_LABELS[item.advice_type]}</Tag>
              </Space>
            }
            description={
              <Space direction="vertical" size="small" style={{ marginTop: 8 }}>
                <Space><span style={{ color: '#999' }}>时间:</span><span>{formatDateTime(item.advice_time)}</span></Space>
                {item.evidence_metrics && <Space><span style={{ color: '#999' }}>证据:</span><span>{item.evidence_metrics}</span></Space>}
                <Space><span style={{ color: '#999' }}>规则ID:</span><span style={{ fontFamily: 'monospace', fontSize: 12 }}>{item.rule_id}</span></Space>
              </Space>
            }
          />
        </List.Item>
      )}
    />
  );
}
