// 系统级综合展示页面
import { useState, useEffect, useMemo } from 'react';
import { Card, Row, Col, Statistic, Table, Progress, Space, Tag, Badge } from 'antd';
import { Alert } from 'antd';
import {
  DashboardOutlined,
  FireOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  CloseCircleOutlined,
  SyncOutlined
} from '@ant-design/icons';
import BaseChart from '../../components/Charts/BaseChart';
import Loading from '../../components/Common/Loading';
import { getSystemDailyReport, getSystemSupplyCurve } from '../../api/report';
import { formatDateTime, formatEnergy } from '../../utils/format';
import type { EChartsOption } from 'echarts';
import type { SystemDailyReportRecord, SystemSupplyCurveRecord, SystemType } from '../../api/report';
import type { BadgeProps } from 'antd';

interface EquipmentStatus {
  system_type: SystemType;
  equipment_id: string;
  status: 'online' | 'offline' | 'warning';
  supply: number;
  cooling: number;
  heating: number;
  electricity: number;
  cop: number;
}

interface SystemTrendPoint {
  stat_hour: string;
  supply: number;
  cooling: number;
  heating: number;
  energy: number;
}

const calculateCop = (record?: SystemSupplyCurveRecord): number => {
  if (!record) return 0;

  const totalSupply = record.supply_kwh || record.cooling_supply_kwh || 0;
  const energyConsumption = record.energy_consumption_kwh || 0;
  if (totalSupply > 0 && energyConsumption > 0) {
    return totalSupply / energyConsumption;
  }

  const coolingCapacity = record.cooling_capacity_kw || 0;
  const avgPower = record.avg_power || 0;
  return coolingCapacity > 0 && avgPower > 0 ? coolingCapacity / avgPower : 0;
};

const inferEquipmentStatus = (record?: SystemSupplyCurveRecord): EquipmentStatus['status'] => {
  if (!record) return 'offline';

  const runMinutes = record.run_minutes || 0;
  const operationRate = record.operation_rate || 0;
  const energyConsumption = record.energy_consumption_kwh || 0;
  const supply = record.supply_kwh || 0;
  const recordCount = record.record_count || 0;
  const cop = calculateCop(record);

  if (runMinutes <= 0 && operationRate <= 0 && energyConsumption <= 0 && supply <= 0) {
    return 'offline';
  }

  if ((recordCount > 0 && recordCount < 3) || (cop > 0 && cop < 0.5)) {
    return 'warning';
  }

  return 'online';
};

const systemTypeLabel: Record<SystemType, string> = {
  chiller: '冷机',
  heating: '热机',
  cchp: '冷热电三联供',
};

const n = (value?: number | null) => (typeof value === 'number' && Number.isFinite(value) ? value : 0);

const supplyValue = (record: SystemSupplyCurveRecord) => {
  const explicitSupply = n(record.supply_kwh);
  if (explicitSupply > 0) return explicitSupply;
  return n(record.cooling_supply_kwh) + n(record.heating_supply_kwh) + n(record.electric_supply_kwh);
};

const buildSystemTrend = (rows: SystemSupplyCurveRecord[], hourCount = 24): SystemTrendPoint[] => {
  const grouped = new Map<string, SystemTrendPoint>();

  rows.forEach((record) => {
    if (!record.stat_hour) return;

    const current = grouped.get(record.stat_hour) || {
      stat_hour: record.stat_hour,
      supply: 0,
      cooling: 0,
      heating: 0,
      energy: 0,
    };

    current.supply += supplyValue(record);
    current.cooling += n(record.cooling_supply_kwh);
    current.heating += n(record.heating_supply_kwh);
    current.energy += n(record.energy_consumption_kwh);
    grouped.set(record.stat_hour, current);
  });

  return Array.from(grouped.values())
    .sort((a, b) => a.stat_hour.localeCompare(b.stat_hour))
    .slice(-hourCount);
};

export default function SystemOverviewPage() {
  const [loading, setLoading] = useState(true);
  const [equipmentStatus, setEquipmentStatus] = useState<EquipmentStatus[]>([]);
  const [dailyReport, setDailyReport] = useState<SystemDailyReportRecord[]>([]);
  const [recentData, setRecentData] = useState<SystemSupplyCurveRecord[]>([]);
  const [lastUpdateTime, setLastUpdateTime] = useState<string>('');

  // 加载所有数据
  useEffect(() => {
    let mounted = true;
    let inFlight = false;

    const loadData = async (showInitialLoading = false) => {
      if (inFlight) return;
      inFlight = true;

      if (showInitialLoading) {
        setLoading(true);
      }

      try {
        const [hourlyData, reportData] = await Promise.all([
          getSystemSupplyCurve({ limit: 500 }),
          getSystemDailyReport({ limit: 80 })
        ]);

        const latestByEquipment = new Map<string, SystemSupplyCurveRecord>();
        hourlyData.forEach((item) => {
          const key = `${item.system_type}-${item.equipment_id}`;
          if (!latestByEquipment.has(key)) {
            latestByEquipment.set(key, item);
          }
        });

        const statusData = Array.from(latestByEquipment.values())
          .map((latest) => ({
            system_type: latest.system_type,
            equipment_id: latest.equipment_id,
            status: inferEquipmentStatus(latest),
            supply: latest.supply_kwh || 0,
            cooling: latest.cooling_supply_kwh || 0,
            heating: latest.heating_supply_kwh || 0,
            electricity: latest.energy_consumption_kwh || 0,
            cop: calculateCop(latest),
          }))
          .sort((a, b) => `${a.system_type}-${a.equipment_id}`.localeCompare(`${b.system_type}-${b.equipment_id}`, undefined, { numeric: true }));

        if (mounted) {
          setEquipmentStatus(statusData);
          setDailyReport(reportData);
          setRecentData(hourlyData);
          setLastUpdateTime(new Date().toLocaleTimeString('zh-CN'));
        }
      } catch (error) {
        console.error('Failed to load data:', error);
      } finally {
        if (mounted) {
          setLoading(false);
        }
        inFlight = false;
      }
    };

    loadData(true);

    // HDFS/Spark 查询比普通内存API重，30秒刷新能展示更新能力且避免请求堆积。
    const intervalId = setInterval(() => loadData(false), 30000);

    return () => {
      mounted = false;
      clearInterval(intervalId);
    };
  }, []);

  const validCopValues = equipmentStatus.map(e => e.cop).filter(cop => cop > 0);

  // 计算系统统计
  const systemStats = {
    totalEquipment: equipmentStatus.length,
    onlineEquipment: equipmentStatus.filter(e => e.status === 'online').length,
    warningEquipment: equipmentStatus.filter(e => e.status === 'warning').length,
    offlineEquipment: equipmentStatus.filter(e => e.status === 'offline').length,
    totalSupply: equipmentStatus.reduce((sum, e) => sum + e.supply, 0),
    totalCooling: equipmentStatus.reduce((sum, e) => sum + e.cooling, 0),
    totalHeating: equipmentStatus.reduce((sum, e) => sum + e.heating, 0),
    totalElectricity: equipmentStatus.reduce((sum, e) => sum + e.electricity, 0),
    avgCOP: validCopValues.length > 0
      ? validCopValues.reduce((sum, cop) => sum + cop, 0) / validCopValues.length
      : 0
  };

  // 系统健康度
  const healthScore = systemStats.totalEquipment > 0
    ? Math.round((systemStats.onlineEquipment / systemStats.totalEquipment) * 100)
    : 0;
  const systemTrend = useMemo(() => buildSystemTrend(recentData, 24), [recentData]);

  // 准备趋势图表
  const trendChartOption: EChartsOption = {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' }
    },
    legend: {
      data: ['总供能量', '供冷量', '供热量', '电/燃气耗'],
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
      data: systemTrend.map(d => formatDateTime(d.stat_hour, 'MM-DD HH:mm'))
    },
    yAxis: {
      type: 'value',
      name: '能耗 (kWh)'
    },
    series: [
      {
        name: '总供能量',
        type: 'line',
        data: systemTrend.map(d => Number(d.supply.toFixed(2))),
        smooth: true,
        itemStyle: { color: '#1890ff' },
        areaStyle: { opacity: 0.3 }
      },
      {
        name: '供冷量',
        type: 'line',
        data: systemTrend.map(d => Number(d.cooling.toFixed(2))),
        smooth: true,
        itemStyle: { color: '#13c2c2' }
      },
      {
        name: '供热量',
        type: 'line',
        data: systemTrend.map(d => Number(d.heating.toFixed(2))),
        smooth: true,
        itemStyle: { color: '#fa541c' }
      },
      {
        name: '电/燃气耗',
        type: 'line',
        data: systemTrend.map(d => Number(d.energy.toFixed(2))),
        smooth: true,
        itemStyle: { color: '#52c41a' },
        areaStyle: { opacity: 0.3 }
      }
    ]
  };

  // 设备状态表格列
  const statusColumns = [
    {
      title: '系统',
      dataIndex: 'system_type',
      key: 'system_type',
      width: 130,
      render: (value: SystemType) => <Tag color={value === 'chiller' ? 'blue' : value === 'heating' ? 'volcano' : 'purple'}>{systemTypeLabel[value]}</Tag>
    },
    {
      title: '设备ID',
      dataIndex: 'equipment_id',
      key: 'equipment_id',
      fixed: 'left' as const,
      width: 150
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const config = {
          online: { color: 'success', icon: <CheckCircleOutlined />, text: '在线' },
          warning: { color: 'warning', icon: <WarningOutlined />, text: '告警' },
          offline: { color: 'error', icon: <CloseCircleOutlined />, text: '离线' }
        }[status] || { color: 'default', icon: null, text: '未知' };

        return (
          <Badge status={config.color as BadgeProps['status']} text={
            <Space>
              {config.icon}
              {config.text}
            </Space>
          } />
        );
      }
    },
    {
      title: '总供能量 (kWh)',
      dataIndex: 'supply',
      key: 'supply',
      render: (val: number) => formatEnergy(val),
      sorter: (a: EquipmentStatus, b: EquipmentStatus) => a.supply - b.supply
    },
    {
      title: '供冷量 (kWh)',
      dataIndex: 'cooling',
      key: 'cooling',
      render: (val: number) => formatEnergy(val),
      sorter: (a: EquipmentStatus, b: EquipmentStatus) => a.cooling - b.cooling
    },
    {
      title: '供热量 (kWh)',
      dataIndex: 'heating',
      key: 'heating',
      render: (val: number) => formatEnergy(val),
      sorter: (a: EquipmentStatus, b: EquipmentStatus) => a.heating - b.heating
    },
    {
      title: '电耗 (kWh)',
      dataIndex: 'electricity',
      key: 'electricity',
      render: (val: number) => formatEnergy(val),
      sorter: (a: EquipmentStatus, b: EquipmentStatus) => a.electricity - b.electricity
    },
    {
      title: 'COP',
      dataIndex: 'cop',
      key: 'cop',
      render: (val: number) => val.toFixed(2),
      sorter: (a: EquipmentStatus, b: EquipmentStatus) => a.cop - b.cop
    }
  ];

  const latestReportDate = dailyReport[0]?.stat_date;
  const latestReports = latestReportDate
    ? dailyReport.filter((item) => item.stat_date === latestReportDate)
    : [];
  const latestReportSummary = {
    totalSupply: latestReports.reduce((sum, item) => sum + (item.total_supply_kwh || item.total_cooling_supply_kwh || 0), 0),
    totalEnergy: latestReports.reduce((sum, item) => sum + (item.total_energy_consumption_kwh || 0), 0),
    netProfit: latestReports.reduce((sum, item) => sum + (item.net_profit || 0), 0),
  };

  if (loading && equipmentStatus.length === 0) {
    return <Loading tip="加载系统数据..." />;
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* 数据说明 */}
      <Alert
        message={
          <Space>
            <SyncOutlined spin />
            <span>实时更新中</span>
          </Space>
        }
        description={
          <Space direction="vertical" size="small">
            <span>本系统展示2018年历史运行数据，当前页面读取统一 Gold 小时表，覆盖冷机、热机和冷热电三联供。</span>
            <span style={{ fontSize: 12, color: '#666' }}>
              系统每30秒自动刷新数据，展示实时更新能力 | 最后更新: {lastUpdateTime}
            </span>
          </Space>
        }
        type="info"
        showIcon
        closable
      />

      {/* 系统概览卡片 */}
      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic
              title="系统健康度"
              value={healthScore}
              suffix="%"
              valueStyle={{ color: healthScore > 80 ? '#52c41a' : healthScore > 60 ? '#faad14' : '#f5222d' }}
            />
            <Progress
              percent={healthScore}
              strokeColor={healthScore > 80 ? '#52c41a' : healthScore > 60 ? '#faad14' : '#f5222d'}
              showInfo={false}
              style={{ marginTop: 8 }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="在线设备"
              value={systemStats.onlineEquipment}
              suffix={`/ ${systemStats.totalEquipment}`}
              valueStyle={{ color: '#52c41a' }}
              prefix={<CheckCircleOutlined />}
            />
            <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
              告警: {systemStats.warningEquipment} | 离线: {systemStats.offlineEquipment}
            </div>
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="总供能量"
              value={systemStats.totalSupply.toFixed(2)}
              suffix="kWh"
              valueStyle={{ color: '#1890ff' }}
              prefix={<DashboardOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="总供热量"
              value={systemStats.totalHeating.toFixed(2)}
              suffix="kWh"
              valueStyle={{ color: '#ff4d4f' }}
              prefix={<FireOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Card>
            <Statistic
              title="总电耗"
              value={systemStats.totalElectricity.toFixed(2)}
              suffix="kWh"
              valueStyle={{ color: '#52c41a' }}
              prefix={<ThunderboltOutlined />}
            />
          </Card>
        </Col>
        <Col span={12}>
          <Card>
            <Statistic
              title="平均COP"
              value={systemStats.avgCOP.toFixed(2)}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      {/* 系统趋势图 */}
      <Card title="系统能耗趋势 (最近24小时)">
        <BaseChart option={trendChartOption} height={350} />
      </Card>

      {/* 设备状态表格 */}
      <Card
        title="设备运行状态"
        extra={
          <Space>
            <Tag color="success">在线: {systemStats.onlineEquipment}</Tag>
            <Tag color="warning">告警: {systemStats.warningEquipment}</Tag>
            <Tag color="error">离线: {systemStats.offlineEquipment}</Tag>
          </Space>
        }
      >
        <Table
          columns={statusColumns}
          dataSource={equipmentStatus}
          rowKey={(record) => `${record.system_type}-${record.equipment_id}`}
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 台设备`
          }}
          scroll={{ x: 1000 }}
        />
      </Card>

      {/* 日报信息 */}
      {dailyReport && dailyReport.length > 0 && (
        <Card title="最新综合日报">
          <Row gutter={16}>
            <Col span={8}>
              <Statistic
                title="日期"
                value={latestReportDate}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="日总供能量"
                value={formatEnergy(latestReportSummary.totalSupply)}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="日电耗"
                value={formatEnergy(latestReportSummary.totalEnergy)}
              />
            </Col>
          </Row>
        </Card>
      )}
    </Space>
  );
}
