// 常量定义

// 时间范围选项
export const TIME_RANGE_OPTIONS = [
  { label: '最近1小时', value: '1h', hours: 1 },
  { label: '最近6小时', value: '6h', hours: 6 },
  { label: '最近24小时', value: '24h', hours: 24 },
  { label: '最近7天', value: '7d', hours: 168 },
  { label: '自定义', value: 'custom', hours: 0 }
];

// 主题选项
export const THEME_OPTIONS = [
  { label: '温度分析', value: 'temperature', unit: '℃' },
  { label: '流量分析', value: 'flow', unit: 'm³/h' },
  { label: '压力分析', value: 'pressure', unit: 'MPa' },
  { label: '能耗分析', value: 'energy', unit: 'kWh' }
];

// 设备状态
export const EQUIPMENT_STATUS = {
  RUNNING: 1,
  STOPPED: 0
};

// 刷新间隔（毫秒）
export const REFRESH_INTERVALS = {
  FAST: 10000,    // 10秒
  NORMAL: 30000,  // 30秒
  SLOW: 60000     // 60秒
};

// 图表颜色
export const CHART_COLORS = [
  '#1890ff',
  '#52c41a',
  '#faad14',
  '#f5222d',
  '#722ed1',
  '#13c2c2',
  '#eb2f96',
  '#fa8c16'
];
