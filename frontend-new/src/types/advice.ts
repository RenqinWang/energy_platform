// 运行建议相关类型定义

export interface AdviceRecord {
  station_id: string;
  equipment_id: string;
  advice_time: string;
  advice_type: 'load_change' | 'anomaly' | 'efficiency' | 'economic';
  risk_level: 'low' | 'medium' | 'high';
  advice_text: string;
  evidence_metrics: string; // JSON string
  rule_id: string;
  is_active: boolean;
  dt: string | null;
}

export interface EvidenceMetrics {
  [key: string]: number | string;
}

export const ADVICE_TYPE_LABELS: Record<string, string> = {
  load_change: '负荷变化',
  anomaly: '异常检测',
  efficiency: '能效优化',
  economic: '经济性'
};

export const RISK_LEVEL_LABELS: Record<string, string> = {
  low: '低风险',
  medium: '中风险',
  high: '高风险'
};

export const RISK_LEVEL_COLORS: Record<string, string> = {
  low: '#52c41a',
  medium: '#faad14',
  high: '#f5222d'
};
