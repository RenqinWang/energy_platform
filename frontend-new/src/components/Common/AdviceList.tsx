import { Alert } from 'antd';

export interface AdviceItem {
  id?: string;
  advice_time?: string;
  system_type?: string;
  equipment_id?: string;
  advice_type?: string;
  risk_level?: string;
  advice_text: string;
}

const riskType = (risk?: string): 'success' | 'info' | 'warning' | 'error' => {
  if (risk === 'high') return 'error';
  if (risk === 'medium') return 'warning';
  if (risk === 'low') return 'info';
  return 'success';
};

export default function AdviceList({ items, dark = false }: { items: AdviceItem[]; dark?: boolean }) {
  if (!items.length) {
    return <div className={`rounded-lg border p-4 text-sm ${dark ? 'border-slate-700 text-slate-400' : 'border-slate-200 text-slate-500'}`}>暂无运行建议</div>;
  }

  return (
    <div className="space-y-3">
      {items.map((item, idx) => (
        <Alert
          key={item.id || `${item.equipment_id || 'advice'}-${idx}`}
          showIcon
          type={riskType(item.risk_level)}
          message={`${item.equipment_id || '系统'} · ${item.advice_type || '运行建议'}`}
          description={item.advice_text}
        />
      ))}
    </div>
  );
}
