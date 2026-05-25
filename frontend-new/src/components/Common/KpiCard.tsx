import { ArrowDownOutlined, ArrowUpOutlined } from '@ant-design/icons';
import type { ReactNode } from 'react';

interface KpiCardProps {
  title: string;
  value: ReactNode;
  unit?: string;
  icon?: ReactNode;
  trend?: number;
  hint?: string;
  dark?: boolean;
}

export default function KpiCard({ title, value, unit, icon, trend, hint, dark = false }: KpiCardProps) {
  const trendColor = trend === undefined ? '' : trend >= 0 ? 'text-emerald-500' : 'text-rose-500';

  return (
    <div className={`page-card rounded-lg p-4 ${dark ? 'dashboard-panel text-slate-100' : 'bg-white text-slate-900'}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className={`text-sm ${dark ? 'text-slate-400' : 'text-slate-500'}`}>{title}</div>
          <div className="mt-2 flex items-baseline gap-2">
            <span className="truncate text-2xl font-semibold tracking-normal">{value}</span>
            {unit ? <span className={`text-sm ${dark ? 'text-slate-400' : 'text-slate-500'}`}>{unit}</span> : null}
          </div>
        </div>
        {icon ? (
          <div className={`grid h-10 w-10 shrink-0 place-items-center rounded-lg ${dark ? 'bg-sky-400/12 text-sky-300' : 'bg-sky-50 text-sky-600'}`}>
            {icon}
          </div>
        ) : null}
      </div>
      <div className={`mt-3 flex min-h-5 items-center gap-2 text-xs ${dark ? 'text-slate-400' : 'text-slate-500'}`}>
        {trend !== undefined ? (
          <span className={`inline-flex items-center gap-1 ${trendColor}`}>
            {trend >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
            {Math.abs(trend).toFixed(1)}%
          </span>
        ) : null}
        {hint ? <span className="truncate">{hint}</span> : null}
      </div>
    </div>
  );
}
