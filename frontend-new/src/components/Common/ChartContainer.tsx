import { Empty, Spin } from 'antd';
import type { ReactNode } from 'react';

interface ChartContainerProps {
  title: string;
  subtitle?: string;
  action?: ReactNode;
  loading?: boolean;
  empty?: boolean;
  dark?: boolean;
  children: ReactNode;
}

export default function ChartContainer({ title, subtitle, action, loading, empty, dark = false, children }: ChartContainerProps) {
  return (
    <section className={`page-card rounded-lg p-4 ${dark ? 'dashboard-panel text-slate-100' : 'bg-white text-slate-900'}`}>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="m-0 text-base font-semibold tracking-normal">{title}</h3>
          {subtitle ? <p className={`mt-1 text-sm ${dark ? 'text-slate-400' : 'text-slate-500'}`}>{subtitle}</p> : null}
        </div>
        {action}
      </div>
      <Spin spinning={Boolean(loading)}>
        {empty ? <Empty description="暂无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} /> : children}
      </Spin>
    </section>
  );
}
