type Status = 'running' | 'stopped' | 'warning' | 'nodata' | 'online' | 'offline';

const statusConfig: Record<Status, { label: string; className: string; dot: string }> = {
  running: { label: '运行中', className: 'bg-emerald-50 text-emerald-700 ring-emerald-200', dot: 'bg-emerald-500' },
  online: { label: '运行中', className: 'bg-emerald-50 text-emerald-700 ring-emerald-200', dot: 'bg-emerald-500' },
  stopped: { label: '停机', className: 'bg-slate-100 text-slate-600 ring-slate-200', dot: 'bg-slate-400' },
  offline: { label: '停机', className: 'bg-slate-100 text-slate-600 ring-slate-200', dot: 'bg-slate-400' },
  warning: { label: '预警', className: 'bg-amber-50 text-amber-700 ring-amber-200', dot: 'bg-amber-500' },
  nodata: { label: '无数据', className: 'bg-zinc-100 text-zinc-500 ring-zinc-200', dot: 'bg-zinc-400' },
};

export default function StatusBadge({ status }: { status: Status }) {
  const config = statusConfig[status] || statusConfig.nodata;
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ring-1 ${config.className}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${config.dot}`} />
      {config.label}
    </span>
  );
}
