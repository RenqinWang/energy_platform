import { useEffect, useState } from 'react';

export default function DateTimeDisplay({ dataTime, dark = false }: { dataTime?: string; dark?: boolean }) {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <div className={`text-right text-xs ${dark ? 'text-slate-400' : 'text-slate-500'}`}>
      <div className={`text-sm font-semibold ${dark ? 'text-slate-100' : 'text-slate-800'}`}>
        {now.toLocaleString('zh-CN', { hour12: false })}
      </div>
      <div>数据时间 {dataTime || '-'}</div>
    </div>
  );
}
