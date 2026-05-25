import StatusBadge from './StatusBadge';
import type { SystemType } from '../../api/report';

interface EquipmentMatrixItem {
  system_type: SystemType;
  equipment_id: string;
  status: 'running' | 'stopped' | 'warning' | 'nodata' | 'online' | 'offline';
  supply?: number;
  efficiency?: number;
}

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

export default function EquipmentMatrix({
  items,
  dark = false,
  onSelect,
}: {
  items: EquipmentMatrixItem[];
  dark?: boolean;
  onSelect?: (item: EquipmentMatrixItem) => void;
}) {
  const groups = ['chiller', 'heating', 'cchp'].map((systemType) => ({
    systemType: systemType as SystemType,
    rows: items.filter((item) => item.system_type === systemType),
  }));

  return (
    <div className="space-y-3">
      {groups.map(({ systemType, rows }) => (
        <div key={systemType} className={`rounded-lg border p-3 ${dark ? 'border-slate-700/70 bg-slate-900/40' : 'border-slate-200 bg-slate-50'}`}>
          <div className={`mb-3 text-sm font-semibold ${dark ? 'text-slate-200' : 'text-slate-700'}`}>{systemLabel[systemType]}</div>
          {rows.length ? (
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
              {rows.map((item) => (
                <button
                  key={`${item.system_type}:${item.equipment_id}`}
                  type="button"
                  onClick={() => onSelect?.(item)}
                  className={`min-h-[72px] rounded-lg border p-3 text-left transition hover:-translate-y-0.5 ${
                    dark ? 'border-slate-700 bg-slate-950/50 hover:border-sky-400/70' : 'border-slate-200 bg-white hover:border-sky-300'
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className={`truncate text-sm font-medium ${dark ? 'text-slate-100' : 'text-slate-800'}`}>
                      {equipmentLabel(item.system_type, item.equipment_id)}
                    </span>
                    <StatusBadge status={item.status} />
                  </div>
                  <div className={`mt-2 text-xs ${dark ? 'text-slate-400' : 'text-slate-500'}`}>
                    供能 {Number(item.supply || 0).toFixed(1)} kWh
                    {item.efficiency ? ` · 效率 ${item.efficiency.toFixed(2)}` : ''}
                  </div>
                </button>
              ))}
            </div>
          ) : (
            <div className={`text-sm ${dark ? 'text-slate-500' : 'text-slate-400'}`}>暂无设备数据</div>
          )}
        </div>
      ))}
    </div>
  );
}
