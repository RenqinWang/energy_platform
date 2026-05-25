import { create } from 'zustand';
import type { SystemType } from '../api/report';

interface AppState {
  stationId?: string;
  systemType: SystemType;
  equipmentId?: string;
  timeRange: {
    start?: string;
    end?: string;
  };
  theme: 'dashboard' | 'business';
  setStationId: (stationId?: string) => void;
  setSystemType: (systemType: SystemType) => void;
  setEquipmentId: (equipmentId?: string) => void;
  setTimeRange: (timeRange: AppState['timeRange']) => void;
  setTheme: (theme: AppState['theme']) => void;
}

export const useAppStore = create<AppState>((set) => ({
  stationId: undefined,
  systemType: 'chiller',
  equipmentId: undefined,
  timeRange: {},
  theme: 'business',
  setStationId: (stationId) => set({ stationId }),
  setSystemType: (systemType) => set({ systemType }),
  setEquipmentId: (equipmentId) => set({ equipmentId }),
  setTimeRange: (timeRange) => set({ timeRange }),
  setTheme: (theme) => set({ theme }),
}));
