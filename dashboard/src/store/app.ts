import { create } from 'zustand';

interface AppState {
  namespace: string;
  setNamespace: (ns: string) => void;
}

export const useAppStore = create<AppState>((set) => ({
  namespace: 'default',
  setNamespace: (ns: string) => set({ namespace: ns }),
}));


