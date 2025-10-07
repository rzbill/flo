import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import type { View as NaviView } from './types/navigation';
import { AppHeader } from './components/AppHeader';
import { SideNav } from './components/SideNav';
import { HealthView } from './components/views/HealthView';
import { NamespacesView } from './components/views/NamespacesView';
import { StreamsView } from './components/views/streams/Index';
import { StreamDetailsView } from './components/views/streams/DetailsView';
import { WorkQueuesView } from './components/views/workqueues/Index';
import { WorkQueueDetailsView } from './components/views/workqueues/DetailsView';
import DevComponentsView from './components/views/DevComponentsView';
import { queryClient } from './lib/query-client';
import { ErrorBoundary } from './components/ErrorBoundary';

import { Toaster } from './components/ui/sonner';

type View = NaviView;

interface AppState {
  theme: 'light' | 'dark' | 'system';
  environment: 'local' | 'dev' | 'staging' | 'prod';
}

// Navigation helper component that uses React Router
function NavigationWrapper() {
  const navigate = useNavigate();
  const location = useLocation();

  const navigateTo = (view: View, namespace?: string, streamOrQueue?: string, tab?: string) => {
    if (view === 'stream-details' && namespace && streamOrQueue) {
      const tabPath = tab || 'messages';
      navigate(`/streams/details/${tabPath}?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(streamOrQueue)}`);
    } else if (view === 'workqueue-details' && namespace && streamOrQueue) {
      const tabPath = tab || 'overview';
      navigate(`/workqueues/details/${tabPath}?namespace=${encodeURIComponent(namespace)}&workqueue=${encodeURIComponent(streamOrQueue)}`);
    } else if (view === 'streams' && namespace) {
      navigate(`/streams?namespace=${encodeURIComponent(namespace)}`);
    } else if (view === 'workqueues' && namespace) {
      navigate(`/workqueues?namespace=${encodeURIComponent(namespace)}`);
    } else if (view === 'live-tail' && namespace && streamOrQueue) {
      navigate(`/live-tail?namespace=${encodeURIComponent(namespace)}&stream=${encodeURIComponent(streamOrQueue)}`);
    } else {
      navigate(`/${view}`);
    }
  };

  // Determine current view from location
  const getCurrentView = (): View => {
    const path = location.pathname;
    if (path.startsWith('/streams/details')) return 'stream-details';
    if (path.startsWith('/streams')) return 'streams';
    if (path.startsWith('/workqueues/details')) return 'workqueue-details';
    if (path.startsWith('/workqueues')) return 'workqueues';
    if (path.startsWith('/namespaces')) return 'namespaces';
    if (path.startsWith('/live-tail')) return 'live-tail';
    if (path.startsWith('/dev-components')) return 'dev-components';
    return 'health';
  };

  const currentView = getCurrentView();

  return (
    <div className="relative flex-1 overflow-hidden">
      <div className="flex h-full">
        <SideNav
          currentView={currentView}
          onNavigate={navigateTo}
        />
        <main className="h-full overflow-auto flex-1">
        <Routes>
          <Route path="/" element={<Navigate to="/health" replace />} />
          <Route path="/health" element={<HealthView onNavigate={navigateTo} />} />
          <Route path="/namespaces" element={<NamespacesView onNavigate={navigateTo as any} />} />
          <Route path="/streams" element={<StreamsView onNavigate={navigateTo as any} />} />
          <Route path="/streams/details/:tab" element={<StreamDetailsView onNavigate={navigateTo as any} />} />
          <Route path="/workqueues" element={<WorkQueuesView onNavigate={navigateTo as any} />} />
          <Route path="/workqueues/details/:tab" element={<WorkQueueDetailsView onNavigate={navigateTo as any} />} />
          <Route path="/dev-components" element={
            (typeof import.meta !== 'undefined' && (import.meta as any).env?.DEV) 
              ? <DevComponentsView /> 
              : <Navigate to="/health" replace />
          } />
          <Route path="*" element={<Navigate to="/health" replace />} />
        </Routes>
        </main>
      </div>
    </div>
  );
}

export default function App() {
  const [state, setState] = useState<AppState>({
    theme: 'system',
    environment: 'local'
  });

  // Theme management
  useEffect(() => {
    const savedTheme = localStorage.getItem('flo.theme') as 'light' | 'dark' | 'system' || 'system';
    setState(prev => ({ ...prev, theme: savedTheme }));
  }, []);

  useEffect(() => {
    const applyTheme = () => {
      const root = document.documentElement;
      
      if (state.theme === 'system') {
        const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        root.classList.toggle('dark', systemPrefersDark);
      } else {
        root.classList.toggle('dark', state.theme === 'dark');
      }
    };

    applyTheme();
    localStorage.setItem('flo.theme', state.theme);

    if (state.theme === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      mediaQuery.addEventListener('change', applyTheme);
      return () => mediaQuery.removeEventListener('change', applyTheme);
    }
  }, [state.theme]);

  const setTheme = (theme: 'light' | 'dark' | 'system') => {
    setState(prev => ({ ...prev, theme }));
  };

  const setEnvironment = (environment: 'local' | 'dev' | 'staging' | 'prod') => {
    setState(prev => ({ ...prev, environment }));
  };

  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
      <Router>
          <div className="h-screen flex flex-col bg-background">
            <AppHeader
              theme={state.theme}
              onThemeChange={setTheme}
              onNavigate={() => {}} // Navigation is now handled by React Router
            />
            
            <NavigationWrapper />
            
            <Toaster />
          </div>
        </Router>
      {/*(import.meta as any).env?.DEV ? <ReactQueryDevtools initialIsOpen={false} /> : null*/}
      </QueryClientProvider>
    </ErrorBoundary>
  );
}