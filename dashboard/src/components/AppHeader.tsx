import React from 'react';
import { Button } from './ui/button';
import { 
  Sun, 
  Moon, 
  Monitor, 
  HelpCircle,
  Zap,
  FlaskConical
} from 'lucide-react';

interface AppHeaderProps {
  theme: 'light' | 'dark' | 'system';
  onThemeChange: (theme: 'light' | 'dark' | 'system') => void;
  onNavigate: (view: any, namespace?: string, stream?: string) => void;
}

export function AppHeader({ 
  theme, 
  onThemeChange,
  onNavigate
}: AppHeaderProps) {
  const themeIcons = {
    light: Sun,
    dark: Moon,
    system: Monitor
  };

  const ThemeIcon = themeIcons[theme];
  // OSS header: no org/project/instance controls

  return (
    <header className="border-b bg-background px-4 py-2 flex items-center gap-3">
      {/* Left: Brand */}
      <div className="flex items-center gap-2">
        <div className="w-6 h-6 rounded-md bg-primary flex items-center justify-center">
          <Zap className="w-4 h-4 text-primary-foreground" />
        </div>
        <span className="font-semibold text-base">FLO</span>
      </div>

      {/* Spacer */}
      <div className="ml-auto" />

      {/* Theme quick toggle: reliable single button */}
      <Button
        variant="ghost"
        size="sm"
        className="h-7 w-7 p-0"
        onClick={() => onThemeChange(theme === 'dark' ? 'light' : 'dark')}
        title={theme === 'dark' ? 'Switch to light' : 'Switch to dark'}
      >
        <ThemeIcon className="w-4 h-4" />
      </Button>

      {/* Playground (dev only) */}
      {(typeof import.meta !== 'undefined' && (import.meta as any).env?.DEV) && (
        <Button
          variant="ghost"
          size="sm"
          className="h-7 px-2 gap-1"
          onClick={() => onNavigate('dev-components')}
          title="Open playground"
        >
          <FlaskConical className="w-4 h-4" />
          <span className="text-xs">Playground</span>
        </Button>
      )}

      {/* Help 
      <Button variant="ghost" size="sm" className="h-7 w-7 p-0">
        <HelpCircle className="w-4 h-4" />
      </Button>*/}
    </header>
  );
}