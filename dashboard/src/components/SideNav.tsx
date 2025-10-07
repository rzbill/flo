import React from 'react';
import { cn } from './ui/utils';
import { Button } from './ui/button';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
} from './ui/dropdown-menu';
import { 
  Activity, 
  Radio, 
  Settings,
  GitBranch,
  Workflow,
  ShieldCheck,
  GripVertical,
  Package
} from 'lucide-react';

interface SideNavProps {
  currentView: string;
  onNavigate: (view: any) => void;
}

const navItems = [
  {
    id: 'overview',
    label: 'Overview',
    icon: Activity,
    description: 'Quick overview'
  },
  {
    id: 'streams',
    label: 'Streams', 
    icon: Radio,
    description: 'Browse streams'
  },
  {
    id: 'workqueues',
    label: 'WorkQueues',
    icon: Package,
    description: 'Task queues'
  },
];

export function SideNav({ currentView, onNavigate }: SideNavProps) {
  const [sidebarMode, setSidebarMode] = React.useState<'expanded' | 'collapsed' | 'hover'>('hover');
  const [hovered, setHovered] = React.useState(false);
  const expanded = sidebarMode === 'expanded';

  const NavContent = ({ expanded }: { expanded: boolean }) => (
    <>
      <div className="space-y-0.5">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = item.id === 'streams'
            ? (currentView === 'streams' || currentView === 'stream-details')
            : item.id === 'workqueues'
            ? (currentView === 'workqueues' || currentView === 'workqueue-details')
            : currentView === item.id;

          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              title={!expanded ? item.label : undefined}
              className={cn(
                "w-full flex items-center gap-2 h-8 rounded-md transition-colors px-2 justify-start",
                "hover:bg-accent hover:text-accent-foreground",
                isActive && "bg-accent text-accent-foreground"
              )}
            >
              <Icon className="w-4 h-4 shrink-0" />
              <div
                className={cn(
                  "flex-1 text-left transition-[opacity,width] duration-200",
                  expanded ? 'opacity-100 w-auto' : 'opacity-0 w-0 overflow-hidden'
                )}
              >
                <div className="font-medium text-sm leading-4">{item.label}</div>
              </div>
            </button>
          );
        })}
      </div>
      <div className={cn("mt-6 px-2", !expanded && 'pointer-events-none opacity-0 h-0 overflow-hidden')}>
        <div className="text-xs text-muted-foreground mb-2 leading-4">Coming Soon</div>
        <div className="space-y-0.5">
          <button
            disabled
            className={cn(
              "w-full flex items-center space-x-2 px-2 py-1.5 rounded-md",
              "text-muted-foreground opacity-50 cursor-not-allowed"
            )}
          >
            <GitBranch className="w-4 h-4" />
            <div className="flex-1 text-left">
              <div className="font-medium text-sm leading-5">Streams</div>
              <div className="text-xs text-muted-foreground leading-4">Data streams</div>
            </div>
          </button>
          <button
            disabled
            className={cn(
              "w-full flex items-center space-x-2 px-2 py-1.5 rounded-md",
              "text-muted-foreground opacity-50 cursor-not-allowed"
            )}
          >
            <Workflow className="w-4 h-4" />
            <div className="flex-1 text-left">
              <div className="font-medium text-sm leading-5">Workflows</div>
              <div className="text-xs text-muted-foreground leading-4">Process flows</div>
            </div>
          </button>
          <button
            disabled
            className={cn(
              "w-full flex items-center space-x-2 px-2 py-1.5 rounded-md",
              "text-muted-foreground opacity-50 cursor-not-allowed"
            )}
          >
            <ShieldCheck className="w-4 h-4" />
            <div className="flex-1 text-left">
              <div className="font-medium text-sm leading-5">Failover Plans</div>
              <div className="text-xs text-muted-foreground leading-4">Backup strategies</div>
            </div>
          </button>
          <button
            disabled
            className={cn(
              "w-full flex items-center space-x-2 px-2 py-1.5 rounded-md",
              "text-muted-foreground opacity-50 cursor-not-allowed"
            )}
          >
            <Settings className="w-4 h-4" />
            <div className="flex-1 text-left">
              <div className="font-medium text-sm leading-5">Admin</div>
              <div className="text-xs text-muted-foreground leading-4">System controls</div>
            </div>
          </button>
        </div>
      </div>
    </>
  );
  return (
    <>
      <nav
        className={cn(
          expanded ? 'w-56' : 'w-14',
          'h-full border-r bg-background px-2 py-3 transition-[width] duration-200 overflow-hidden overflow-y-auto'
        )}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >
      <NavContent expanded={expanded} />
      </nav>

      {sidebarMode === 'hover' && hovered ? (
        <div
          className="absolute inset-y-0 left-0 z-30 w-56 border-r bg-background px-2 py-3 shadow-md overflow-y-auto"
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          <NavContent expanded={true} />
        </div>
      ) : null}

      {/* Sidebar control - floating button */}
      <div className="fixed bottom-4 left-2 z-50">
        <DropdownMenu>
          <DropdownMenuTrigger asChild className="cursor-pointer">
            <Button size="icon" variant="outline" className="rounded-1 p-1 shadow-none text-[8px]">
              <GripVertical className="w-2 h-2" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" side="top">
            <DropdownMenuLabel className="text-xs">Sidebar control</DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuRadioGroup
              value={sidebarMode}
              className="text-xs"
              onValueChange={(v: string) => setSidebarMode(v as 'expanded' | 'collapsed' | 'hover')}
            >
              <DropdownMenuRadioItem value="expanded" className="text-xs">Expanded</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="collapsed" className="text-xs">Collapsed</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="hover" className="text-xs">Expand on hover</DropdownMenuRadioItem>
            </DropdownMenuRadioGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </>
  );
}