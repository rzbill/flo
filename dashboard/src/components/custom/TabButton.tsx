import React from 'react';
import { Button } from '../ui/button';

export interface TabButtonProps {
  label: string;
  icon?: React.ElementType;
  onClick?: () => void;
  className?: string;
}

export function TabButton({ label, icon: Icon, onClick, className }: TabButtonProps) {
  return (
    <Button
      onClick={onClick}
      className={className ?? 'gap-2 h-8 px-3 rounded-none border-none shadow-none bg-emerald-100 text-emerald-700 hover:bg-emerald-200'}
    >
      {Icon ? <Icon className="w-3 h-3" /> : null}
      <span className="text-xs">{label}</span>
    </Button>
  );
}

export default TabButton;


