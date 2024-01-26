import React, { useCallback, useState } from 'react';
import cn from 'classnames';

export type DashboardPlotWrapperProps = {
  className?: string;
  children?: React.ReactNode;
  onPointerOver?: (event: React.PointerEvent) => void;
  onPointerOut?: (event: React.PointerEvent) => void;
} & React.HTMLAttributes<HTMLDivElement>;

/**
 * Firefox css :has plot hover fix
 */
export function DashboardPlotWrapper({
  children,
  className,
  onPointerOver,
  onPointerOut,
  ...props
}: DashboardPlotWrapperProps) {
  const [hover, setHover] = useState(false);
  const onOver = useCallback(
    (event: React.PointerEvent) => {
      setHover(true);
      onPointerOver?.(event);
    },
    [onPointerOver]
  );
  const onOut = useCallback(
    (event: React.PointerEvent) => {
      setHover(false);
      onPointerOut?.(event);
    },
    [onPointerOut]
  );
  return (
    <div className={cn(hover && 'plot-item-hover', className)} onPointerOver={onOver} onPointerOut={onOut} {...props}>
      {children}
    </div>
  );
}
