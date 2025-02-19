import { ReactNode, useMemo } from 'react';
import cn from 'classnames';

export type PlotBoxProps = {
  fixRatio?: boolean;
  children?: ReactNode;
  className?: string;
  innerClassName?: string;
};

export function PlotBox({ fixRatio, children, className, innerClassName }: PlotBoxProps) {
  const styleBox = useMemo(() => {
    if (fixRatio) {
      return {
        paddingTop: '61.8034%',
      };
    }
    return undefined;
  }, [fixRatio]);
  return (
    <div className={cn('position-relative w-100', !fixRatio && 'flex-grow-1', className)} style={styleBox}>
      <div className={cn('position-absolute w-100 h-100 top-0 start-0', innerClassName)}>{children}</div>
    </div>
  );
}
